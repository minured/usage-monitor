"""运维页面与 HTTP 接口。"""

from __future__ import annotations

import gzip
import json
import logging
import threading
from socketserver import ThreadingMixIn
from time import monotonic, sleep
from typing import Any, Callable
from urllib.parse import parse_qs
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

from .collector import configure_logging
from .config import Settings, load_settings
from .db import FILTERS, UsageDatabase
from .models import (
    COLLECTOR_PHASE_ERROR,
    COLLECTOR_PHASE_IDLE,
    COLLECTOR_PHASE_QUERYING,
    COLLECTOR_PHASE_RECONCILING,
    COLLECTOR_PHASE_SCANNING,
    COLLECTOR_PHASE_SLEEPING,
)
from .timeutil import utc_now_iso


logger = logging.getLogger("usage_monitor.web")
PROGRESS_DEFAULT_POLL_MS = 15_000
PROGRESS_CONTROL_PENDING_POLL_MS = 1_000
PROGRESS_POLL_INTERVALS = {
    COLLECTOR_PHASE_IDLE: 15_000,
    COLLECTOR_PHASE_SCANNING: 2_000,
    COLLECTOR_PHASE_QUERYING: 1_000,
    COLLECTOR_PHASE_RECONCILING: 2_000,
    COLLECTOR_PHASE_SLEEPING: 30_000,
    COLLECTOR_PHASE_ERROR: 15_000,
}
EVENT_STREAM_RETRY_MS = 1_500
EVENT_STREAM_KEEPALIVE_SECONDS = 20.0


class ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    """轻量多线程 WSGI Server，避免首页与接口请求互相串行阻塞。"""

    daemon_threads = True


class _JsonCacheEntry:
    """JSON 响应缓存项。"""

    __slots__ = ("body",)

    def __init__(self, *, body: bytes):
        self.body = body


class _JsonResponseCache:
    """进程内小型 JSON 缓存，按修订号复用已编码响应。"""

    def __init__(self, max_entries: int = 16) -> None:
        self._entries: dict[tuple[Any, ...], _JsonCacheEntry] = {}
        self._lock = threading.Lock()
        self._max_entries = max(max_entries, 1)

    def get_or_build(
        self,
        key: tuple[Any, ...],
        builder: Callable[[], bytes],
    ) -> bytes:
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                return entry.body

        body = builder()
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                return entry.body
            if len(self._entries) >= self._max_entries:
                self._entries.clear()
            self._entries[key] = _JsonCacheEntry(body=body)
        return body

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


def _source_file_name(source_file: Any) -> str:
    value = str(source_file or "").strip()
    if not value:
        return "-"
    head, _sep, tail = value.rpartition("/")
    if head:
        return tail or value
    return value


def _format_remaining_percent(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return "-"
    remaining = max(0.0, min(100.0, 100.0 - float(value)))
    if remaining.is_integer():
        return f"{int(remaining)}%"
    return f"{remaining:.1f}%"


def _remaining_percent_value(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    remaining = max(0.0, min(100.0, 100.0 - float(value)))
    return round(remaining, 4)


def _int_value(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _progress_percent(phase: str, total_candidates: int, processed_candidates: int, round_started_at_utc: str) -> float:
    if total_candidates > 0:
        return round(max(0.0, min(100.0, (processed_candidates / total_candidates) * 100.0)), 1)
    if round_started_at_utc and phase in {
        COLLECTOR_PHASE_IDLE,
        COLLECTOR_PHASE_RECONCILING,
        COLLECTOR_PHASE_SLEEPING,
        COLLECTOR_PHASE_ERROR,
    }:
        return 100.0
    return 0.0


def _control_state_from_runtime(row: dict[str, Any]) -> dict[str, Any]:
    phase = str(row.get("phase") or COLLECTOR_PHASE_IDLE)
    manual_trigger_requested_at_utc = str(row.get("manual_trigger_requested_at") or "")
    manual_stop_requested_at_utc = str(row.get("manual_stop_requested_at") or "")
    can_manual_start = (
        phase in {COLLECTOR_PHASE_IDLE, COLLECTOR_PHASE_SLEEPING, COLLECTOR_PHASE_ERROR}
        and not manual_trigger_requested_at_utc
    )
    can_manual_stop = (
        phase in {COLLECTOR_PHASE_SCANNING, COLLECTOR_PHASE_QUERYING, COLLECTOR_PHASE_RECONCILING}
        and not manual_stop_requested_at_utc
    )
    return {
        "manual_trigger_pending": bool(manual_trigger_requested_at_utc),
        "manual_trigger_requested_at_utc": manual_trigger_requested_at_utc,
        "manual_stop_pending": bool(manual_stop_requested_at_utc),
        "manual_stop_requested_at_utc": manual_stop_requested_at_utc,
        "can_manual_start": can_manual_start,
        "can_manual_stop": can_manual_stop,
    }


def _progress_poll_interval_ms(phase: str, control_state: dict[str, Any]) -> int:
    if control_state.get("manual_trigger_pending") or control_state.get("manual_stop_pending"):
        return PROGRESS_CONTROL_PENDING_POLL_MS
    return PROGRESS_POLL_INTERVALS.get(phase, PROGRESS_DEFAULT_POLL_MS)


def build_progress_payload(
    settings: Settings,
    database: UsageDatabase | None = None,
) -> dict[str, Any]:
    database = database if database is not None else UsageDatabase(settings.db_path)
    database.initialize()
    row = database.ensure_runtime_row()

    phase = str(row.get("phase") or COLLECTOR_PHASE_IDLE)
    total_scanned = max(_int_value(row.get("total_scanned")), 0)
    total_candidates = max(_int_value(row.get("total_candidates")), 0)
    processed_candidates = max(_int_value(row.get("processed_candidates")), 0)
    skipped_candidates = max(_int_value(row.get("skipped_candidates")), 0)
    current_index = max(_int_value(row.get("current_index")), 0)
    round_started_at_utc = str(row.get("round_started_at_utc") or "")
    source_file = str(row.get("current_source_file") or "")
    progress_percent = _progress_percent(
        phase=phase,
        total_candidates=total_candidates,
        processed_candidates=processed_candidates,
        round_started_at_utc=round_started_at_utc,
    )
    generated_at = utc_now_iso()
    control_state = _control_state_from_runtime(row)

    payload = {
        "generated_at": generated_at,
        "phase": phase,
        "total_scanned": total_scanned,
        "total_candidates": total_candidates,
        "processed_candidates": processed_candidates,
        "skipped_candidates": skipped_candidates,
        "current_index": current_index,
        "current_account_id": str(row.get("current_account_id") or ""),
        "current_account_email": str(row.get("current_account_email") or ""),
        "current_source_file": source_file,
        "current_source_file_name": _source_file_name(source_file),
        "round_started_at_utc": round_started_at_utc,
        "round_finished_at_utc": str(row.get("round_finished_at_utc") or ""),
        "next_round_at_utc": str(row.get("next_round_at_utc") or ""),
        "last_heartbeat_at_utc": str(row.get("last_heartbeat_at_utc") or ""),
        "last_error_detail": str(row.get("last_error_detail") or ""),
        "progress_percent": progress_percent,
        "poll_interval_ms": _progress_poll_interval_ms(phase, control_state),
    }
    payload.update(control_state)
    return payload


def build_manual_scan_payload(
    settings: Settings,
    database: UsageDatabase | None = None,
) -> tuple[str, dict[str, Any]]:
    database = database if database is not None else UsageDatabase(settings.db_path)
    database.initialize()
    result = database.request_manual_scan()
    row = database.ensure_runtime_row()

    phase = str(result.get("phase") or COLLECTOR_PHASE_IDLE)
    accepted = bool(result.get("accepted"))
    status = str(result.get("status") or "")

    if status == "already_requested":
        message = "已有手动触发请求等待执行，本次不再重复排队。"
    elif accepted:
        message = "已提交手动触发请求，collector 会在 sleeping 轮询中尽快开始下一轮。"
    else:
        message = "当前已有扫描在执行，本次未排队。"

    payload = {
        "accepted": accepted,
        "status": status,
        "phase": phase,
        "message": message,
    }
    payload.update(_control_state_from_runtime(row))
    return ("202 Accepted" if accepted else "409 Conflict"), payload


def build_manual_stop_payload(
    settings: Settings,
    database: UsageDatabase | None = None,
) -> tuple[str, dict[str, Any]]:
    database = database if database is not None else UsageDatabase(settings.db_path)
    database.initialize()
    result = database.request_manual_stop()
    row = database.ensure_runtime_row()

    phase = str(result.get("phase") or COLLECTOR_PHASE_IDLE)
    accepted = bool(result.get("accepted"))
    status = str(result.get("status") or "")

    if status == "already_requested":
        message = "已有停止请求等待生效，本次不再重复提交。"
    elif accepted:
        message = "已提交停止请求，当前账号处理完成后将停止本轮。"
    else:
        message = "当前没有进行中的扫描，本次未提交停止请求。"

    payload = {
        "accepted": accepted,
        "status": status,
        "phase": phase,
        "message": message,
    }
    payload.update(_control_state_from_runtime(row))
    return ("202 Accepted" if accepted else "409 Conflict"), payload


def build_dashboard_payload(
    settings: Settings,
    filter_name: str,
    database: UsageDatabase | None = None,
) -> dict[str, Any]:
    current_filter = filter_name if filter_name in FILTERS else "all"
    database = database if database is not None else UsageDatabase(settings.db_path)
    database.initialize()
    accounts_revision = 0
    summary: dict[str, int] = {}
    rows: list[Any] = []
    for _ in range(3):
        before_state = database.fetch_change_state()
        summary, rows = database.fetch_dashboard(current_filter)
        after_state = database.fetch_change_state()
        accounts_revision = int(after_state["accounts_revision"])
        if accounts_revision == int(before_state["accounts_revision"]):
            break

    items: list[dict[str, Any]] = []
    for row in rows:
        source_file = str(row["source_file"] or "")
        used_percent = row["used_percent"]
        remaining_percent_value = _remaining_percent_value(used_percent)
        remaining_percent_text = (
            _format_remaining_percent(used_percent)
            if remaining_percent_value is not None
            else "-"
        )
        reset_at_utc = str(row["reset_at_utc"] or "")
        last_checked_at_utc = str(row["last_checked_at_utc"] or "")
        note = (
            str(row["invalid_reason_detail"] or "").strip()
            or str(row["last_error_detail"] or "").strip()
        )

        items.append(
            {
                "dimension_key": str(row["dimension_key"] or ""),
                "email": str(row["email"] or ""),
                "lifecycle_status": str(row["lifecycle_status"] or ""),
                "quota_status": str(row["quota_status"] or ""),
                "remaining_percent_text": remaining_percent_text,
                "remaining_percent_value": remaining_percent_value,
                "reset_at_utc": reset_at_utc,
                "last_checked_at_utc": last_checked_at_utc,
                "note": note or "-",
                "source_file": source_file,
                "source_file_name": _source_file_name(source_file),
                "plan_type": str(row["plan_type"] or ""),
            }
        )

    generated_at = utc_now_iso()
    return {
        "generated_at": generated_at,
        "accounts_revision": accounts_revision,
        "filter": current_filter,
        "summary": summary,
        "items": items,
    }


def build_dashboard_overview_payload(
    settings: Settings,
    filter_name: str,
    database: UsageDatabase | None = None,
) -> dict[str, Any]:
    dashboard_payload = build_dashboard_payload(settings, filter_name, database)
    return {
        "generated_at": dashboard_payload["generated_at"],
        "accounts_revision": dashboard_payload["accounts_revision"],
        "filter": dashboard_payload["filter"],
        "summary": dashboard_payload["summary"],
    }


def build_dashboard_patch_payload(
    previous_payload: dict[str, Any],
    current_payload: dict[str, Any],
) -> dict[str, Any]:
    previous_items = {
        str(item.get("dimension_key") or ""): item
        for item in previous_payload.get("items", [])
        if str(item.get("dimension_key") or "")
    }
    current_items = {
        str(item.get("dimension_key") or ""): item
        for item in current_payload.get("items", [])
        if str(item.get("dimension_key") or "")
    }
    upserted_items = [
        item
        for dimension_key, item in current_items.items()
        if previous_items.get(dimension_key) != item
    ]
    removed_dimension_keys = [
        dimension_key
        for dimension_key in previous_items
        if dimension_key not in current_items
    ]
    return {
        "generated_at": str(current_payload.get("generated_at") or ""),
        "accounts_revision": int(current_payload.get("accounts_revision") or 0),
        "filter": str(current_payload.get("filter") or ""),
        "summary": current_payload.get("summary") or {},
        "upserted_items": upserted_items,
        "removed_dimension_keys": removed_dimension_keys,
    }


_TABLE_COLUMN_SPECS: tuple[tuple[str, str, str], ...] = (
    ("email", "邮箱", "col-email"),
    ("lifecycle_status", "生命周期", "col-lifecycle"),
    ("remaining_percent_value", "剩余", "col-remaining"),
    ("reset_at_utc", "重置时间", "col-reset"),
    ("last_checked_at_utc", "最近查询", "col-last-checked"),
    ("note", "备注", "col-note"),
)
_TABLE_COLUMN_COUNT = len(_TABLE_COLUMN_SPECS)
_LIFECYCLE_ORDER = {"active": 0, "invalid": 1, "source_missing": 2}
_QUOTA_ORDER = {"available": 0, "exhausted": 1, "unknown": 2}


def _render_table_header_row() -> str:
    rows = ["            <tr>"]
    for sort_key, label, class_name in _TABLE_COLUMN_SPECS:
        th_attrs = ['scope="col"', 'aria-sort="none"']
        if class_name:
            th_attrs.append(f'class="{class_name}"')
        th_attr_text = " " + " ".join(th_attrs)
        rows.append(
            "              "
            f'<th{th_attr_text}><button type="button" class="sort-button" '
            f'data-sort-key="{sort_key}" aria-label="按{label}排序">{label}<span class="sort-indicator" aria-hidden="true"></span></button></th>'
        )
    rows.append("            </tr>")
    return "\n".join(rows)


def _render_index_styles() -> str:
    return """
    :root {
      color-scheme: light;
      --bg: #eef3f7;
      --panel: #ffffff;
      --panel-muted: #f8fafc;
      --line: #d7dee8;
      --line-strong: #b8c2d0;
      --text: #172033;
      --muted: #536173;
      --muted-soft: #738094;
      --primary: #253043;
      --primary-hover: #172033;
      --focus: #0f766e;
      --focus-shadow: 0 0 0 3px rgba(15, 118, 110, 0.18);
      --active: #0f766e;
      --invalid: #dc2626;
      --missing: #b45309;
      --available: #16a34a;
      --exhausted: #f97316;
      --unknown: #64748b;
      --filter-total: #334155;
      --filter-active: #0f766e;
      --filter-available: #16a34a;
      --filter-exhausted: #f97316;
      --filter-unknown: #64748b;
      --filter-invalid: #dc2626;
      --filter-missing: #b45309;
      --radius: 8px;
      --radius-sm: 6px;
      --shadow: 0 1px 2px rgba(23, 23, 23, 0.04);
      --plan-free-bg: #f1f5f9;
      --plan-free-text: #475569;
      --plan-free-border: #cbd5e1;
      --plan-team-bg: #ecfdf5;
      --plan-team-text: #047857;
      --plan-team-border: #a7f3d0;
      --plan-plus-bg: #fff7ed;
      --plan-plus-text: #c2410c;
      --plan-plus-border: #fed7aa;
      --plan-pro-bg: #fff1f2;
      --plan-pro-text: #be123c;
      --plan-pro-border: #fecdd3;
      --plan-business-bg: #f0fdfa;
      --plan-business-text: #0f766e;
      --plan-business-border: #99f6e4;
      --plan-enterprise-bg: #fefce8;
      --plan-enterprise-text: #a16207;
      --plan-enterprise-border: #fde68a;
      --plan-unknown-bg: #f5f5f4;
      --plan-unknown-text: #57534e;
      --plan-unknown-border: #d6d3d1;
    }
    * { box-sizing: border-box; }
    html { scrollbar-gutter: stable; background: var(--bg); }
    body {
      margin: 0;
      color: var(--text);
      background: var(--bg);
      font: 13px/1.45 "IBM Plex Sans", "Helvetica Neue", sans-serif;
      -webkit-font-smoothing: antialiased;
      text-rendering: optimizeLegibility;
    }
    body.modal-open { overflow: hidden; }
    button, input, select, textarea { font: inherit; }
    button:focus-visible,
    .summary-card:focus-visible,
    .sort-button:focus-visible,
    .skip-link:focus-visible,
    .action-button:focus-visible,
    .sticky-filter-chip:focus-visible {
      outline: none;
      box-shadow: var(--focus-shadow);
    }
    .skip-link {
      position: absolute;
      left: 12px;
      top: 10px;
      z-index: 60;
      transform: translateY(-160%);
      padding: 6px 8px;
      border-radius: var(--radius-sm);
      background: var(--primary);
      color: #fff;
      text-decoration: none;
      transition: transform 140ms ease;
    }
    .skip-link:focus-visible { transform: translateY(0); }
    .sr-only {
      position: absolute;
      width: 1px;
      height: 1px;
      padding: 0;
      margin: -1px;
      overflow: hidden;
      clip: rect(0, 0, 0, 0);
      white-space: nowrap;
      border: 0;
    }
    .hidden-runtime-fields { display: none !important; }
    .page {
      max-width: 1500px;
      margin: 0 auto;
      padding: 10px 16px 28px;
    }
    .page-header {
      display: grid;
      gap: 6px;
      margin-bottom: 8px;
    }
    .surface,
    .panel,
    .summary-block {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
    }
    .app-toolbar {
      display: grid;
      grid-template-columns: minmax(148px, auto) minmax(320px, 1fr) auto;
      align-items: center;
      gap: 10px;
      min-height: 42px;
      padding: 6px 8px 6px 10px;
    }
    .brand-row {
      display: flex;
      align-items: center;
      gap: 8px;
      min-width: 0;
    }
    .title {
      margin: 0;
      color: var(--text);
      font-size: 15px;
      line-height: 1.1;
      font-weight: 720;
      letter-spacing: -0.02em;
      white-space: nowrap;
    }
    .toolbar-subtitle,
    .toolbar-stats,
    .meta-pair {
      display: none;
    }
    .toolbar-actions,
    .progress-head-actions {
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      align-items: center;
      gap: 6px;
      min-width: 0;
    }
    .action-button {
      appearance: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 28px;
      border: 1px solid var(--primary);
      border-radius: var(--radius-sm);
      padding: 0 9px;
      background: var(--primary);
      color: #fff;
      font-weight: 650;
      cursor: pointer;
      transition: background-color 140ms ease, border-color 140ms ease, color 140ms ease, opacity 140ms ease;
    }
    .action-button:hover { background: var(--primary-hover); border-color: var(--primary-hover); }
    .action-button-secondary {
      background: #fff;
      border-color: var(--line-strong);
      color: var(--text);
    }
    .action-button-secondary:hover { background: var(--panel-muted); border-color: var(--muted-soft); }
    .action-button-danger {
      background: #b42318;
      border-color: #b42318;
    }
    .action-button-danger:hover { background: #991b1b; border-color: #991b1b; }
    .action-button:disabled {
      opacity: 0.48;
      cursor: not-allowed;
    }
    .run-line {
      display: grid;
      grid-template-columns: auto minmax(140px, 1fr) minmax(220px, auto);
      align-items: center;
      gap: 8px;
      min-width: 0;
    }
    .toolbar-progress {
      min-width: 0;
    }
    .run-progress-label {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      white-space: nowrap;
    }
    .progress-count {
      color: var(--text);
      font-size: 15px;
      line-height: 1;
      font-weight: 760;
      letter-spacing: -0.03em;
    }
    .progress-percent {
      color: var(--muted);
      font-size: 11px;
      font-weight: 650;
    }
    .progress-track {
      height: 5px;
      overflow: hidden;
      border-radius: 999px;
      background: #dbe3ee;
    }
    .progress-bar {
      height: 100%;
      width: 0;
      border-radius: inherit;
      background: var(--primary);
      transition: width 160ms ease;
    }
    .run-meta {
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      align-items: center;
      gap: 3px 10px;
      min-width: 0;
      color: var(--muted);
      font-size: 11px;
    }
    .run-meta-item {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      min-width: 0;
      white-space: nowrap;
    }
    .run-meta-item b {
      min-width: 0;
      color: var(--text);
      font-weight: 680;
    }
    #progress-next-round.is-countdown {
      color: var(--filter-active);
      font-variant-numeric: tabular-nums;
    }
    .run-account b {
      display: inline-block;
      max-width: 220px;
      overflow: hidden;
      text-overflow: ellipsis;
      vertical-align: bottom;
      white-space: nowrap;
    }
    .progress-subsummary,
    .progress-note,
    .action-note {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.4;
    }
    .progress-note {
      display: none;
      padding: 6px 8px;
      border-radius: var(--radius-sm);
      border: 1px solid var(--line);
      background: var(--panel-muted);
    }
    .progress-note.is-visible { display: block; }
    .progress-note.is-error,
    .action-note.is-error {
      color: var(--invalid);
    }
    .action-note {
      min-height: 0;
    }
    .summary-block {
      display: grid;
      overflow: hidden;
      border-color: rgba(184, 194, 208, 0.62);
      box-shadow: none;
    }
    .summary-block .section-heading {
      border-bottom: 1px solid rgba(215, 222, 232, 0.64);
      background: #fff;
      padding: 8px 10px;
    }
    .section-heading,
    .panel-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      min-height: 38px;
      padding: 7px 10px;
      border-bottom: 1px solid var(--line);
      background: var(--panel-muted);
    }
    .section-title,
    .panel-title {
      margin: 0;
      color: var(--text);
      font-size: 13px;
      line-height: 1.2;
      font-weight: 720;
      letter-spacing: 0;
    }
    .section-kicker,
    .eyebrow,
    .section-description,
    .subtitle { display: none; }
    .summary-heading-main {
      display: inline-flex;
      align-items: center;
      gap: 10px;
      min-width: 0;
    }
    .availability-rate {
      --rate-color: var(--filter-unknown);
      display: inline-flex;
      align-items: center;
      gap: 5px;
      min-height: 24px;
      padding: 0 8px;
      border-radius: 999px;
      background: color-mix(in srgb, var(--rate-color) 9%, #ffffff);
      color: var(--rate-color);
      font-size: 12px;
      font-weight: 720;
      white-space: nowrap;
    }
    .availability-rate-label {
      color: var(--muted);
      font-weight: 640;
    }
    .availability-rate.is-high { --rate-color: var(--filter-available); }
    .availability-rate.is-medium { --rate-color: var(--filter-exhausted); }
    .availability-rate.is-low { --rate-color: var(--filter-invalid); }
    .summary {
      display: grid;
      grid-template-columns: repeat(7, minmax(0, 1fr));
      gap: 4px;
      padding: 5px;
      background: #fff;
    }
    .summary-card {
      --filter-accent: var(--filter-total);
      --filter-bg: #f1f5f9;
      --filter-soft: color-mix(in srgb, var(--filter-accent) 8%, #ffffff);
      appearance: none;
      position: relative;
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      align-items: center;
      gap: 4px 8px;
      min-width: 0;
      min-height: 48px;
      padding: 8px 10px 9px;
      border: 1px solid transparent;
      border-radius: var(--radius-sm);
      background: transparent;
      color: var(--text);
      text-align: left;
      cursor: pointer;
      transition: background-color 140ms ease, color 140ms ease;
    }
    .summary-card::before {
      content: "";
      position: absolute;
      left: -3px;
      top: 11px;
      bottom: 11px;
      width: 1px;
      border-radius: 999px;
      background: rgba(184, 194, 208, 0.54);
      pointer-events: none;
    }
    .summary-card:first-child::before,
    .summary-card.is-active::before,
    .summary-card.is-active + .summary-card::before {
      opacity: 0;
    }
    .summary-card::after {
      content: "";
      position: absolute;
      left: 10px;
      right: 10px;
      bottom: 2px;
      height: 2px;
      border-radius: 999px;
      background: var(--filter-accent);
      opacity: 0;
      transition: opacity 140ms ease;
    }
    .summary-card-total,
    .sticky-filter-all,
    .table-filter-all { --filter-accent: var(--filter-total); --filter-bg: #f1f5f9; --filter-border: #cbd5e1; }
    .summary-card-active,
    .sticky-filter-active,
    .table-filter-active { --filter-accent: var(--filter-active); --filter-bg: #ecfdf5; --filter-border: #a7f3d0; }
    .summary-card-available,
    .sticky-filter-available,
    .table-filter-available { --filter-accent: var(--filter-available); --filter-bg: #f0fdf4; --filter-border: #bbf7d0; }
    .summary-card-exhausted,
    .sticky-filter-exhausted,
    .table-filter-exhausted { --filter-accent: var(--filter-exhausted); --filter-bg: #fff7ed; --filter-border: #fed7aa; }
    .summary-card-unknown,
    .sticky-filter-unknown,
    .table-filter-unknown { --filter-accent: var(--filter-unknown); --filter-bg: #f8fafc; --filter-border: #cbd5e1; }
    .summary-card-invalid,
    .sticky-filter-invalid,
    .table-filter-invalid { --filter-accent: var(--filter-invalid); --filter-bg: #fef2f2; --filter-border: #fecaca; }
    .summary-card-source_missing,
    .sticky-filter-source_missing,
    .table-filter-source_missing { --filter-accent: var(--filter-missing); --filter-bg: #fffbeb; --filter-border: #fde68a; }
    .summary-card:not(.is-active):hover {
      border-radius: var(--radius-sm);
      background: color-mix(in srgb, var(--filter-bg) 62%, #ffffff);
    }
    .summary-card.is-active {
      border-color: transparent;
      border-radius: var(--radius-sm);
      background: var(--filter-bg);
      z-index: 1;
    }
    .summary-card.is-active::after { opacity: 1; }
    .summary-label {
      min-width: 0;
      color: var(--muted);
      font-size: 12px;
      font-weight: 640;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .summary-value {
      color: var(--filter-accent);
      font-size: 18px;
      line-height: 1;
      font-weight: 760;
      letter-spacing: -0.03em;
    }
    .summary-card.has-subtext .summary-label {
      align-self: end;
    }
    .summary-card.has-subtext .summary-value {
      grid-row: 1 / 3;
      align-self: center;
    }
    .summary-card.has-subtext .summary-subtext {
      grid-column: 1 / 2;
      grid-row: 2;
      align-self: start;
    }
    .summary-subtext {
      grid-column: 1 / -1;
      min-width: 0;
      overflow: hidden;
      color: var(--filter-accent);
      font-size: 11px;
      font-weight: 650;
      line-height: 1.2;
      text-overflow: ellipsis;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    .panel {
      position: relative;
      isolation: isolate;
      overflow: hidden;
    }
    .table-panel { display: grid; gap: 0; }
    .panel-head-meta {
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      align-items: center;
      gap: 6px;
    }
    .table-filter-pill,
    .table-row-count {
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      padding: 0 8px;
      border: 1px solid var(--line);
      border-radius: var(--radius-sm);
      background: #fff;
      color: var(--muted);
      font-size: 12px;
      font-weight: 640;
      white-space: nowrap;
    }
    .table-filter-pill {
      border-color: transparent;
      background: var(--filter-bg, #fff);
      color: var(--filter-accent, var(--text));
    }
    .sticky-table-shell {
      position: fixed;
      top: 0;
      left: 0;
      z-index: 30;
      width: 0;
      overflow: hidden;
      pointer-events: none;
      background: #fff;
      border: 1px solid var(--line);
      border-top: 0;
      box-shadow: 0 3px 10px rgba(23, 23, 23, 0.08);
    }
    .sticky-table-shell.is-visible { pointer-events: auto; }
    .sticky-table-toolbar {
      display: flex;
      align-items: center;
      gap: 8px;
      min-height: 30px;
      padding: 0 10px;
      border-bottom: 1px solid var(--line);
      background: var(--panel-muted);
    }
    .sticky-table-toolbar-label {
      flex: none;
      color: var(--muted-soft);
      font-size: 11px;
      font-weight: 650;
      white-space: nowrap;
    }
    .sticky-filter-list {
      display: flex;
      flex-wrap: nowrap;
      align-items: center;
      gap: 4px;
      min-width: 0;
      overflow: hidden;
    }
    .sticky-filter-chip {
      appearance: none;
      position: relative;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 22px;
      padding: 0 8px;
      border: 0;
      border-radius: var(--radius-sm);
      background: transparent;
      color: var(--filter-accent);
      font-size: 11px;
      font-weight: 650;
      line-height: 1;
      white-space: nowrap;
      cursor: pointer;
      transition: background-color 140ms ease, color 140ms ease;
    }
    .sticky-filter-chip::before {
      content: "";
      position: absolute;
      left: -3px;
      top: 5px;
      bottom: 5px;
      width: 1px;
      border-radius: 999px;
      background: rgba(184, 194, 208, 0.58);
      pointer-events: none;
    }
    .sticky-filter-chip:first-child::before,
    .sticky-filter-chip.is-active::before,
    .sticky-filter-chip.is-active + .sticky-filter-chip::before {
      opacity: 0;
    }
    .sticky-filter-chip:hover,
    .sticky-filter-chip.is-active {
      background: var(--filter-bg);
      color: var(--filter-accent);
    }
    .sticky-table-scroll { overflow: hidden; background: #fff; }
    .sticky-table,
    table {
      width: 100%;
      min-width: 1040px;
      border-collapse: separate;
      border-spacing: 0;
      table-layout: fixed;
    }
    .sticky-table { transform: translateX(0); will-change: transform; }
    .table-wrap {
      overflow-x: auto;
      overflow-y: hidden;
      border-radius: 0 0 var(--radius) var(--radius);
    }
    .mobile-list-wrap { display: none; overflow: hidden; }
    .mobile-list { display: grid; }
    thead,
    .sticky-table thead { background: var(--panel-muted); }
    th,
    td {
      padding: 7px 10px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }
    th {
      position: sticky;
      top: 0;
      z-index: 1;
      padding: 0;
      background: var(--panel-muted);
      background-clip: padding-box;
    }
    .sticky-table th {
      position: static;
      padding: 0;
      border-bottom: 0;
      background: transparent;
    }
    .sort-button {
      appearance: none;
      box-sizing: border-box;
      display: flex;
      align-items: center;
      justify-content: flex-start;
      gap: 4px;
      width: 100%;
      height: 100%;
      min-width: 0;
      min-height: 34px;
      padding: 0 10px;
      border: 0;
      background: transparent;
      color: var(--muted);
      text-align: left;
      font-size: 11px;
      font-weight: 680;
      line-height: 1.2;
      cursor: pointer;
      transition: color 140ms ease, background-color 140ms ease;
    }
    .sort-button:hover,
    .sort-button.is-active {
      color: var(--text);
      background: #fff;
    }
    .sort-indicator {
      min-width: 12px;
      color: var(--text);
      font-weight: 720;
    }
    .col-email { width: 32%; min-width: 280px; }
    .col-lifecycle { width: 112px; min-width: 112px; white-space: nowrap; }
    .col-remaining { width: 132px; min-width: 132px; }
    .col-reset { width: 170px; min-width: 170px; white-space: nowrap; }
    .col-last-checked { width: 150px; min-width: 150px; white-space: nowrap; }
    .col-note { width: 280px; white-space: normal; }
    tbody tr { transition: background-color 120ms ease; }
    tbody tr:hover { background: #fbfaf7; }
    .mono {
      font-family: "IBM Plex Mono", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 12px;
    }
    .cell-primary {
      min-width: 0;
      max-width: 100%;
      color: var(--text);
      font-weight: 650;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .cell-secondary {
      margin-top: 4px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
      word-break: break-word;
    }
    .cell-note {
      color: var(--text);
      font-size: 12px;
      line-height: 1.45;
      overflow-wrap: anywhere;
      white-space: normal;
      word-break: break-word;
    }
    .cell-secondary.is-truncate {
      display: -webkit-box;
      -webkit-box-orient: vertical;
      -webkit-line-clamp: 2;
      overflow: hidden;
    }
    .cell-tag-row {
      display: flex;
      flex-wrap: wrap;
      gap: 4px;
      margin-top: 5px;
    }
    .plan-tag {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 18px;
      padding: 0 6px;
      border: 1px solid var(--plan-unknown-border);
      border-radius: 5px;
      background: var(--plan-unknown-bg);
      color: var(--plan-unknown-text);
      font-size: 11px;
      font-weight: 680;
      line-height: 1;
      text-transform: lowercase;
      white-space: nowrap;
    }
    .plan-tag-free { border-color: var(--plan-free-border); background: var(--plan-free-bg); color: var(--plan-free-text); }
    .plan-tag-team { border-color: var(--plan-team-border); background: var(--plan-team-bg); color: var(--plan-team-text); }
    .plan-tag-plus { border-color: var(--plan-plus-border); background: var(--plan-plus-bg); color: var(--plan-plus-text); }
    .plan-tag-pro { border-color: var(--plan-pro-border); background: var(--plan-pro-bg); color: var(--plan-pro-text); }
    .plan-tag-business { border-color: var(--plan-business-border); background: var(--plan-business-bg); color: var(--plan-business-text); }
    .plan-tag-enterprise { border-color: var(--plan-enterprise-border); background: var(--plan-enterprise-bg); color: var(--plan-enterprise-text); }
    .plan-tag-unknown { border-color: var(--plan-unknown-border); background: var(--plan-unknown-bg); color: var(--plan-unknown-text); }
    .remaining-cell {
      display: grid;
      gap: 5px;
      min-width: 104px;
    }
    .remaining-value {
      color: var(--text);
      font-size: 13px;
      font-weight: 760;
    }
    .remaining-track {
      display: block;
      width: 100%;
      height: 5px;
      overflow: hidden;
      border-radius: 999px;
      background: #dbe3ee;
    }
    .remaining-fill {
      display: block;
      height: 100%;
      border-radius: inherit;
      background: var(--available);
    }
    .remaining-cell.is-medium .remaining-fill { background: #d97706; }
    .remaining-cell.is-low .remaining-fill { background: #dc2626; }
    .remaining-cell.is-empty .remaining-fill { width: 0 !important; background: #a8a29e; }
    .cell-time { display: flex; flex-direction: column; gap: 1px; }
    .cell-time-date,
    .cell-time-clock,
    .cell-time-countdown { display: block; white-space: nowrap; }
    .cell-time-date { color: var(--text); font-weight: 650; }
    .cell-time-clock { color: var(--muted); font-size: 11px; }
    .cell-time-countdown {
      color: var(--filter-active);
      font-size: 11px;
      font-weight: 680;
      font-variant-numeric: tabular-nums;
    }
    .status-pill {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 24px;
      padding: 0 8px;
      border: 1px solid currentColor;
      border-radius: var(--radius-sm);
      background: #fff;
      font-size: 12px;
      font-weight: 680;
      line-height: 1;
      white-space: nowrap;
    }
    .status-lifecycle-active { color: var(--active); }
    .status-lifecycle-invalid { color: var(--invalid); }
    .status-lifecycle-source_missing { color: var(--missing); }
    .status-phase-idle { color: var(--unknown); }
    .status-phase-scanning { color: #57534e; }
    .status-phase-querying { color: var(--active); }
    .status-phase-reconciling { color: #7c2d12; }
    .status-phase-sleeping { color: var(--missing); }
    .status-phase-error { color: var(--invalid); }
    .muted { color: var(--muted); }
    .error-banner {
      display: none;
      padding: 7px 10px;
      border: 1px solid #fecaca;
      border-radius: var(--radius);
      color: var(--invalid);
      background: #fff1f2;
    }
    .empty {
      padding: 24px 12px;
      text-align: center;
      color: var(--muted);
      background: #fff;
    }
    .modal {
      position: fixed;
      inset: 0;
      z-index: 40;
      display: none;
      align-items: center;
      justify-content: center;
      padding: 20px;
      background: rgba(23, 23, 23, 0.36);
    }
    .modal.is-visible { display: flex; }
    .modal-card {
      width: min(100%, 420px);
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: #fff;
      box-shadow: 0 12px 28px rgba(23, 23, 23, 0.14);
    }
    .modal-title {
      margin: 0;
      color: var(--text);
      font-size: 16px;
      line-height: 1.2;
      font-weight: 720;
    }
    .modal-text {
      margin: 10px 0 0;
      color: var(--muted);
      white-space: pre-line;
    }
    .modal-actions {
      display: flex;
      justify-content: flex-end;
      gap: 8px;
      margin-top: 16px;
    }
    .mobile-list-wrap .empty {
      padding: 14px 10px;
      border: 0;
      background: transparent;
    }
    .mobile-account-item {
      display: grid;
      gap: 5px;
      padding: 8px 10px;
      border-bottom: 1px solid var(--line);
      background: #fff;
    }
    .mobile-account-item:last-child { border-bottom: 0; }
    .mobile-account-top {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 8px;
      min-width: 0;
    }
    .mobile-account-main { min-width: 0; display: grid; gap: 4px; }
    .mobile-account-email {
      color: var(--text);
      font-size: 12px;
      line-height: 1.25;
      font-weight: 720;
      word-break: break-all;
    }
    .mobile-account-inline,
    .mobile-account-tags {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 4px 6px;
      min-width: 0;
    }
    .mobile-metric {
      display: inline-flex;
      align-items: baseline;
      gap: 3px;
      min-width: 0;
      max-width: 100%;
    }
    .mobile-metric + .mobile-metric::before {
      content: "|";
      color: var(--line-strong);
      margin-right: 2px;
    }
    .mobile-metric-label {
      flex: none;
      color: var(--muted-soft);
      font-size: 10px;
      font-weight: 650;
    }
    .mobile-metric-value {
      min-width: 0;
      max-width: 130px;
      overflow: hidden;
      color: var(--text);
      font-size: 11px;
      font-weight: 650;
      line-height: 1.25;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .mobile-account-foot { display: grid; gap: 0; }
    .mobile-account-extra {
      min-width: 0;
      color: var(--muted);
      font-size: 11px;
      line-height: 1.35;
      overflow-wrap: anywhere;
      white-space: normal;
      word-break: break-word;
    }
    .mobile-account-extra b { color: var(--text); font-weight: 650; }
    @media (max-width: 1120px) {
      .page { padding-inline: 12px; }
      .app-toolbar { grid-template-columns: 1fr; align-items: start; }
      .toolbar-actions,
      .run-meta { justify-content: flex-start; }
      .run-line { grid-template-columns: auto minmax(160px, 1fr); }
      .run-meta { grid-column: 1 / -1; }
      .summary { grid-template-columns: repeat(4, minmax(0, 1fr)); }
    }
    @media (max-width: 900px) {
      .page { padding: 10px 10px 22px; }
      .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .sticky-table-shell { display: none !important; }
      .panel-head { align-items: flex-start; flex-direction: column; }
      .summary-block .section-heading { align-items: center; flex-direction: row; }
    }
    @media (max-width: 768px) {
      body { overflow-x: hidden; }
      .page { padding: 8px; }
      .page-header { gap: 6px; margin-bottom: 8px; }
      .app-toolbar { padding: 7px 8px; }
      .brand-row { justify-content: space-between; }
      .toolbar-subtitle { display: none; }
      .toolbar-actions { width: 100%; }
      .toolbar-actions .action-button { flex: 1 1 0; }
      .status-pill { min-height: 22px; padding: 0 6px; font-size: 11px; }
      .action-button { min-height: 28px; padding: 0 8px; font-size: 12px; }
      .run-line { grid-template-columns: 1fr; gap: 6px; }
      .run-progress-label { justify-content: space-between; }
      .progress-count { font-size: 16px; }
      .run-meta { gap: 3px 9px; }
      .run-account b { max-width: 190px; }
      .summary-card { min-height: 44px; padding: 7px 8px; }
      .summary-value { font-size: 16px; }
      .panel-head,
      .section-heading { min-height: 34px; padding: 7px 8px; }
      .table-filter-pill,
      .table-row-count { min-height: 22px; padding: 0 6px; font-size: 11px; }
      .mobile-list-wrap { display: block; }
      .table-wrap { display: none; }
      .mobile-list { display: block; }
      .mobile-account-tags .plan-tag { min-height: 16px; padding: 0 5px; font-size: 10px; }
    }
    @media (max-width: 420px) {
      .toolbar-stats { gap: 4px 8px; }
      .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .mobile-metric-value { max-width: 108px; }
    }
    @media (prefers-reduced-motion: reduce) {
      *, *::before, *::after {
        animation: none !important;
        transition: none !important;
        scroll-behavior: auto !important;
      }
    }
  """.strip()



def _render_index_header() -> str:
    return """
    <div class="page-header">
      <a href="#accounts-panel" class="skip-link">跳到账号列表</a>

      <section class="app-toolbar surface" aria-label="usage-monitor 工具栏">
        <div class="brand-row">
          <h1 class="title">usage-monitor</h1>
        </div>
        <div class="run-line toolbar-progress" aria-label="本轮进度">
          <div class="run-progress-label">
            <span id="progress-phase" class="status-pill status-phase-idle">idle</span>
            <strong id="progress-count" class="progress-count">-</strong>
            <span id="progress-percent" class="progress-percent">0%</span>
          </div>
          <div class="progress-track" aria-hidden="true">
            <div id="progress-bar" class="progress-bar"></div>
          </div>
          <div class="run-meta" aria-label="进度统计">
            <span class="run-meta-item run-account">当前 <b id="progress-account">-</b></span>
            <span class="run-meta-item">下轮 <b id="progress-next-round">-</b></span>
          </div>
        </div>
        <div class="toolbar-actions">
          <button id="scan-trigger-button" type="button" class="action-button" disabled>手动开始扫描</button>
          <button id="scan-stop-button" type="button" class="action-button action-button-danger" disabled>停止本轮</button>
        </div>
        <div class="hidden-runtime-fields" aria-hidden="true">
          <span id="current-filter">active</span>
          <strong id="toolbar-total">0</strong>
          <span id="generated-at">-</span>
          <span id="progress-total-scanned">-</span>
          <span id="progress-total-candidates">-</span>
          <span id="progress-skipped">-</span>
          <span id="progress-subsummary" class="progress-subsummary">-</span>
          <span id="progress-note" class="progress-note" role="status" aria-live="polite"></span>
          <span id="scan-action-note" class="action-note" role="status" aria-live="polite"></span>
          <span id="progress-source">-</span>
          <span id="progress-started">-</span>
          <span id="progress-heartbeat">-</span>
          <span id="progress-finished">-</span>
        </div>
      </section>

      <div id="error-banner" class="error-banner" role="alert" aria-live="assertive"></div>

      <section class="summary-block" aria-labelledby="summary-title">
        <div class="section-heading">
          <div class="summary-heading-main">
            <h2 id="summary-title" class="section-title">账号总览</h2>
            <span id="availability-rate" class="availability-rate is-medium" title="available / active">
              <span class="availability-rate-label">可用率</span>
              <strong id="availability-rate-value">-</strong>
            </span>
          </div>
        </div>
        <section id="summary" class="summary" aria-label="账号汇总"></section>
      </section>
    </div>
  """.strip()



def _render_index_table(table_header_row: str) -> str:
    return f"""
    <section id="accounts-panel" class="panel table-panel">
      <div class="panel-head">
        <h2 id="accounts-panel-title" class="panel-title">账号列表</h2>
        <div class="panel-head-meta">
          <span id="table-summary-filter" class="table-filter-pill">filter: active</span>
          <span id="table-row-count" class="table-row-count">0 条</span>
        </div>
      </div>
      <div id="sticky-table-shell" class="sticky-table-shell" aria-hidden="true" hidden>
        <div class="sticky-table-toolbar">
          <span class="sticky-table-toolbar-label">筛选</span>
          <div id="sticky-filter-list" class="sticky-filter-list" role="toolbar" aria-label="快捷筛选"></div>
        </div>
        <div id="sticky-table-scroll" class="sticky-table-scroll">
          <table id="sticky-table" class="sticky-table">
            <thead id="sticky-table-head">
{table_header_row}
            </thead>
          </table>
        </div>
      </div>
      <div id="mobile-list-wrap" class="mobile-list-wrap" aria-live="polite" aria-busy="false" aria-labelledby="accounts-panel-title">
        <div id="mobile-list" class="mobile-list" role="list" aria-label="usage-monitor 账号状态列表">
          <div class="empty">加载中...</div>
        </div>
      </div>
      <div id="table-wrap" class="table-wrap" aria-live="polite" aria-busy="false">
        <table id="main-table">
          <caption class="sr-only">usage-monitor 账号状态列表</caption>
          <thead id="main-table-head">
{table_header_row}
          </thead>
          <tbody id="rows">
            <tr>
              <td colspan="{_TABLE_COLUMN_COUNT}" class="empty">加载中...</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  """.strip()



def _render_confirm_modal() -> str:
    return """
  <div id="confirm-modal" class="modal" aria-hidden="true">
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="confirm-modal-title" aria-describedby="confirm-modal-text">
      <h2 id="confirm-modal-title" class="modal-title">确认操作</h2>
      <p id="confirm-modal-text" class="modal-text"></p>
      <div class="modal-actions">
        <button id="confirm-modal-cancel" type="button" class="action-button action-button-secondary">取消</button>
        <button id="confirm-modal-confirm" type="button" class="action-button">确认</button>
      </div>
    </div>
  </div>
  """.strip()


def _render_index_script(
    *,
    lifecycle_order_js: str,
    quota_order_js: str,
    url_prefix_js: str,
    initial_dashboard_js: str,
    initial_progress_js: str,
) -> str:
    return f"""
    const STICKY_TABLE_TOP = 0;
    const TABLE_CARD_BREAKPOINT = 900;
    const MOBILE_LAYOUT_BREAKPOINT = 768;
    const TABLE_COLUMN_COUNT = {_TABLE_COLUMN_COUNT};
    const STICKY_QUICK_FILTERS = ["all", "active", "available", "exhausted", "unknown", "invalid", "source_missing"];
    const initialDashboardPayload = {initial_dashboard_js};
    const initialProgressPayload = {initial_progress_js};
    const state = {{
      filter: "active",
      eventSource: null,
      eventStreamConnected: false,
      reconnectMessageShown: false,
      lastProgressPhase: String(initialProgressPayload.phase || ""),
      lastProgressPayload: initialProgressPayload,
      controlRequestInFlight: "",
      confirmResolver: null,
      lastFocusedElement: null,
      sortKey: "last_checked_at_utc",
      sortDirection: "desc",
      items: Array.isArray(initialDashboardPayload.items) ? initialDashboardPayload.items : [],
      summary: {{}},
      dashboardRevision: Number(initialDashboardPayload.accounts_revision || 0),
      nextRoundAtUtc: String(initialProgressPayload.next_round_at_utc || ""),
      nextRoundCountdownEnabled: String(initialProgressPayload.phase || "") === "sleeping",
      lastRenderedMode: ""
    }};

    // 常量与标签
    const lifecycleOrder = {lifecycle_order_js};
    const quotaOrder = {quota_order_js};
    const urlPrefix = {url_prefix_js};
    const labels = {{
      lifecycle: {{
        active: "active",
        invalid: "invalid",
        source_missing: "source_missing"
      }},
      quota: {{
        available: "available",
        exhausted: "exhausted",
        unknown: "unknown"
      }},
      progress: {{
        idle: "idle",
        scanning: "scanning",
        querying: "querying",
        reconciling: "reconciling",
        sleeping: "sleeping",
        error: "error"
      }},
      summary: {{
        total: "total",
        active: "active",
        available: "available",
        exhausted: "exhausted",
        unknown: "unknown",
        invalid: "invalid",
        source_missing: "source_missing"
      }},
      filter: {{
        all: "all",
        active: "active",
        available: "available",
        exhausted: "exhausted",
        unknown: "unknown",
        invalid: "invalid",
        source_missing: "source_missing"
      }}
    }};
    const summaryDescriptions = {{
      total: "全部账号",
      active: "生命周期正常",
      available: "主额度可用",
      exhausted: "主额度已耗尽",
      unknown: "额度状态待确认",
      invalid: "已失效",
      source_missing: "源文件已缺失"
    }};
    const progressDescriptions = {{
      idle: "当前空闲，等待下一轮或手动开始。",
      scanning: "正在整理账号列表。",
      querying: "正在查询账号额度状态。",
      reconciling: "正在整理本轮结果。",
      sleeping: "本轮已结束，等待下一个自动周期。",
      error: "运行出现异常，可手动开始下一轮重试。"
    }};
    const shanghaiDateTimeFormatter = new Intl.DateTimeFormat("zh-CN", {{
      timeZone: "Asia/Shanghai",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false
    }});
    const shanghaiTimeTextCache = new Map();

    // 基础工具
    function escapeHtml(value) {{
      return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }}

    function getPlanTagClass(planType) {{
      const normalized = String(planType || "").trim().toLowerCase();
      if (normalized === "free") {{
        return "plan-tag-free";
      }}
      if (normalized === "team") {{
        return "plan-tag-team";
      }}
      if (normalized === "plus") {{
        return "plan-tag-plus";
      }}
      if (normalized === "pro") {{
        return "plan-tag-pro";
      }}
      if (normalized === "business") {{
        return "plan-tag-business";
      }}
      if (normalized === "enterprise") {{
        return "plan-tag-enterprise";
      }}
      return "plan-tag-unknown";
    }}

    function renderPlanTag(planType) {{
      const normalized = String(planType || "").trim().toLowerCase() || "unknown";
      return `<span class="plan-tag ${{getPlanTagClass(normalized)}}">${{escapeHtml(normalized)}}</span>`;
    }}

    function getPlanTypePriority(planType) {{
      const normalized = String(planType || "").trim().toLowerCase();
      const priorityMap = {{
        enterprise: 10,
        business: 20,
        pro: 30,
        plus: 40,
        team: 50,
        unknown: 900,
        free: 1000,
      }};
      if (Object.prototype.hasOwnProperty.call(priorityMap, normalized)) {{
        return priorityMap[normalized];
      }}
      return 800;
    }}

    function withPrefix(path) {{
      return `${{urlPrefix}}${{path}}`;
    }}

    function formatUtcToShanghai(value, empty = "-") {{
      const text = String(value || "").trim();
      if (!text) {{
        return empty;
      }}

      const cacheKey = empty === "-" ? text : "";
      if (cacheKey && shanghaiTimeTextCache.has(cacheKey)) {{
        return shanghaiTimeTextCache.get(cacheKey) || empty;
      }}

      const date = new Date(text);
      if (Number.isNaN(date.getTime())) {{
        return empty;
      }}

      const mapped = {{}};
      shanghaiDateTimeFormatter.formatToParts(date).forEach((part) => {{
        if (part.type !== "literal") {{
          mapped[part.type] = part.value;
        }}
      }});
      const formatted = `${{mapped.year || "0000"}}-${{mapped.month || "00"}}-${{mapped.day || "00"}} ${{mapped.hour || "00"}}:${{mapped.minute || "00"}}:${{mapped.second || "00"}}`;

      if (cacheKey) {{
        if (shanghaiTimeTextCache.size >= 4096) {{
          shanghaiTimeTextCache.clear();
        }}
        shanghaiTimeTextCache.set(cacheKey, formatted);
      }}
      return formatted;
    }}

    function formatCompactDateTimeText(value) {{
      const text = formatUtcToShanghai(value, "");
      if (!text) {{
        return "-";
      }}
      const parts = text.split(" ");
      const datePart = parts[0] || "";
      const timeMatch = text.match(/\\b\\d{{2}}:\\d{{2}}/);
      const shortDate = /^\\d{{4}}-\\d{{2}}-\\d{{2}}$/.test(datePart) ? datePart.slice(5) : datePart;
      if (timeMatch) {{
        return `${{shortDate}} ${{timeMatch[0]}}`.trim();
      }}
      return shortDate || text;
    }}


    // 倒计时只依赖服务端返回的 UTC 时间；前端本地刷新，不增加接口压力。
    function parseUtcMillis(value) {{
      const text = String(value || "").trim();
      if (!text) {{
        return null;
      }}
      const millis = Date.parse(text);
      return Number.isFinite(millis) ? millis : null;
    }}

    function pad2(value) {{
      return String(Math.max(0, Math.floor(value))).padStart(2, "0");
    }}

    function formatDurationSeconds(totalSeconds) {{
      const safeSeconds = Math.max(0, Math.floor(Number(totalSeconds) || 0));
      const hours = Math.floor(safeSeconds / 3600);
      const minutes = Math.floor((safeSeconds % 3600) / 60);
      const seconds = safeSeconds % 60;
      if (hours > 0) {{
        return `${{hours}}:${{pad2(minutes)}}:${{pad2(seconds)}}`;
      }}
      return `${{pad2(minutes)}}:${{pad2(seconds)}}`;
    }}

    function formatDurationSecondsFromNow(value) {{
      const targetMillis = parseUtcMillis(value);
      if (targetMillis === null) {{
        return "";
      }}
      return formatDurationSeconds(Math.ceil((targetMillis - Date.now()) / 1000));
    }}

    function formatDurationMinutesFromNow(value) {{
      const targetMillis = parseUtcMillis(value);
      if (targetMillis === null) {{
        return "";
      }}
      const remainingMs = targetMillis - Date.now();
      if (remainingMs <= 0) {{
        return "已重置";
      }}
      const totalMinutes = Math.max(1, Math.ceil(remainingMs / 60000));
      const days = Math.floor(totalMinutes / 1440);
      const hours = Math.floor((totalMinutes % 1440) / 60);
      const minutes = totalMinutes % 60;
      if (days > 0) {{
        return `剩 ${{days}}天 ${{hours}}小时`;
      }}
      if (hours > 0) {{
        return `剩 ${{hours}}小时 ${{minutes}}分`;
      }}
      return `剩 ${{minutes}}分`;
    }}

    function refreshNextRoundCountdown() {{
      const element = document.getElementById("progress-next-round");
      if (!element) {{
        return;
      }}
      const target = String(state.nextRoundAtUtc || "").trim();
      const enabled = Boolean(state.nextRoundCountdownEnabled && target);
      element.classList.toggle("is-countdown", enabled);
      element.title = formatUtcToShanghai(target, "");
      if (!target) {{
        element.textContent = "-";
        return;
      }}
      if (enabled) {{
        const countdown = formatDurationSecondsFromNow(target);
        element.textContent = countdown || "即将开始";
        return;
      }}
      element.textContent = formatCompactDateTimeText(target);
    }}

    function refreshResetCountdowns() {{
      document.querySelectorAll("[data-reset-countdown]").forEach((element) => {{
        const target = element.getAttribute("data-countdown-target") || "";
        const countdown = formatDurationMinutesFromNow(target);
        element.textContent = countdown || "-";
        element.setAttribute("title", formatUtcToShanghai(target, "") || "-");
      }});
    }}

    function refreshLiveCountdowns() {{
      refreshNextRoundCountdown();
      refreshResetCountdowns();
      refreshExhaustedRecoveryCountdown();
    }}

    function formatProgressPercent(value) {{
      const parsed = Number(value);
      if (!Number.isFinite(parsed)) {{
        return "0%";
      }}
      if (Math.abs(parsed - Math.round(parsed)) < 0.05) {{
        return `${{Math.round(parsed)}}%`;
      }}
      return `${{parsed.toFixed(1)}}%`;
    }}

    function clampPercent(value) {{
      const parsed = Number(value);
      if (!Number.isFinite(parsed)) {{
        return 0;
      }}
      return Math.max(0, Math.min(parsed, 100));
    }}

    function getRemainingToneClass(value) {{
      const parsed = Number(value);
      if (!Number.isFinite(parsed)) {{
        return "is-empty";
      }}
      if (parsed <= 15) {{
        return "is-low";
      }}
      if (parsed <= 50) {{
        return "is-medium";
      }}
      return "is-high";
    }}

    function setProgressNote(message, isError = false) {{
      const note = document.getElementById("progress-note");
      note.classList.toggle("is-visible", Boolean(message));
      note.classList.toggle("is-error", Boolean(message) && isError);
      note.setAttribute("role", Boolean(message) && isError ? "alert" : "status");
      note.textContent = message || "";
    }}

    function setActionNote(message, isError = false) {{
      const note = document.getElementById("scan-action-note");
      note.classList.toggle("is-error", Boolean(message) && isError);
      note.textContent = message || "";
    }}

    function updateTableMeta(count = 0) {{
      const currentFilter = String(state.filter || "all");
      const filterElement = document.getElementById("table-summary-filter");
      filterElement.textContent = `filter: ${{labels.filter[currentFilter] || currentFilter}}`;
      filterElement.className = `table-filter-pill table-filter-${{currentFilter}}`;
      document.getElementById("table-row-count").textContent = `${{count}} 条`;
    }}

    function renderRemainingCell(item) {{
      const rawValue = item.remaining_percent_value;
      if (rawValue === null || rawValue === undefined || rawValue === "") {{
        return `
          <div class="remaining-cell is-empty">
            <strong class="remaining-value">-</strong>
            <span class="remaining-track" aria-hidden="true"><span class="remaining-fill" style="width:0%"></span></span>
          </div>
        `;
      }}
      const value = clampPercent(rawValue);
      const width = value > 0 ? Math.max(value, 6) : 0;
      return `
        <div class="remaining-cell ${{getRemainingToneClass(value)}}">
          <strong class="remaining-value">${{escapeHtml(item.remaining_percent_text || "-")}}</strong>
          <span class="remaining-track" aria-hidden="true"><span class="remaining-fill" style="width:${{width}}%"></span></span>
        </div>
      `;
    }}

    function renderDateTimeCell(value, withCountdown = false) {{
      const target = String(value || "").trim();
      const text = formatUtcToShanghai(target, "");
      if (!text || text === "-") {{
        return '<div class="cell-time"><span class="cell-time-date">-</span></div>';
      }}
      const countdownHtml = withCountdown
        ? `<span class="cell-time-countdown" data-reset-countdown data-countdown-target="${{escapeHtml(target)}}">${{escapeHtml(formatDurationMinutesFromNow(target) || "-")}}</span>`
        : "";
      const parts = text.split(" ");
      if (parts.length >= 2) {{
        const datePart = parts[0] || "-";
        const timePart = parts.slice(1).join(" ") || "";
        return `
          <div class="cell-time">
            <span class="cell-time-date">${{escapeHtml(datePart)}}</span>
            <span class="cell-time-clock">${{escapeHtml(timePart)}}</span>
            ${{countdownHtml}}
          </div>
        `;
      }}
      return `<div class="cell-time"><span class="cell-time-date">${{escapeHtml(text)}}</span>${{countdownHtml}}</div>`;
    }}

    function renderMobileMetric(label, value, extraClass = "") {{
      const text = String(value || "-").trim() || "-";
      const className = extraClass ? `mobile-metric-value ${{extraClass}}` : "mobile-metric-value";
      return `
        <span class="mobile-metric">
          <span class="mobile-metric-label">${{escapeHtml(label)}}</span>
          <span class="${{className}}" title="${{escapeHtml(text)}}">${{escapeHtml(text)}}</span>
        </span>
      `;
    }}

    function renderMobileResetMetric(value) {{
      const target = String(value || "").trim();
      const text = formatDurationMinutesFromNow(target) || formatCompactDateTimeText(target);
      const title = formatUtcToShanghai(target, "") || text;
      return `
        <span class="mobile-metric">
          <span class="mobile-metric-label">重</span>
          <span class="mobile-metric-value mono" data-reset-countdown data-countdown-target="${{escapeHtml(target)}}" title="${{escapeHtml(title)}}">${{escapeHtml(text)}}</span>
        </span>
      `;
    }}

    function renderMobileRows(items) {{
      const container = document.getElementById("mobile-list");
      if (!container) {{
        return;
      }}
      if (!Array.isArray(items) || items.length === 0) {{
        container.innerHTML = '<div class="empty">当前筛选下没有账号数据。</div>';
        return;
      }}

      const rows = items.map((item) => {{
        const remainingText = item.remaining_percent_text || "-";
        const checkedText = formatCompactDateTimeText(item.last_checked_at_utc || "");
        const extraParts = [];
        if (item.note && String(item.note).trim() && String(item.note).trim() !== "-") {{
          extraParts.push(`<b>备</b> ${{escapeHtml(String(item.note).trim())}}`);
        }}
        const footerHtml = extraParts.length
          ? `<div class="mobile-account-foot"><div class="mobile-account-extra" title="${{escapeHtml(extraParts.map((part) => part.replace(/<[^>]+>/g, "")).join(" · "))}}">${{extraParts.join(" · ")}}</div></div>`
          : "";

        return `
          <div class="mobile-account-item" role="listitem">
            <div class="mobile-account-top">
              <div class="mobile-account-main">
                <div class="mobile-account-email">${{escapeHtml(item.email || "-")}}</div>
                <div class="mobile-account-inline">
                  <div class="mobile-account-tags">
                    ${{renderPlanTag(item.plan_type)}}
                  </div>
                  ${{renderMobileMetric("余", remainingText, "is-primary")}}
                  ${{renderMobileResetMetric(item.reset_at_utc || "")}}
                  ${{renderMobileMetric("查", checkedText, "mono")}}
                </div>
              </div>
              <span class="status-pill status-lifecycle-${{escapeHtml(item.lifecycle_status)}}">
                ${{escapeHtml(labels.lifecycle[item.lifecycle_status] || item.lifecycle_status || "-")}}
              </span>
            </div>
            ${{footerHtml}}
          </div>
        `;
      }}).join("");
      container.innerHTML = rows;
      refreshResetCountdowns();
    }}

    function renderStickyQuickFilters(summary = state.summary || {{}}) {{
      const container = document.getElementById("sticky-filter-list");
      if (!container) {{
        return;
      }}
      const html = STICKY_QUICK_FILTERS.map((filterKey) => {{
        const summaryKey = filterKey === "all" ? "total" : filterKey;
        const count = Number(summary[summaryKey] ?? 0);
        const active = filterKey === state.filter;
        const label = labels.filter[filterKey] || filterKey;
        return `
          <button
            type="button"
            class="sticky-filter-chip sticky-filter-${{filterKey}}${{active ? " is-active" : ""}}"
            data-sticky-filter="${{filterKey}}"
            aria-pressed="${{active ? "true" : "false"}}"
            aria-label="快捷筛选 ${{label}}，当前${{count}}条"
            title="${{label}} · ${{count}} 条"
          >${{label}}</button>
        `;
      }}).join("");
      container.innerHTML = html;
    }}

    function setStickyHeaderVisibility(visible) {{
      const shell = document.getElementById("sticky-table-shell");
      if (!shell) {{
        return;
      }}
      shell.hidden = !visible;
      shell.classList.toggle("is-visible", visible);
      shell.setAttribute("aria-hidden", visible ? "false" : "true");
    }}

    function syncStickyHeaderScroll() {{
      const tableWrap = document.getElementById("table-wrap");
      const stickyTable = document.getElementById("sticky-table");
      if (!tableWrap || !stickyTable) {{
        return;
      }}
      stickyTable.style.transform = `translateX(-${{tableWrap.scrollLeft}}px)`;
    }}

    function updateStickyHeaderVisibility() {{
      const shell = document.getElementById("sticky-table-shell");
      const mainHead = document.getElementById("main-table-head");
      const mainTable = document.getElementById("main-table");
      if (!shell || !mainHead || !mainTable) {{
        return;
      }}

      if (window.innerWidth <= TABLE_CARD_BREAKPOINT) {{
        setStickyHeaderVisibility(false);
        return;
      }}

      const headRect = mainHead.getBoundingClientRect();
      const tableRect = mainTable.getBoundingClientRect();
      const stickyHeight = shell.offsetHeight || 56;
      const shouldShow =
        headRect.top <= STICKY_TABLE_TOP &&
        tableRect.bottom > STICKY_TABLE_TOP + stickyHeight + 24;

      setStickyHeaderVisibility(shouldShow);
      if (shouldShow) {{
        syncStickyHeaderScroll();
      }}
    }}

    function syncStickyHeaderLayout() {{
      const shell = document.getElementById("sticky-table-shell");
      const mainHead = document.getElementById("main-table-head");
      const stickyHead = document.getElementById("sticky-table-head");
      const mainTable = document.getElementById("main-table");
      const stickyTable = document.getElementById("sticky-table");
      const tableWrap = document.getElementById("table-wrap");
      const tablePanel = tableWrap ? (tableWrap.closest(".table-panel") || tableWrap.closest(".panel")) : null;
      if (!shell || !mainHead || !stickyHead || !mainTable || !stickyTable || !tableWrap || !tablePanel) {{
        return;
      }}

      if (window.innerWidth <= TABLE_CARD_BREAKPOINT) {{
        setStickyHeaderVisibility(false);
        shell.style.width = "";
        shell.style.left = "";
        stickyTable.style.width = "";
        stickyTable.style.transform = "";
        return;
      }}

      const mainHeaders = Array.from(mainHead.querySelectorAll("th"));
      const stickyHeaders = Array.from(stickyHead.querySelectorAll("th"));
      if (mainHeaders.length === 0 || mainHeaders.length !== stickyHeaders.length) {{
        setStickyHeaderVisibility(false);
        return;
      }}

      const panelRect = tablePanel.getBoundingClientRect();
      const shellOuterLeft = Math.floor(panelRect.left) - 3;
      const shellOuterWidth = Math.ceil(panelRect.width) + 6;
      shell.style.left = `${{shellOuterLeft}}px`;
      shell.style.width = `${{Math.max(shellOuterWidth, 0)}}px`;

      let totalWidth = 0;
      mainHeaders.forEach((header, index) => {{
        const width = Math.round(header.getBoundingClientRect().width);
        const stickyHeader = stickyHeaders[index];
        totalWidth += width;
        stickyHeader.style.width = `${{width}}px`;
        stickyHeader.style.minWidth = `${{width}}px`;
        stickyHeader.style.maxWidth = `${{width}}px`;
      }});

      const tableWidth = Math.max(totalWidth, mainTable.scrollWidth, tableWrap.scrollWidth);
      stickyTable.style.width = `${{tableWidth}}px`;
      syncStickyHeaderScroll();
      updateStickyHeaderVisibility();
    }}

    // 确认弹窗
    function toggleConfirmModal(visible) {{
      const modal = document.getElementById("confirm-modal");
      modal.classList.toggle("is-visible", visible);
      modal.setAttribute("aria-hidden", visible ? "false" : "true");
      document.body.classList.toggle("modal-open", visible);
    }}

    function closeConfirmModal(confirmed) {{
      const resolver = state.confirmResolver;
      state.confirmResolver = null;
      toggleConfirmModal(false);
      if (state.lastFocusedElement && typeof state.lastFocusedElement.focus === "function") {{
        state.lastFocusedElement.focus();
      }}
      state.lastFocusedElement = null;
      if (resolver) {{
        resolver(Boolean(confirmed));
      }}
    }}

    function openConfirmModal(config) {{
      state.lastFocusedElement = document.activeElement instanceof HTMLElement ? document.activeElement : null;
      document.getElementById("confirm-modal-title").textContent = config.title || "确认操作";
      document.getElementById("confirm-modal-text").textContent = config.message || "";
      const confirmButton = document.getElementById("confirm-modal-confirm");
      confirmButton.textContent = config.confirmText || "确认";
      confirmButton.className = `action-button${{config.danger ? " action-button-danger" : ""}}`;
      toggleConfirmModal(true);
      window.requestAnimationFrame(() => {{
        document.getElementById("confirm-modal-cancel").focus();
      }});
      return new Promise((resolve) => {{
        state.confirmResolver = resolve;
      }});
    }}

    // 控制动作与实时推送
    function setDashboardBusy(isBusy) {{
      const busy = isBusy ? "true" : "false";
      const tableWrap = document.getElementById("table-wrap");
      const mobileListWrap = document.getElementById("mobile-list-wrap");
      tableWrap.setAttribute("aria-busy", busy);
      if (mobileListWrap) {{
        mobileListWrap.setAttribute("aria-busy", busy);
      }}
    }}

    function getCurrentLayoutMode() {{
      return window.innerWidth <= MOBILE_LAYOUT_BREAKPOINT ? "mobile" : "desktop";
    }}

    function closeEventStream() {{
      if (!state.eventSource) {{
        return;
      }}
      state.eventSource.close();
      state.eventSource = null;
      state.eventStreamConnected = false;
    }}

    function getEventStreamUrl() {{
      const params = new URLSearchParams();
      params.set("filter", "all");
      if (Number.isFinite(state.dashboardRevision) && state.dashboardRevision > 0) {{
        params.set("known_accounts_revision", String(state.dashboardRevision));
        params.set("skip_initial_dashboard", "1");
      }}
      return withPrefix(`/api/events?${{params.toString()}}`);
    }}

    function parseEventPayload(event) {{
      try {{
        return JSON.parse(event.data || "{{}}");
      }} catch (_error) {{
        return null;
      }}
    }}

    function areSummaryEqual(left, right) {{
      const keys = ["total", "active", "available", "exhausted", "unknown", "invalid", "source_missing"];
      return keys.every((key) => Number((left && left[key]) ?? 0) === Number((right && right[key]) ?? 0));
    }}

    function matchesFilter(item, filterName = state.filter) {{
      const currentFilter = String(filterName || "all");
      const lifecycleStatus = String((item && item.lifecycle_status) || "");
      const quotaStatus = String((item && item.quota_status) || "");
      if (currentFilter === "all") {{
        return true;
      }}
      if (currentFilter === "active") {{
        return lifecycleStatus === "active";
      }}
      if (currentFilter === "available" || currentFilter === "exhausted" || currentFilter === "unknown") {{
        return lifecycleStatus === "active" && quotaStatus === currentFilter;
      }}
      if (currentFilter === "invalid" || currentFilter === "source_missing") {{
        return lifecycleStatus === currentFilter;
      }}
      return true;
    }}

    function filterItemsByActiveView(items, filterName = state.filter) {{
      if (!Array.isArray(items) || items.length === 0) {{
        return [];
      }}
      return items.filter((item) => matchesFilter(item, filterName));
    }}

    function updateGeneratedAt(value) {{
      document.getElementById("generated-at").textContent = formatUtcToShanghai(value);
    }}

    function renderDashboardPlaceholder(message = "正在加载账号数据...") {{
      const tbody = document.getElementById("rows");
      const mobileList = document.getElementById("mobile-list");
      if (tbody) {{
        tbody.innerHTML = `<tr><td colspan="${{TABLE_COLUMN_COUNT}}" class="empty">${{escapeHtml(message)}}</td></tr>`;
      }}
      if (mobileList) {{
        mobileList.innerHTML = `<div class="empty">${{escapeHtml(message)}}</div>`;
      }}
      updateTableMeta(0);
      window.requestAnimationFrame(syncStickyHeaderLayout);
    }}

    function applyDashboardOverview(payload) {{
      if (!payload || typeof payload !== "object") {{
        return;
      }}
      if (!areSummaryEqual(payload.summary || {{}}, state.summary || {{}})) {{
        renderSummary(payload.summary || {{}});
      }} else {{
        state.summary = payload.summary || {{}};
        renderStickyQuickFilters(state.summary);
      }}
      updateGeneratedAt(payload.generated_at || "");
      showError("");
      setDashboardBusy(false);
    }}

    function applyDashboardSnapshot(payload) {{
      if (!payload || typeof payload !== "object") {{
        return;
      }}
      state.items = Array.isArray(payload.items) ? payload.items : [];
      state.dashboardRevision = Number(payload.accounts_revision || 0);
      applyDashboardOverview(payload);
      renderRows(state.items);
      renderSortButtons();
    }}

    function applyDashboardPatch(payload) {{
      if (!payload || typeof payload !== "object") {{
        return;
      }}

      const nextSummary = payload.summary || state.summary || {{}};
      state.dashboardRevision = Number(payload.accounts_revision || state.dashboardRevision || 0);
      if (!areSummaryEqual(nextSummary, state.summary || {{}})) {{
        renderSummary(nextSummary);
      }}

      const upsertedItems = Array.isArray(payload.upserted_items) ? payload.upserted_items : [];
      const removedDimensionKeys = Array.isArray(payload.removed_dimension_keys) ? payload.removed_dimension_keys : [];
      if (upsertedItems.length > 0 || removedDimensionKeys.length > 0) {{
        const itemsMap = new Map(state.items.map((item) => [String(item.dimension_key || ""), item]));
        upsertedItems.forEach((item) => {{
          const dimensionKey = String(item.dimension_key || "");
          if (dimensionKey) {{
            itemsMap.set(dimensionKey, item);
          }}
        }});
        removedDimensionKeys.forEach((dimensionKey) => {{
          itemsMap.delete(String(dimensionKey || ""));
        }});
        state.items = Array.from(itemsMap.values());
        renderRows(state.items);
        renderSortButtons();
      }} else {{
        state.summary = nextSummary;
        renderStickyQuickFilters(state.summary);
      }}

      updateGeneratedAt(payload.generated_at || "");
      showError("");
      setDashboardBusy(false);
    }}

    function applyProgressPayload(payload) {{
      if (!payload || typeof payload !== "object") {{
        return;
      }}
      renderProgress(payload);
      state.lastProgressPhase = String(payload.phase || "idle");
      state.lastProgressPayload = payload;
    }}

    function connectEventStream(showBusy = false) {{
      if (typeof window.EventSource !== "function") {{
        showError("当前浏览器不支持实时更新，请更换浏览器。");
        return;
      }}

      closeEventStream();
      if (showBusy || state.items.length === 0) {{
        setDashboardBusy(true);
      }}

      const source = new EventSource(getEventStreamUrl());
      state.eventSource = source;

      source.addEventListener("open", () => {{
        if (state.eventSource !== source) {{
          return;
        }}
        state.eventStreamConnected = true;
        state.reconnectMessageShown = false;
        showError("");
      }});

      source.addEventListener("progress", (event) => {{
        if (state.eventSource !== source) {{
          return;
        }}
        const payload = parseEventPayload(event);
        if (!payload) {{
          return;
        }}
        applyProgressPayload(payload);
      }});

      source.addEventListener("dashboard", (event) => {{
        if (state.eventSource !== source) {{
          return;
        }}
        const payload = parseEventPayload(event);
        if (!payload) {{
          return;
        }}
        applyDashboardSnapshot(payload);
      }});

      source.addEventListener("dashboard_patch", (event) => {{
        if (state.eventSource !== source) {{
          return;
        }}
        const payload = parseEventPayload(event);
        if (!payload) {{
          return;
        }}
        applyDashboardPatch(payload);
      }});

      source.onerror = () => {{
        if (state.eventSource !== source) {{
          return;
        }}
        state.eventStreamConnected = false;
        setDashboardBusy(false);
        if (!state.reconnectMessageShown) {{
          showError("实时连接中断，正在自动重连...");
          state.reconnectMessageShown = true;
        }}
      }};
    }}

    function updateControlButtons(payload, preserveNote = false) {{
      const startButton = document.getElementById("scan-trigger-button");
      const stopButton = document.getElementById("scan-stop-button");
      state.lastProgressPayload = payload;

      startButton.disabled = Boolean(state.controlRequestInFlight) || !payload.can_manual_start;
      stopButton.disabled = Boolean(state.controlRequestInFlight) || !payload.can_manual_stop;
      startButton.setAttribute("aria-busy", state.controlRequestInFlight === "start" ? "true" : "false");
      stopButton.setAttribute("aria-busy", state.controlRequestInFlight === "stop" ? "true" : "false");

      if (state.controlRequestInFlight === "start") {{
        startButton.textContent = "开始提交中...";
        stopButton.textContent = "停止本轮";
        return;
      }}
      if (state.controlRequestInFlight === "stop") {{
        startButton.textContent = "手动开始扫描";
        stopButton.textContent = "停止提交中...";
        return;
      }}

      if (payload.manual_trigger_pending) {{
        startButton.textContent = "已请求，等待开始";
      }} else {{
        startButton.textContent = "手动开始扫描";
      }}

      if (payload.manual_stop_pending) {{
        stopButton.textContent = "停止已提交";
      }} else {{
        stopButton.textContent = "停止本轮";
      }}

      if (!preserveNote) {{
        if (payload.manual_stop_pending) {{
          const requestedAt = formatUtcToShanghai(payload.manual_stop_requested_at_utc || "", "");
          setActionNote(
            requestedAt
              ? `停止请求已提交：${{requestedAt}}；当前账号处理完成后将停止本轮。`
              : "停止请求已提交；当前账号处理完成后将停止本轮。"
          );
          return;
        }}
        if (payload.manual_trigger_pending) {{
          const requestedAt = formatUtcToShanghai(payload.manual_trigger_requested_at_utc || "", "");
          setActionNote(
            requestedAt
              ? `开始请求已提交：${{requestedAt}}；系统将尽快开始下一轮。`
              : "开始请求已提交；系统将尽快开始下一轮。"
          );
          return;
        }}
        if (payload.can_manual_stop) {{
          setActionNote("当前正在扫描，可点击“停止本轮”安全结束当前轮次。");
          return;
        }}
        if (payload.can_manual_start) {{
          setActionNote("空闲或 sleeping 时可手动开始；运行中可点击“停止本轮”安全结束当前轮次。");
          return;
        }}
        if (String(payload.phase || "") === "error") {{
          setActionNote("当前处于 error，可手动开始下一轮重新尝试。");
          return;
        }}
        setActionNote("当前无需手动操作。");
      }}
    }}

    function getConfirmConfig(action) {{
      if (action === "stop") {{
        return {{
          title: "确认停止本轮",
          message: "停止请求提交后，当前正在处理的账号会先完成，本轮剩余账号不再继续。\\n这不会暂停服务，也不会影响后续自动扫描。",
          confirmText: "确认停止",
          danger: true
        }};
      }}
      return {{
        title: "确认开始扫描",
        message: "提交后会立即请求开始下一轮扫描。\\n如果当前在 sleeping，会提前结束等待并尽快开始。",
        confirmText: "确认开始",
        danger: false
      }};
    }}

    async function confirmAndSubmitAction(action) {{
      const confirmed = await openConfirmModal(getConfirmConfig(action));
      if (!confirmed) {{
        updateControlButtons(state.lastProgressPayload || {{ phase: state.lastProgressPhase || "idle" }});
        return;
      }}
      await submitControlAction(action);
    }}

    async function submitControlAction(action) {{
      if (state.controlRequestInFlight) {{
        return;
      }}
      state.controlRequestInFlight = action;
      updateControlButtons(state.lastProgressPayload || {{ phase: state.lastProgressPhase || "idle" }});
      let preserveNote = false;

      try {{
        const path = action === "stop" ? "/api/scan/stop" : "/api/scan";
        const response = await fetch(withPrefix(path), {{
          method: "POST",
          cache: "no-store"
        }});
        const payload = await response.json().catch(() => ({{}}));

        if (response.status === 202 || response.status === 409) {{
          preserveNote = true;
          setActionNote(payload.message || "操作已处理。", response.status !== 202);
          state.lastProgressPayload = {{
            ...(state.lastProgressPayload || {{ phase: state.lastProgressPhase || "idle" }}),
            ...payload
          }};
          updateControlButtons(state.lastProgressPayload, true);
          return;
        }}
        throw new Error(payload.message || `操作失败: ${{response.status}}`);
      }} catch (error) {{
        preserveNote = true;
        setActionNote(error.message || "操作失败", true);
      }} finally {{
        state.controlRequestInFlight = "";
        updateControlButtons(state.lastProgressPayload || {{ phase: state.lastProgressPhase || "idle" }}, preserveNote);
      }}
    }}

    // 页面渲染
    function setActiveFilter(filter) {{
      state.filter = filter;
      syncFilterSelectionState();
      renderRows(state.items);
      renderSortButtons();
    }}

    function prepareDashboardForFilterChange() {{
      state.items = [];
      renderDashboardPlaceholder("正在加载账号数据...");
      setDashboardBusy(true);
    }}

    function renderProgress(payload) {{
      const phase = String(payload.phase || "idle");
      const totalCandidates = Number(payload.total_candidates || 0);
      const processedCandidates = Number(payload.processed_candidates || 0);
      const totalScanned = Number(payload.total_scanned || 0);
      const skippedCandidates = Number(payload.skipped_candidates || 0);
      const progressPercent = Number(payload.progress_percent || 0);
      const accountText = payload.current_account_email || payload.current_account_id || "-";
      const sourceText = payload.current_source_file_name || "-";
      const countText = totalCandidates > 0 ? `${{Math.min(processedCandidates, totalCandidates)}} / ${{totalCandidates}}` : "0 / 0";

      const phaseElement = document.getElementById("progress-phase");
      phaseElement.className = `status-pill status-phase-${{phase}}`;
      phaseElement.textContent = labels.progress[phase] || phase;
      phaseElement.title = progressDescriptions[phase] || phase;

      document.getElementById("progress-count").textContent = countText;
      document.getElementById("progress-subsummary").textContent = progressDescriptions[phase] || "";
      document.getElementById("progress-percent").textContent = formatProgressPercent(progressPercent);
      document.getElementById("progress-bar").style.width = `${{Math.max(0, Math.min(progressPercent, 100))}}%`;
      document.getElementById("progress-total-scanned").textContent = String(totalScanned);
      document.getElementById("progress-total-candidates").textContent = String(totalCandidates);
      document.getElementById("progress-skipped").textContent = String(skippedCandidates);
      const accountElement = document.getElementById("progress-account");
      accountElement.textContent = accountText;
      accountElement.title = payload.current_account_id || payload.current_account_email || "";
      const sourceElement = document.getElementById("progress-source");
      sourceElement.textContent = sourceText;
      sourceElement.title = payload.current_source_file || "";
      document.getElementById("progress-started").textContent = formatUtcToShanghai(payload.round_started_at_utc);
      state.nextRoundAtUtc = String(payload.next_round_at_utc || "");
      state.nextRoundCountdownEnabled = phase === "sleeping";
      refreshNextRoundCountdown();
      document.getElementById("progress-heartbeat").textContent = formatUtcToShanghai(payload.last_heartbeat_at_utc);
      document.getElementById("progress-finished").textContent = formatUtcToShanghai(payload.round_finished_at_utc);
      setProgressNote(payload.last_error_detail || "", phase === "error");
      updateControlButtons(payload);
    }}

    function getSortIndicator(key) {{
      if (state.sortKey !== key) {{
        return "";
      }}
      return state.sortDirection === "asc" ? "↑" : "↓";
    }}

    function renderSortButtons() {{
      document.querySelectorAll(".sort-button").forEach((button) => {{
        const key = button.dataset.sortKey || "";
        const active = key === state.sortKey;
        button.classList.toggle("is-active", active);
        const indicator = button.querySelector(".sort-indicator");
        if (indicator) {{
          indicator.textContent = getSortIndicator(key);
        }}
        button.setAttribute(
          "aria-label",
          active
            ? `当前按${{(button.textContent || "").replace(/[↑↓]/g, "").trim()}}排序，${{state.sortDirection === "asc" ? "升序" : "降序"}}；点击切换`
            : `按${{(button.textContent || "").trim()}}排序`
        );
        const th = button.closest("th");
        if (th) {{
          th.setAttribute("aria-sort", active ? (state.sortDirection === "asc" ? "ascending" : "descending") : "none");
        }}
      }});
    }}

    function getEarliestExhaustedResetAt(items = state.items) {{
      if (!Array.isArray(items) || items.length === 0) {{
        return "";
      }}
      const now = Date.now();
      let earliest = null;
      items.forEach((item) => {{
        if (String((item && item.lifecycle_status) || "") !== "active") {{
          return;
        }}
        if (String((item && item.quota_status) || "") !== "exhausted") {{
          return;
        }}
        const rawResetAt = String((item && item.reset_at_utc) || "").trim();
        const millis = parseUtcMillis(rawResetAt);
        if (millis === null || millis <= now) {{
          return;
        }}
        if (earliest === null || millis < earliest.millis) {{
          earliest = {{ millis, rawResetAt }};
        }}
      }});
      return earliest ? earliest.rawResetAt : "";
    }}

    function refreshExhaustedRecoveryCountdown() {{
      const element = document.getElementById("exhausted-recovery");
      if (!element) {{
        return;
      }}
      const resetAt = getEarliestExhaustedResetAt();
      if (!resetAt) {{
        element.textContent = "最快恢复 -";
        element.title = "暂无可预计恢复时间";
        return;
      }}
      const countdown = formatDurationMinutesFromNow(resetAt) || "-";
      element.textContent = `最快恢复 ${{countdown}}`;
      element.title = formatUtcToShanghai(resetAt, "") || "-";
    }}

    function renderSummaryExtra(key) {{
      if (key !== "exhausted") {{
        return "";
      }}
      return '<span id="exhausted-recovery" class="summary-subtext" data-exhausted-recovery>最快恢复 -</span>';
    }}

    function renderAvailabilityRate(summary) {{
      const rateElement = document.getElementById("availability-rate");
      const valueElement = document.getElementById("availability-rate-value");
      if (!rateElement || !valueElement) {{
        return;
      }}
      const active = Number((summary && summary.active) || 0);
      const available = Number((summary && summary.available) || 0);
      if (!Number.isFinite(active) || active <= 0) {{
        valueElement.textContent = "-";
        rateElement.className = "availability-rate is-medium";
        rateElement.title = "暂无 active 账号";
        return;
      }}
      const rate = Math.max(0, Math.min(100, (available / active) * 100));
      valueElement.textContent = `${{rate.toFixed(1)}}%`;
      rateElement.className = `availability-rate ${{rate >= 80 ? "is-high" : rate >= 50 ? "is-medium" : "is-low"}}`;
      rateElement.title = `available / active = ${{available}} / ${{active}}`;
    }}

    function renderSummary(summary) {{
      state.summary = summary || {{}};
      const totalElement = document.getElementById("toolbar-total");
      if (totalElement) {{
        totalElement.textContent = String(state.summary.total ?? 0);
      }}
      renderAvailabilityRate(state.summary);
      const order = ["total", "active", "available", "exhausted", "unknown", "invalid", "source_missing"];
      const html = order.map((key) => {{
        const filterKey = key === "total" ? "all" : key;
        const active = filterKey === state.filter;
        const hasSubtext = key === "exhausted";
        return `
        <button
          type="button"
          class="summary-card summary-card-${{key}}${{hasSubtext ? " has-subtext" : ""}}${{active ? " is-active" : ""}}"
          data-filter="${{filterKey}}"
          aria-pressed="${{active ? "true" : "false"}}"
          title="${{escapeHtml(summaryDescriptions[key] || labels.summary[key] || key)}}"
        >
          <span class="summary-label">${{labels.summary[key] || key}}</span>
          <strong class="summary-value">${{summary[key] ?? 0}}</strong>
          ${{renderSummaryExtra(key)}}
        </button>
      `;
      }}).join("");
      document.getElementById("summary").innerHTML = html;
      refreshExhaustedRecoveryCountdown();
      renderStickyQuickFilters(state.summary);
    }}

    function syncFilterSelectionState() {{
      const currentFilter = String(state.filter || "all");
      document.getElementById("current-filter").textContent = labels.filter[currentFilter] || currentFilter;

      document.querySelectorAll("#summary [data-filter]").forEach((button) => {{
        const active = String(button.dataset.filter || "all") === currentFilter;
        button.classList.toggle("is-active", active);
        button.setAttribute("aria-pressed", active ? "true" : "false");
      }});

      document.querySelectorAll("#sticky-filter-list [data-sticky-filter]").forEach((button) => {{
        const active = String(button.dataset.stickyFilter || "all") === currentFilter;
        button.classList.toggle("is-active", active);
        button.setAttribute("aria-pressed", active ? "true" : "false");
      }});
    }}

    function compareNullable(left, right, direction) {{
      const leftMissing = left === null || left === undefined || left === "";
      const rightMissing = right === null || right === undefined || right === "";
      if (leftMissing && rightMissing) {{
        return 0;
      }}
      if (leftMissing) {{
        return 1;
      }}
      if (rightMissing) {{
        return -1;
      }}
      if (left < right) {{
        return direction === "asc" ? -1 : 1;
      }}
      if (left > right) {{
        return direction === "asc" ? 1 : -1;
      }}
      return 0;
    }}

    function getSortValue(item, key) {{
      switch (key) {{
        case "email":
        case "note":
        case "source_file_name":
          return String(item[key] || "").toLowerCase();
        case "lifecycle_status":
          return lifecycleOrder[item.lifecycle_status] ?? 999;
        case "quota_status":
          return quotaOrder[item.quota_status] ?? 999;
        case "remaining_percent_value":
          return item.remaining_percent_value;
        case "reset_at_utc":
        case "last_checked_at_utc":
          return item[key] || "";
        default:
          return item[key];
      }}
    }}

    function sortItems(items) {{
      const sorted = [...items];
      sorted.sort((left, right) => {{
        const typeOrder = compareNullable(
          getPlanTypePriority(left.plan_type),
          getPlanTypePriority(right.plan_type),
          "asc"
        );
        if (typeOrder !== 0) {{
          return typeOrder;
        }}

        const typeNameOrder = compareNullable(
          String(left.plan_type || "").toLowerCase(),
          String(right.plan_type || "").toLowerCase(),
          "asc"
        );
        if (typeNameOrder !== 0) {{
          return typeNameOrder;
        }}

        const primary = compareNullable(
          getSortValue(left, state.sortKey),
          getSortValue(right, state.sortKey),
          state.sortDirection
        );
        if (primary !== 0) {{
          return primary;
        }}
        return compareNullable(
          String(left.dimension_key || "").toLowerCase(),
          String(right.dimension_key || "").toLowerCase(),
          "asc"
        );
      }});
      return sorted;
    }}

    function renderRows(items) {{
      const tbody = document.getElementById("rows");
      const visibleItems = filterItemsByActiveView(items, state.filter);
      updateTableMeta(visibleItems.length);
      const sortedItems = sortItems(visibleItems);
      const layoutMode = getCurrentLayoutMode();
      state.lastRenderedMode = layoutMode;

      if (layoutMode === "mobile") {{
        renderMobileRows(sortedItems);
      }}
      if (sortedItems.length === 0) {{
        if (layoutMode !== "mobile") {{
          tbody.innerHTML = '<tr><td colspan="{_TABLE_COLUMN_COUNT}" class="empty">当前筛选下没有账号数据。</td></tr>';
        }}
        window.requestAnimationFrame(syncStickyHeaderLayout);
        return;
      }}

      if (layoutMode === "mobile") {{
        window.requestAnimationFrame(syncStickyHeaderLayout);
        return;
      }}

      const rows = sortedItems.map((item) => `
        <tr>
          <td data-label="邮箱" class="col-email">
            <div class="cell-primary">${{escapeHtml(item.email || "-")}}</div>
            <div class="cell-tag-row">
              ${{renderPlanTag(item.plan_type)}}
            </div>
          </td>
          <td data-label="生命周期" class="col-lifecycle">
            <span class="status-pill status-lifecycle-${{escapeHtml(item.lifecycle_status)}}">
              ${{escapeHtml(labels.lifecycle[item.lifecycle_status] || item.lifecycle_status || "-")}}
            </span>
          </td>
          <td data-label="剩余" class="col-remaining">${{renderRemainingCell(item)}}</td>
          <td data-label="重置时间" class="mono col-reset">
            ${{renderDateTimeCell(item.reset_at_utc || "", true)}}
          </td>
          <td data-label="最近查询" class="mono col-last-checked">
            ${{renderDateTimeCell(item.last_checked_at_utc || "")}}
          </td>
          <td data-label="备注" class="col-note">
            <div class="cell-note">${{escapeHtml(item.note || "-")}}</div>
          </td>
        </tr>
      `).join("");
      tbody.innerHTML = rows;
      refreshResetCountdowns();
      window.requestAnimationFrame(syncStickyHeaderLayout);
    }}

    function showError(message) {{
      const banner = document.getElementById("error-banner");
      banner.style.display = message ? "block" : "none";
      banner.textContent = message || "";
    }}

    // 事件绑定
    document.getElementById("summary").addEventListener("click", (event) => {{
      const button = event.target.closest("[data-filter]");
      if (!button) {{
        return;
      }}
      const nextFilter = button.dataset.filter || "all";
      if (nextFilter === state.filter) {{
        return;
      }}
      setActiveFilter(nextFilter);
    }});

    document.getElementById("sticky-filter-list").addEventListener("click", (event) => {{
      const button = event.target.closest("[data-sticky-filter]");
      if (!button) {{
        return;
      }}
      const nextFilter = button.dataset.stickyFilter || "all";
      if (nextFilter === state.filter) {{
        return;
      }}
      setActiveFilter(nextFilter);
    }});

    document.addEventListener("click", (event) => {{
      const button = event.target.closest(".sort-button[data-sort-key]");
      if (!button) {{
        return;
      }}
      const key = button.dataset.sortKey || "";
      if (!key) {{
        return;
      }}
      if (state.sortKey === key) {{
        state.sortDirection = state.sortDirection === "asc" ? "desc" : "asc";
      }} else {{
        state.sortKey = key;
        state.sortDirection = key === "last_checked_at_utc" ? "desc" : "asc";
      }}
      renderRows(state.items);
      renderSortButtons();
    }});

    document.getElementById("scan-trigger-button").addEventListener("click", () => {{
      void confirmAndSubmitAction("start");
    }});

    document.getElementById("scan-stop-button").addEventListener("click", () => {{
      void confirmAndSubmitAction("stop");
    }});

    document.getElementById("confirm-modal-cancel").addEventListener("click", () => {{
      closeConfirmModal(false);
    }});

    document.getElementById("confirm-modal-confirm").addEventListener("click", () => {{
      closeConfirmModal(true);
    }});

    document.getElementById("confirm-modal").addEventListener("click", (event) => {{
      if (event.target.id === "confirm-modal") {{
        closeConfirmModal(false);
      }}
    }});

    document.addEventListener("keydown", (event) => {{
      if (event.key === "Escape") {{
        closeConfirmModal(false);
      }}
    }});

    document.getElementById("table-wrap").addEventListener("scroll", () => {{
      syncStickyHeaderScroll();
    }});

    window.addEventListener(
      "scroll",
      () => {{
        updateStickyHeaderVisibility();
      }},
      {{ passive: true }}
    );

    window.addEventListener(
      "resize",
      () => {{
        if (state.lastRenderedMode && state.lastRenderedMode !== getCurrentLayoutMode()) {{
          renderRows(state.items);
          renderSortButtons();
        }}
        syncStickyHeaderLayout();
      }},
      {{ passive: true }}
    );

    window.addEventListener("beforeunload", () => {{
      closeEventStream();
    }});

    // 初始化
    applyDashboardSnapshot(initialDashboardPayload);
    syncFilterSelectionState();
    applyProgressPayload(initialProgressPayload);
    updateControlButtons(initialProgressPayload);
    refreshLiveCountdowns();
    window.setInterval(refreshLiveCountdowns, 1000);
    window.requestAnimationFrame(syncStickyHeaderLayout);
    connectEventStream(false);
  """.strip()


def render_index_page(
    settings: Settings,
    *,
    initial_dashboard_payload: dict[str, Any],
    initial_progress_payload: dict[str, Any],
) -> str:
    lifecycle_order_js = json.dumps(_LIFECYCLE_ORDER, ensure_ascii=False)
    quota_order_js = json.dumps(_QUOTA_ORDER, ensure_ascii=False)
    url_prefix_js = json.dumps(settings.url_prefix, ensure_ascii=False)
    initial_dashboard_js = _encode_json_for_script(initial_dashboard_payload)
    initial_progress_js = _encode_json_for_script(initial_progress_payload)
    table_header_row = _render_table_header_row()

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>usage-monitor</title>
  <style>
{_render_index_styles()}
  </style>
</head>
<body>
  <main class="page">
{_render_index_header()}
{_render_index_table(table_header_row)}
  </main>

{_render_confirm_modal()}

  <script>
{_render_index_script(
    lifecycle_order_js=lifecycle_order_js,
    quota_order_js=quota_order_js,
    url_prefix_js=url_prefix_js,
    initial_dashboard_js=initial_dashboard_js,
    initial_progress_js=initial_progress_js,
)}
  </script>
</body>
</html>
"""


def _encode_json_for_script(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")


def _json_response(
    start_response: Any,
    status: str,
    payload: dict[str, Any],
    *,
    environ: dict[str, Any] | None = None,
    settings: Settings | None = None,
) -> list[bytes]:
    body = _encode_json_body(payload)
    return _json_bytes_response(
        start_response,
        status,
        body,
        environ=environ,
        settings=settings,
    )


def _encode_json_body(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _accepts_gzip(environ: dict[str, Any] | None) -> bool:
    if not environ:
        return False
    return "gzip" in str(environ.get("HTTP_ACCEPT_ENCODING", "")).lower()


def _maybe_compress_body(
    *,
    environ: dict[str, Any] | None,
    settings: Settings | None,
    content_type: str,
    body: bytes,
) -> tuple[bytes, list[tuple[str, str]]]:
    gzip_min_bytes = settings.web_gzip_min_bytes if settings is not None else 1024
    if (
        gzip_min_bytes <= 0
        or len(body) < gzip_min_bytes
        or not _accepts_gzip(environ)
        or not (
            content_type.startswith("application/json")
            or content_type.startswith("text/html")
        )
    ):
        return body, []

    compressed = gzip.compress(body, compresslevel=1)
    return compressed, [
        ("Content-Encoding", "gzip"),
        ("Vary", "Accept-Encoding"),
    ]


def _json_bytes_response(
    start_response: Any,
    status: str,
    body: bytes,
    *,
    environ: dict[str, Any] | None = None,
    settings: Settings | None = None,
) -> list[bytes]:
    response_body, extra_headers = _maybe_compress_body(
        environ=environ,
        settings=settings,
        content_type="application/json; charset=utf-8",
        body=body,
    )
    start_response(
        status,
        [
            ("Content-Type", "application/json; charset=utf-8"),
            ("Cache-Control", "no-store"),
            *extra_headers,
            ("Content-Length", str(len(response_body))),
        ],
    )
    return [response_body]


def _text_response(start_response: Any, status: str, text: str) -> list[bytes]:
    body = text.encode("utf-8")
    start_response(
        status,
        [
            ("Content-Type", "text/plain; charset=utf-8"),
            ("Content-Length", str(len(body))),
        ],
    )
    return [body]


def _html_response(
    start_response: Any,
    html: str,
    *,
    environ: dict[str, Any] | None = None,
    settings: Settings | None = None,
) -> list[bytes]:
    return _html_bytes_response(
        start_response,
        html.encode("utf-8"),
        environ=environ,
        settings=settings,
    )


def _html_bytes_response(
    start_response: Any,
    body: bytes,
    *,
    environ: dict[str, Any] | None = None,
    settings: Settings | None = None,
) -> list[bytes]:
    response_body, extra_headers = _maybe_compress_body(
        environ=environ,
        settings=settings,
        content_type="text/html; charset=utf-8",
        body=body,
    )
    start_response(
        "200 OK",
        [
            ("Content-Type", "text/html; charset=utf-8"),
            ("Cache-Control", "no-store"),
            *extra_headers,
            ("Content-Length", str(len(response_body))),
        ],
    )
    return [response_body]


def _encode_sse_event(event_name: str, payload: dict[str, Any]) -> bytes:
    lines = [f"event: {event_name}", f"data: {json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}", ""]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _encode_sse_comment(text: str) -> bytes:
    return f": {text}\n\n".encode("utf-8")


def _event_stream_response(start_response: Any, body_iter: Any) -> Any:
    start_response(
        "200 OK",
        [
            ("Content-Type", "text/event-stream; charset=utf-8"),
            ("Cache-Control", "no-store"),
            ("X-Accel-Buffering", "no"),
        ],
    )
    return body_iter


def create_app(settings: Settings):
    database = UsageDatabase(settings.db_path)
    database.initialize()
    json_cache = _JsonResponseCache()
    page_cache = _JsonResponseCache(max_entries=8)

    def _normalize_filter_from_query(query_string: str) -> str:
        query = parse_qs(query_string, keep_blank_values=False)
        filter_name = query.get("filter", ["all"])[0]
        return filter_name if filter_name in FILTERS else "all"

    def _parse_event_stream_options(query_string: str) -> tuple[str, bool, int]:
        query = parse_qs(query_string, keep_blank_values=False)
        filter_name = query.get("filter", ["all"])[0]
        current_filter = filter_name if filter_name in FILTERS else "all"
        skip_initial_dashboard = str(query.get("skip_initial_dashboard", ["0"])[0]).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        try:
            known_accounts_revision = max(int(query.get("known_accounts_revision", ["0"])[0]), 0)
        except ValueError:
            known_accounts_revision = 0
        return current_filter, skip_initial_dashboard, known_accounts_revision

    def _build_dashboard_json(current_filter: str) -> bytes:
        revisions = database.fetch_change_state()
        cache_key = ("dashboard", current_filter, int(revisions["accounts_revision"]))
        return json_cache.get_or_build(
            cache_key,
            lambda: _encode_json_body(build_dashboard_payload(settings, current_filter, database)),
        )

    def _build_progress_json() -> bytes:
        revisions = database.fetch_change_state()
        cache_key = ("progress", int(revisions["runtime_revision"]))
        return json_cache.get_or_build(
            cache_key,
            lambda: _encode_json_body(build_progress_payload(settings, database)),
        )

    def _build_index_body(gzip_enabled: bool) -> bytes:
        revisions = database.fetch_change_state()
        cache_key = (
            "index",
            int(revisions["accounts_revision"]),
            int(revisions["runtime_revision"]),
            1 if gzip_enabled else 0,
        )

        def _builder() -> bytes:
            html = render_index_page(
                settings,
                initial_dashboard_payload=build_dashboard_payload(settings, "all", database),
                initial_progress_payload=build_progress_payload(settings, database),
            ).encode("utf-8")
            if gzip_enabled and len(html) >= settings.web_gzip_min_bytes:
                return gzip.compress(html, compresslevel=1)
            return html

        return page_cache.get_or_build(cache_key, _builder)

    def _iter_event_stream(
        current_filter: str,
        *,
        skip_initial_dashboard: bool = False,
        known_accounts_revision: int = 0,
    ):
        current_dashboard_payload = build_dashboard_payload(settings, current_filter, database)
        yield f"retry: {EVENT_STREAM_RETRY_MS}\n\n".encode("utf-8")
        yield _encode_sse_event("progress", build_progress_payload(settings, database))
        current_accounts_revision = int(current_dashboard_payload.get("accounts_revision") or 0)
        if not (
            skip_initial_dashboard
            and known_accounts_revision > 0
            and known_accounts_revision == current_accounts_revision
        ):
            yield _encode_sse_event("dashboard", current_dashboard_payload)

        initial_revisions = database.fetch_change_state()
        last_accounts_revision = current_accounts_revision or int(initial_revisions["accounts_revision"])
        last_runtime_revision = int(initial_revisions["runtime_revision"])
        last_dashboard_payload = current_dashboard_payload
        last_keepalive_at = monotonic()

        try:
            while True:
                sleep(settings.sse_poll_seconds)
                revisions = database.fetch_change_state()
                current_accounts_revision = int(revisions["accounts_revision"])
                current_runtime_revision = int(revisions["runtime_revision"])
                emitted = False

                if current_runtime_revision != last_runtime_revision:
                    last_runtime_revision = current_runtime_revision
                    yield _encode_sse_event("progress", build_progress_payload(settings, database))
                    emitted = True

                if current_accounts_revision != last_accounts_revision:
                    last_accounts_revision = current_accounts_revision
                    next_dashboard_payload = build_dashboard_payload(settings, current_filter, database)
                    patch_payload = build_dashboard_patch_payload(last_dashboard_payload, next_dashboard_payload)
                    if (
                        patch_payload["upserted_items"]
                        or patch_payload["removed_dimension_keys"]
                        or patch_payload["summary"] != last_dashboard_payload.get("summary")
                    ):
                        yield _encode_sse_event("dashboard_patch", patch_payload)
                        emitted = True
                    last_dashboard_payload = next_dashboard_payload

                now = monotonic()
                if emitted:
                    last_keepalive_at = now
                    continue

                if now - last_keepalive_at >= max(settings.sse_ping_seconds, EVENT_STREAM_KEEPALIVE_SECONDS):
                    yield _encode_sse_comment("ping")
                    last_keepalive_at = now
        except GeneratorExit:
            logger.debug("SSE 客户端已断开：filter=%s", current_filter)
            return
        except Exception:  # noqa: BLE001
            logger.exception("SSE 推送异常：filter=%s", current_filter)
            return

    def app(environ: dict[str, Any], start_response: Any) -> Any:
        method = environ.get("REQUEST_METHOD", "GET").upper()
        path = environ.get("PATH_INFO", "/")

        if path == "/api/scan":
            if method != "POST":
                return _text_response(start_response, "405 Method Not Allowed", "method not allowed")
            status, payload = build_manual_scan_payload(settings, database)
            json_cache.clear()
            return _json_response(start_response, status, payload, environ=environ, settings=settings)

        if path == "/api/scan/stop":
            if method != "POST":
                return _text_response(start_response, "405 Method Not Allowed", "method not allowed")
            status, payload = build_manual_stop_payload(settings, database)
            json_cache.clear()
            return _json_response(start_response, status, payload, environ=environ, settings=settings)

        if method != "GET":
            return _text_response(start_response, "405 Method Not Allowed", "method not allowed")

        if path == "/":
            gzip_enabled = _accepts_gzip(environ) and settings.web_gzip_min_bytes > 0
            body = _build_index_body(gzip_enabled)
            headers = [
                ("Content-Type", "text/html; charset=utf-8"),
                ("Cache-Control", "no-store"),
            ]
            if gzip_enabled and len(body) >= settings.web_gzip_min_bytes:
                headers.extend(
                    [
                        ("Content-Encoding", "gzip"),
                        ("Vary", "Accept-Encoding"),
                    ]
                )
            headers.append(("Content-Length", str(len(body))))
            start_response(
                "200 OK",
                headers,
            )
            return [body]

        if path == "/healthz":
            try:
                database.ping()
            except Exception as exc:  # noqa: BLE001
                logger.exception("健康检查失败")
                return _text_response(start_response, "500 Internal Server Error", str(exc))
            return _text_response(start_response, "200 OK", "ok")

        if path == "/api/dashboard":
            current_filter = _normalize_filter_from_query(environ.get("QUERY_STRING", ""))
            body = _build_dashboard_json(current_filter)
            return _json_bytes_response(
                start_response,
                "200 OK",
                body,
                environ=environ,
                settings=settings,
            )

        if path == "/api/progress":
            body = _build_progress_json()
            return _json_bytes_response(
                start_response,
                "200 OK",
                body,
                environ=environ,
                settings=settings,
            )

        if path == "/api/events":
            current_filter, skip_initial_dashboard, known_accounts_revision = _parse_event_stream_options(
                environ.get("QUERY_STRING", "")
            )
            return _event_stream_response(
                start_response,
                _iter_event_stream(
                    current_filter,
                    skip_initial_dashboard=skip_initial_dashboard,
                    known_accounts_revision=known_accounts_revision,
                ),
            )

        return _text_response(start_response, "404 Not Found", "not found")

    return app


def main() -> int:
    settings = load_settings()
    configure_logging(settings.log_level)
    UsageDatabase(settings.db_path).initialize()
    app = create_app(settings)
    logger.info("Web 监听地址: http://%s:%s", settings.web_host, settings.web_port)
    with make_server(
        settings.web_host,
        settings.web_port,
        app,
        server_class=ThreadingWSGIServer,
        handler_class=WSGIRequestHandler,
    ) as server:
        server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
