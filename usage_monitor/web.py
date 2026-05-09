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
from .timeutil import format_shanghai, utc_now_iso


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
        "manual_trigger_requested_at_shanghai": format_shanghai(manual_trigger_requested_at_utc),
        "manual_stop_pending": bool(manual_stop_requested_at_utc),
        "manual_stop_requested_at_utc": manual_stop_requested_at_utc,
        "manual_stop_requested_at_shanghai": format_shanghai(manual_stop_requested_at_utc),
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
        "generated_at_shanghai": format_shanghai(generated_at),
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
        "round_started_at_shanghai": format_shanghai(round_started_at_utc),
        "round_finished_at_utc": str(row.get("round_finished_at_utc") or ""),
        "round_finished_at_shanghai": format_shanghai(row.get("round_finished_at_utc")),
        "next_round_at_utc": str(row.get("next_round_at_utc") or ""),
        "next_round_at_shanghai": format_shanghai(row.get("next_round_at_utc")),
        "last_heartbeat_at_utc": str(row.get("last_heartbeat_at_utc") or ""),
        "last_heartbeat_at_shanghai": format_shanghai(row.get("last_heartbeat_at_utc")),
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
    summary, rows = database.fetch_dashboard(current_filter)

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
                "reset_at_shanghai": format_shanghai(reset_at_utc),
                "last_checked_at_utc": last_checked_at_utc,
                "last_checked_at_shanghai": format_shanghai(last_checked_at_utc),
                "note": note or "-",
                "source_file": source_file,
                "source_file_name": _source_file_name(source_file),
                "plan_type": str(row["plan_type"] or ""),
            }
        )

    generated_at = utc_now_iso()
    return {
        "generated_at": generated_at,
        "generated_at_shanghai": format_shanghai(generated_at),
        "filter": current_filter,
        "summary": summary,
        "items": items,
    }


_TABLE_COLUMN_SPECS: tuple[tuple[str, str, str], ...] = (
    ("email", "邮箱", "col-email"),
    ("lifecycle_status", "生命周期", "col-lifecycle"),
    ("remaining_percent_value", "剩余", "col-remaining"),
    ("reset_at_utc", "重置时间", "col-reset"),
    ("last_checked_at_utc", "最近查询", "col-last-checked"),
    ("note", "备注", "col-note"),
    ("source_file_name", "来源文件", "col-source"),
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
        button_class = f"sort-button {class_name}".strip()
        rows.append(
            "              "
            f'<th{th_attr_text}><button type="button" class="{button_class}" '
            f'data-sort-key="{sort_key}" aria-label="按{label}排序">{label}<span class="sort-indicator" aria-hidden="true"></span></button></th>'
        )
    rows.append("            </tr>")
    return "\n".join(rows)


def _render_index_styles() -> str:
    return """
    :root {
      color-scheme: light;
      --bg: #eef3f9;
      --bg-accent: #e2e8f0;
      --panel: rgba(255, 255, 255, 0.94);
      --panel-strong: #ffffff;
      --panel-muted: #f8fafc;
      --panel-info: #f5f9ff;
      --line: #d7e0eb;
      --line-strong: #b9c6d8;
      --text: #0f172a;
      --muted: #475569;
      --muted-soft: #64748b;
      --shadow-soft: 0 10px 30px rgba(15, 23, 42, 0.06);
      --shadow-card: 0 12px 28px rgba(15, 23, 42, 0.08);
      --active: #0f766e;
      --invalid: #b42318;
      --missing: #b45309;
      --available: #15803d;
      --exhausted: #c2410c;
      --unknown: #475467;
      --focus: #2563eb;
      --focus-shadow: 0 0 0 4px rgba(37, 99, 235, 0.18);
      --plan-free-bg: #dbeafe;
      --plan-free-text: #1d4ed8;
      --plan-free-border: #93c5fd;
      --plan-team-bg: #dcfce7;
      --plan-team-text: #15803d;
      --plan-team-border: #86efac;
      --plan-plus-bg: #ffedd5;
      --plan-plus-text: #c2410c;
      --plan-plus-border: #fdba74;
      --plan-pro-bg: #ffe4e6;
      --plan-pro-text: #be123c;
      --plan-pro-border: #fda4af;
      --plan-business-bg: #cffafe;
      --plan-business-text: #0f766e;
      --plan-business-border: #67e8f9;
      --plan-enterprise-bg: #fef3c7;
      --plan-enterprise-text: #b45309;
      --plan-enterprise-border: #fcd34d;
      --plan-unknown-bg: #e2e8f0;
      --plan-unknown-text: #475569;
      --plan-unknown-border: #cbd5e1;
      --radius-lg: 20px;
      --radius-md: 16px;
      --radius-sm: 12px;
    }
    * {
      box-sizing: border-box;
    }
    html {
      scrollbar-gutter: stable;
      background:
        radial-gradient(circle at top, rgba(59, 130, 246, 0.08), transparent 32%),
        linear-gradient(180deg, #f8fbff 0%, var(--bg) 100%);
    }
    body {
      margin: 0;
      font: 14px/1.55 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--text);
      background: transparent;
      -webkit-font-smoothing: antialiased;
      text-rendering: optimizeLegibility;
    }
    body.modal-open {
      overflow: hidden;
    }
    button,
    input,
    select,
    textarea {
      font: inherit;
    }
    button:focus-visible,
    .summary-card:focus-visible,
    .sort-button:focus-visible,
    .skip-link:focus-visible,
    .action-button:focus-visible {
      outline: none;
      box-shadow: var(--focus-shadow);
    }
    .skip-link {
      position: absolute;
      left: 16px;
      top: 16px;
      transform: translateY(-160%);
      padding: 10px 12px;
      border-radius: 999px;
      background: var(--text);
      color: #ffffff;
      text-decoration: none;
      z-index: 60;
      transition: transform 160ms ease;
    }
    .skip-link:focus-visible {
      transform: translateY(0);
    }
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
    .page {
      position: relative;
      max-width: 1460px;
      margin: 0 auto;
      padding: 28px 20px 40px;
    }
    .page-header {
      display: grid;
      gap: 18px;
      margin-bottom: 18px;
    }
    .surface {
      background: var(--panel);
      border: 1px solid rgba(215, 224, 235, 0.9);
      border-radius: var(--radius-lg);
      box-shadow: var(--shadow-soft);
      backdrop-filter: blur(10px);
    }
    .surface-accent {
      background:
        linear-gradient(180deg, rgba(245, 249, 255, 0.96) 0%, rgba(255, 255, 255, 0.94) 100%);
      border-color: rgba(191, 219, 254, 0.95);
    }
    .topbar {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 20px;
      padding: 22px 24px;
    }
    .title-group {
      min-width: 0;
    }
    .eyebrow,
    .section-kicker {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: #1d4ed8;
    }
    .eyebrow::before,
    .section-kicker::before {
      content: "";
      width: 8px;
      height: 8px;
      border-radius: 999px;
      background: linear-gradient(135deg, #60a5fa 0%, #2563eb 100%);
      box-shadow: 0 0 0 4px rgba(37, 99, 235, 0.12);
    }
    .title {
      margin: 10px 0 0;
      font-size: 30px;
      line-height: 1.05;
      font-weight: 800;
      letter-spacing: -0.04em;
      color: var(--text);
    }
    .subtitle {
      max-width: 880px;
      margin: 12px 0 0;
      color: var(--muted);
      font-size: 14px;
    }
    .meta-panel {
      display: grid;
      grid-template-columns: repeat(2, minmax(160px, 1fr));
      gap: 12px;
      width: min(100%, 360px);
    }
    .meta-item {
      padding: 14px 16px;
      border-radius: var(--radius-md);
      border: 1px solid rgba(215, 224, 235, 0.9);
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.95) 0%, rgba(248, 250, 252, 0.95) 100%);
    }
    .meta-label {
      display: block;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted-soft);
    }
    .meta-value {
      display: inline-block;
      margin-top: 8px;
      font-size: 15px;
      font-weight: 700;
      color: var(--text);
      word-break: break-word;
    }
    .progress-panel {
      display: grid;
      grid-template-columns: minmax(0, 1.45fr) minmax(320px, 1fr);
      gap: 16px;
      align-items: stretch;
    }
    .progress-main,
    .progress-side {
      padding: 18px;
    }
    .progress-main {
      display: grid;
      gap: 14px;
    }
    .progress-head {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 16px;
    }
    .progress-title-block {
      min-width: 0;
    }
    .progress-title-row {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 10px;
      margin-top: 10px;
    }
    .progress-refresh-hint {
      display: inline-flex;
      align-items: center;
      padding: 4px 10px;
      border-radius: 999px;
      background: #eff6ff;
      color: #1d4ed8;
      font-size: 12px;
      font-weight: 600;
    }
    .progress-head-actions {
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      align-items: center;
      gap: 8px;
      max-width: 420px;
    }
    .card-label {
      color: var(--muted-soft);
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .progress-track {
      height: 12px;
      overflow: hidden;
      background: #e2e8f0;
      border-radius: 999px;
      box-shadow: inset 0 1px 2px rgba(15, 23, 42, 0.06);
    }
    .progress-bar {
      height: 100%;
      width: 0;
      background: linear-gradient(90deg, #2563eb 0%, #0f766e 55%, #22c55e 100%);
      border-radius: inherit;
      transition: width 180ms ease;
    }
    .progress-summary {
      display: flex;
      justify-content: space-between;
      align-items: flex-end;
      gap: 16px;
    }
    .progress-count {
      font-size: 34px;
      line-height: 1;
      font-weight: 800;
      letter-spacing: -0.05em;
    }
    .progress-subsummary {
      margin-top: 8px;
      color: var(--muted);
    }
    .progress-percent {
      font-size: 13px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: #1d4ed8;
      white-space: nowrap;
    }
    .progress-inline-stats {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }
    .inline-stat {
      padding: 12px 14px;
      border-radius: var(--radius-md);
      border: 1px solid rgba(215, 224, 235, 0.95);
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.96) 0%, rgba(248, 250, 252, 0.96) 100%);
    }
    .inline-stat-label {
      display: block;
      color: var(--muted-soft);
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }
    .inline-stat strong {
      display: block;
      margin-top: 6px;
      font-size: 22px;
      line-height: 1;
      font-weight: 800;
      letter-spacing: -0.03em;
      color: var(--text);
    }
    .progress-side {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.96) 0%, rgba(248, 250, 252, 0.96) 100%);
    }
    .progress-side-card {
      min-width: 0;
      padding: 14px;
      border-radius: var(--radius-md);
      border: 1px solid rgba(215, 224, 235, 0.85);
      background: rgba(255, 255, 255, 0.72);
    }
    .progress-item-value {
      margin-top: 6px;
      word-break: break-word;
      color: var(--text);
      font-weight: 600;
    }
    .progress-note,
    .action-note {
      margin-top: 0;
      padding: 12px 14px;
      border-radius: var(--radius-md);
      border: 1px solid transparent;
      font-size: 13px;
      line-height: 1.5;
    }
    .progress-note {
      display: none;
      background: #f8fafc;
      border-color: rgba(215, 224, 235, 0.95);
      color: var(--muted);
    }
    .progress-note.is-visible {
      display: block;
    }
    .progress-note.is-error {
      border-color: rgba(240, 68, 56, 0.28);
      background: #fef3f2;
      color: #b42318;
    }
    .action-note {
      background: #f8fafc;
      border-color: rgba(191, 219, 254, 0.75);
      color: var(--muted);
    }
    .action-note.is-error {
      border-color: rgba(240, 68, 56, 0.28);
      background: #fff1f2;
      color: #b42318;
    }
    .action-button {
      appearance: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 40px;
      border: 1px solid transparent;
      border-radius: 12px;
      padding: 0 14px;
      background: linear-gradient(180deg, #1d4ed8 0%, #1e40af 100%);
      color: #ffffff;
      font-weight: 700;
      cursor: pointer;
      transition: transform 160ms ease, box-shadow 160ms ease, opacity 160ms ease, background-color 160ms ease;
      box-shadow: 0 8px 20px rgba(29, 78, 216, 0.22);
    }
    .action-button:hover {
      transform: translateY(-1px);
      box-shadow: 0 12px 24px rgba(29, 78, 216, 0.24);
    }
    .action-button-secondary {
      background: #ffffff;
      border-color: rgba(185, 198, 216, 0.95);
      color: var(--text);
      box-shadow: none;
    }
    .action-button-danger {
      background: linear-gradient(180deg, #dc2626 0%, #b42318 100%);
      box-shadow: 0 8px 20px rgba(180, 35, 24, 0.18);
    }
    .action-button:disabled {
      opacity: 0.55;
      cursor: not-allowed;
      transform: none;
      box-shadow: none;
    }
    .modal {
      position: fixed;
      inset: 0;
      z-index: 40;
      display: none;
      align-items: center;
      justify-content: center;
      padding: 20px;
      background: rgba(15, 23, 42, 0.38);
      backdrop-filter: blur(6px);
    }
    .modal.is-visible {
      display: flex;
    }
    .modal-card {
      width: min(100%, 440px);
      background: var(--panel-strong);
      border: 1px solid rgba(215, 224, 235, 0.98);
      border-radius: 22px;
      box-shadow: 0 24px 60px rgba(15, 23, 42, 0.18);
      padding: 22px;
    }
    .modal-title {
      margin: 0;
      font-size: 20px;
      line-height: 1.2;
      font-weight: 800;
      letter-spacing: -0.03em;
    }
    .modal-text {
      margin: 12px 0 0;
      color: var(--muted);
      white-space: pre-line;
    }
    .modal-actions {
      display: flex;
      justify-content: flex-end;
      gap: 10px;
      margin-top: 20px;
    }
    .summary-block {
      display: grid;
      gap: 12px;
    }
    .section-heading {
      display: flex;
      justify-content: space-between;
      align-items: flex-end;
      gap: 16px;
    }
    .section-title {
      margin: 8px 0 0;
      font-size: 22px;
      line-height: 1.1;
      font-weight: 800;
      letter-spacing: -0.03em;
    }
    .section-description {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 13px;
    }
    .summary {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      margin-bottom: 0;
    }
    .card {
      min-width: 0;
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.98) 0%, rgba(248, 250, 252, 0.98) 100%);
      border: 1px solid rgba(215, 224, 235, 0.95);
      border-radius: 18px;
      padding: 16px;
      min-height: 116px;
    }
    .summary-card {
      position: relative;
      appearance: none;
      width: 100%;
      text-align: left;
      cursor: pointer;
      color: var(--text);
      overflow: hidden;
      transition: transform 160ms ease, border-color 160ms ease, box-shadow 160ms ease, background-color 160ms ease;
    }
    .summary-card::before {
      content: "";
      position: absolute;
      left: 0;
      right: 0;
      top: 0;
      height: 4px;
      opacity: 0;
      transition: opacity 160ms ease;
      background: linear-gradient(90deg, #94a3b8 0%, #64748b 100%);
    }
    .summary-card:hover {
      transform: translateY(-2px);
      border-color: rgba(185, 198, 216, 0.98);
      box-shadow: var(--shadow-card);
    }
    .summary-card:hover::before,
    .summary-card.is-active::before {
      opacity: 1;
    }
    .summary-card.is-active {
      border-color: rgba(37, 99, 235, 0.38);
      background: linear-gradient(180deg, rgba(239, 246, 255, 0.98) 0%, rgba(255, 255, 255, 0.98) 100%);
      box-shadow: 0 0 0 1px rgba(37, 99, 235, 0.12), var(--shadow-card);
    }
    .summary-card-total::before { background: linear-gradient(90deg, #0f172a 0%, #475569 100%); }
    .summary-card-active::before,
    .summary-card-available::before { background: linear-gradient(90deg, #0f766e 0%, #22c55e 100%); }
    .summary-card-exhausted::before { background: linear-gradient(90deg, #c2410c 0%, #f59e0b 100%); }
    .summary-card-unknown::before { background: linear-gradient(90deg, #64748b 0%, #94a3b8 100%); }
    .summary-card-invalid::before { background: linear-gradient(90deg, #dc2626 0%, #b42318 100%); }
    .summary-card-source_missing::before { background: linear-gradient(90deg, #d97706 0%, #f59e0b 100%); }
    .card-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
    }
    .card-hint {
      color: var(--muted-soft);
      font-size: 11px;
      font-weight: 600;
      white-space: nowrap;
    }
    .card-value {
      margin-top: 12px;
      font-size: 34px;
      line-height: 1;
      font-weight: 800;
      letter-spacing: -0.05em;
      color: var(--text);
    }
    .card-meta {
      margin-top: 8px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.45;
    }
    .panel {
      position: relative;
      isolation: isolate;
      background: rgba(255, 255, 255, 0.92);
      border: 1px solid rgba(215, 224, 235, 0.95);
      border-radius: 22px;
      overflow: hidden;
      box-shadow: var(--shadow-soft);
    }
    .table-panel {
      display: grid;
      gap: 0;
    }
    .panel-head {
      display: flex;
      justify-content: space-between;
      align-items: flex-end;
      gap: 16px;
      padding: 18px 20px 14px;
      border-bottom: 1px solid rgba(215, 224, 235, 0.9);
      background: linear-gradient(180deg, rgba(248, 250, 252, 0.96) 0%, rgba(255, 255, 255, 0.96) 100%);
    }
    .panel-title {
      margin: 8px 0 0;
      font-size: 22px;
      line-height: 1.1;
      font-weight: 800;
      letter-spacing: -0.03em;
    }
    .panel-head-meta {
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      gap: 8px;
    }
    .table-filter-pill,
    .table-row-count {
      display: inline-flex;
      align-items: center;
      min-height: 36px;
      padding: 0 12px;
      border-radius: 999px;
      border: 1px solid rgba(215, 224, 235, 0.95);
      background: #ffffff;
      color: var(--text);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.02em;
    }
    .table-filter-pill {
      color: #1d4ed8;
      background: #eff6ff;
      border-color: rgba(191, 219, 254, 0.95);
    }
    .sticky-table-shell {
      position: fixed;
      top: 0;
      left: 0;
      width: 0;
      z-index: 30;
      pointer-events: none;
      overflow: hidden;
      background: #f8fafc;
      border-left: 1px solid rgba(215, 224, 235, 0.96);
      border-right: 1px solid rgba(215, 224, 235, 0.96);
      border-bottom: 1px solid rgba(215, 224, 235, 0.96);
      box-shadow: 0 8px 18px rgba(15, 23, 42, 0.08);
    }
    .sticky-table-shell.is-visible {
      pointer-events: auto;
    }
    .sticky-table-toolbar {
      display: flex;
      align-items: center;
      gap: 10px;
      min-height: 36px;
      padding: 0 16px;
      background: #f8fafc;
      border-bottom: 1px solid rgba(215, 224, 235, 0.96);
    }
    .sticky-table-toolbar-label {
      flex: none;
      color: var(--muted-soft);
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      white-space: nowrap;
    }
    .sticky-filter-list {
      display: flex;
      flex-wrap: nowrap;
      align-items: center;
      gap: 6px;
      min-width: 0;
      overflow: hidden;
    }
    .sticky-filter-chip {
      appearance: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 24px;
      padding: 0 10px;
      border: 1px solid rgba(215, 224, 235, 0.96);
      border-radius: 999px;
      background: #ffffff;
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      line-height: 1;
      white-space: nowrap;
      cursor: pointer;
      transition: border-color 160ms ease, background-color 160ms ease, color 160ms ease;
    }
    .sticky-filter-chip:hover {
      border-color: rgba(148, 163, 184, 0.96);
      color: var(--text);
    }
    .sticky-filter-chip.is-active {
      border-color: rgba(191, 219, 254, 0.96);
      background: #eff6ff;
      color: #1d4ed8;
    }
    .sticky-table-scroll {
      overflow: hidden;
      background: #f8fafc;
    }
    .sticky-table {
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      transform: translateX(0);
      will-change: transform;
    }
    .sticky-table thead {
      background: transparent;
    }
    .sticky-table th {
      padding: 0;
      background: transparent;
      background-clip: padding-box;
      # border-bottom: 1px solid rgba(215, 224, 235, 0.96);
      border-bottom: none;
      position: static;
      top: auto;
      z-index: auto;
    }
    .sticky-table th:first-child {
      border-top-left-radius: 0;
    }
    .sticky-table th:last-child {
      border-top-right-radius: 0;
    }
    .sticky-table .sort-button {
      min-height: 42px;
      background: transparent;
    }
    .sticky-table .sort-button:hover {
      background: rgba(255, 255, 255, 0.96);
    }
    .table-wrap {
      overflow-x: auto;
      overflow-y: hidden;
      border-radius: 0 0 22px 22px;
    }
    .mobile-list-wrap {
      display: none;
      border-radius: 0 0 22px 22px;
      overflow: hidden;
    }
    .mobile-list {
      display: grid;
    }
    .mobile-list-wrap .empty {
      padding: 18px 14px;
      border: 0;
      border-radius: 0;
      box-shadow: none;
      background: transparent;
    }
    .mobile-account-item {
      display: grid;
      gap: 8px;
      padding: 12px 14px;
      border-bottom: 1px solid rgba(215, 224, 235, 0.92);
      background: rgba(255, 255, 255, 0.92);
    }
    .mobile-account-item:last-child {
      border-bottom: 0;
    }
    .mobile-account-top {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 10px;
      min-width: 0;
    }
    .mobile-account-main {
      min-width: 0;
      display: grid;
      gap: 5px;
    }
    .mobile-account-email {
      font-size: 13px;
      line-height: 1.35;
      font-weight: 800;
      color: var(--text);
      word-break: break-all;
    }
    .mobile-account-tags {
      display: flex;
      flex-wrap: wrap;
      gap: 4px;
    }
    .mobile-account-tags .plan-tag {
      min-height: 18px;
      padding: 0 6px;
      font-size: 10px;
    }
    .mobile-account-foot {
      display: grid;
      gap: 4px;
    }
    .mobile-account-inline {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 6px;
      min-width: 0;
    }
    .mobile-metric {
      display: inline-flex;
      align-items: baseline;
      gap: 4px;
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
      line-height: 1.1;
      font-weight: 700;
      letter-spacing: 0.04em;
    }
    .mobile-metric-value {
      min-width: 0;
      color: var(--text);
      font-size: 11px;
      line-height: 1.25;
      font-weight: 600;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .mobile-account-extra {
      min-width: 0;
      color: var(--muted);
      font-size: 11px;
      line-height: 1.3;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .mobile-account-extra b {
      color: var(--text);
      font-weight: 600;
    }
    table {
      width: 100%;
      min-width: 1120px;
      border-collapse: separate;
      border-spacing: 0;
    }
    thead {
      background: #f8fafc;
    }
    th,
    td {
      padding: 12px 14px;
      border-bottom: 1px solid rgba(215, 224, 235, 0.92);
      text-align: left;
      vertical-align: top;
    }
    th {
      padding: 0;
      background: #f8fafc;
      background-clip: padding-box;
      position: sticky;
      top: 0;
      z-index: 1;
    }
    thead th:first-child {
      border-top-left-radius: 18px;
    }
    thead th:last-child {
      border-top-right-radius: 18px;
    }
    .sort-button {
      appearance: none;
      display: flex;
      align-items: center;
      justify-content: flex-start;
      gap: 6px;
      width: 100%;
      min-height: 46px;
      border: 0;
      background: transparent;
      color: var(--muted-soft);
      padding: 0 14px;
      text-align: left;
      font-size: 12px;
      font-weight: 700;
      line-height: 1.2;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      cursor: pointer;
      transition: color 160ms ease, background-color 160ms ease;
    }
    .sort-button:hover {
      color: var(--text);
      background: rgba(255, 255, 255, 0.72);
    }
    .sort-button.is-active {
      color: var(--text);
    }
    .sticky-table .sort-button.is-active {
      color: var(--text);
    }
    .sort-indicator {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 14px;
      color: #1d4ed8;
      font-weight: 700;
    }
    .col-remaining {
      min-width: 148px;
    }
    .col-email {
      min-width: 260px;
    }
    .col-reset,
    .col-last-checked {
      min-width: 168px;
      white-space: nowrap;
    }
    .col-note {
      width: 280px;
      max-width: 280px;
    }
    .col-source {
      width: 230px;
      max-width: 230px;
    }
    tbody tr {
      transition: background-color 160ms ease;
    }
    tbody tr:hover {
      background: #f8fbff;
    }
    .mono {
      font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, monospace;
      font-size: 12px;
    }
    .cell-primary {
      min-width: 0;
      font-weight: 700;
      color: var(--text);
      word-break: break-word;
    }
    .cell-secondary {
      margin-top: 6px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.45;
      word-break: break-word;
    }
    .cell-time {
      display: grid;
      gap: 4px;
    }
    .cell-time-date,
    .cell-time-clock {
      display: block;
      white-space: nowrap;
    }
    .cell-time-date {
      color: var(--text);
      font-weight: 700;
    }
    .cell-time-clock {
      color: var(--muted);
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 0.02em;
    }
    .cell-tag-row {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-top: 8px;
    }
    .plan-tag {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 22px;
      padding: 0 8px;
      border-radius: 999px;
      border: 1px solid var(--plan-unknown-border);
      background: var(--plan-unknown-bg);
      color: var(--plan-unknown-text);
      font-size: 11px;
      font-weight: 700;
      line-height: 1;
      text-transform: lowercase;
      white-space: nowrap;
    }
    .plan-tag-free {
      border-color: var(--plan-free-border);
      background: var(--plan-free-bg);
      color: var(--plan-free-text);
    }
    .plan-tag-team {
      border-color: var(--plan-team-border);
      background: var(--plan-team-bg);
      color: var(--plan-team-text);
    }
    .plan-tag-plus {
      border-color: var(--plan-plus-border);
      background: var(--plan-plus-bg);
      color: var(--plan-plus-text);
    }
    .plan-tag-pro {
      border-color: var(--plan-pro-border);
      background: var(--plan-pro-bg);
      color: var(--plan-pro-text);
    }
    .plan-tag-business {
      border-color: var(--plan-business-border);
      background: var(--plan-business-bg);
      color: var(--plan-business-text);
    }
    .plan-tag-enterprise {
      border-color: var(--plan-enterprise-border);
      background: var(--plan-enterprise-bg);
      color: var(--plan-enterprise-text);
    }
    .plan-tag-unknown {
      border-color: var(--plan-unknown-border);
      background: var(--plan-unknown-bg);
      color: var(--plan-unknown-text);
    }
    .cell-secondary.is-truncate {
      display: -webkit-box;
      -webkit-box-orient: vertical;
      -webkit-line-clamp: 2;
      overflow: hidden;
    }
    .remaining-cell {
      display: grid;
      gap: 8px;
      min-width: 120px;
    }
    .remaining-value {
      font-size: 15px;
      font-weight: 800;
      color: var(--text);
    }
    .remaining-track {
      display: block;
      width: 100%;
      height: 8px;
      overflow: hidden;
      border-radius: 999px;
      background: #e2e8f0;
      box-shadow: inset 0 1px 2px rgba(15, 23, 42, 0.05);
    }
    .remaining-fill {
      display: block;
      height: 100%;
      border-radius: inherit;
      background: linear-gradient(90deg, #0f766e 0%, #22c55e 100%);
    }
    .remaining-cell.is-medium .remaining-fill {
      background: linear-gradient(90deg, #d97706 0%, #f59e0b 100%);
    }
    .remaining-cell.is-low .remaining-fill {
      background: linear-gradient(90deg, #dc2626 0%, #f97316 100%);
    }
    .remaining-cell.is-empty .remaining-fill {
      width: 0 !important;
      background: #94a3b8;
    }
    .status-pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      min-height: 30px;
      padding: 0 10px;
      border-radius: 999px;
      border: 1px solid currentColor;
      background: rgba(255, 255, 255, 0.74);
      font-size: 12px;
      font-weight: 700;
      line-height: 1;
      white-space: nowrap;
    }
    .status-pill::before {
      content: "";
      width: 8px;
      height: 8px;
      flex: none;
      border-radius: 999px;
      background: currentColor;
      box-shadow: 0 0 0 4px color-mix(in srgb, currentColor 14%, transparent);
    }
    .status-lifecycle-active { color: var(--active); }
    .status-lifecycle-invalid { color: var(--invalid); }
    .status-lifecycle-source_missing { color: var(--missing); }
    .status-phase-idle { color: var(--unknown); }
    .status-phase-scanning { color: #2563eb; }
    .status-phase-querying { color: var(--active); }
    .status-phase-reconciling { color: #7c3aed; }
    .status-phase-sleeping { color: var(--missing); }
    .status-phase-error { color: var(--invalid); }
    .muted {
      color: var(--muted);
    }
    .error-banner {
      display: none;
      padding: 12px 14px;
      border-radius: var(--radius-md);
      border: 1px solid rgba(240, 68, 56, 0.28);
      color: #b42318;
      background: #fef3f2;
      box-shadow: 0 8px 24px rgba(180, 35, 24, 0.08);
    }
    .empty {
      padding: 38px 16px;
      text-align: center;
      color: var(--muted);
      background: linear-gradient(180deg, rgba(248, 250, 252, 0.72) 0%, rgba(255, 255, 255, 0.9) 100%);
    }
    @media (max-width: 1120px) {
      .page {
        padding-inline: 16px;
      }
      .topbar,
      .panel-head {
        padding-inline: 18px;
      }
      .progress-panel {
        grid-template-columns: 1fr;
      }
      .meta-panel {
        width: 100%;
        max-width: none;
      }
    }
    @media (max-width: 900px) {
      .page {
        padding: 18px 14px 28px;
      }
      .topbar,
      .progress-main,
      .progress-side,
      .panel-head {
        padding: 16px;
      }
      .topbar,
      .section-heading,
      .panel-head,
      .progress-head,
      .progress-summary {
        flex-direction: column;
        align-items: flex-start;
      }
      .meta-panel,
      .progress-side,
      .progress-inline-stats {
        grid-template-columns: 1fr;
      }
      .progress-head-actions,
      .panel-head-meta {
        width: 100%;
        justify-content: flex-start;
      }
      .sticky-table-shell {
        display: none !important;
      }
      .empty {
        border: 1px dashed rgba(185, 198, 216, 0.95);
        border-radius: 16px;
      }
    }
    @media (max-width: 768px) {
      html {
        background: #ffffff;
      }
      body {
        overflow-x: hidden;
        background: #ffffff;
      }
      .page {
        padding: 8px 8px 14px;
      }
      .page-header {
        gap: 8px;
        margin-bottom: 8px;
      }
      .surface,
      .panel,
      .modal-card {
        border-radius: 0;
        background: transparent;
        border: 0;
        box-shadow: none;
        backdrop-filter: none;
      }
      .topbar,
      .progress-main,
      .progress-side,
      .panel-head {
        padding: 8px 0;
      }
      .eyebrow,
      .section-kicker,
      .card-label {
        font-size: 9px;
      }
      .eyebrow,
      .progress-title-block .section-kicker,
      .section-description,
      .subtitle,
      .progress-refresh-hint {
        display: none;
      }
      .title {
        margin-top: 0;
        font-size: 18px;
        letter-spacing: -0.03em;
      }
      .meta-panel {
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 4px 10px;
      }
      .meta-item {
        padding: 0;
        border: 0;
        border-radius: 0;
        background: transparent;
      }
      .meta-label,
      .meta-value {
        margin-top: 0;
      }
      .meta-label {
        font-size: 9px;
      }
      .meta-value {
        font-size: 11px;
        font-weight: 600;
      }
      .progress-panel,
      .progress-main,
      .progress-side,
      .summary-block {
        gap: 6px;
      }
      .progress-title-row {
        margin-top: 0;
        gap: 6px;
      }
      .progress-track {
        height: 8px;
      }
      .card-value,
      .progress-count {
        font-size: 18px;
      }
      .progress-percent {
        font-size: 10px;
      }
      .progress-subsummary,
      .progress-note,
      .action-note {
        font-size: 11px;
        line-height: 1.35;
      }
      .progress-note,
      .action-note {
        padding: 0;
        border: 0;
        border-radius: 0;
        background: transparent;
      }
      .progress-inline-stats {
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 8px;
      }
      .inline-stat {
        padding: 0;
        border: 0;
        border-radius: 0;
        background: transparent;
      }
      .inline-stat strong {
        margin-top: 3px;
        font-size: 14px;
      }
      .progress-side {
        grid-template-columns: 1fr;
        gap: 3px;
        background: transparent;
      }
      .progress-side-card {
        display: grid;
        grid-template-columns: 52px minmax(0, 1fr);
        align-items: baseline;
        gap: 8px;
        padding: 0;
        border: 0;
        border-radius: 0;
        background: transparent;
      }
      .progress-item-value {
        margin-top: 0;
        font-size: 11px;
        font-weight: 600;
      }
      .action-button {
        min-height: 28px;
        padding: 0 8px;
        font-size: 11px;
        border-radius: 8px;
        box-shadow: none;
      }
      .status-pill {
        min-height: 18px;
        gap: 4px;
        padding: 0 6px;
        font-size: 10px;
        background: transparent;
      }
      .status-pill::before {
        width: 5px;
        height: 5px;
      }
      .section-title,
      .panel-title {
        margin-top: 0;
        font-size: 14px;
      }
      .summary {
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 4px;
      }
      .card {
        min-height: 0;
        padding: 6px 8px;
        border-radius: 8px;
        border-color: rgba(215, 224, 235, 1);
        background: #ffffff;
        box-shadow: none;
      }
      .card-value {
        margin-top: 2px;
      }
      .card-meta {
        display: none;
      }
      .panel-head {
        gap: 4px;
        border-top: 1px solid rgba(215, 224, 235, 0.92);
        border-bottom: 1px solid rgba(215, 224, 235, 0.92);
        background: transparent;
      }
      .panel-head-meta {
        gap: 4px;
      }
      .table-filter-pill,
      .table-row-count {
        min-height: 22px;
        padding: 0 7px;
        font-size: 10px;
        border-radius: 999px;
      }
      .mobile-list-wrap {
        display: block;
        border-radius: 0;
        overflow: visible;
        background: transparent;
      }
      .table-wrap {
        display: none;
      }
      .mobile-list {
        display: block;
      }
      .mobile-list-wrap .empty {
        padding: 12px 0;
      }
      .mobile-account-item {
        gap: 3px;
        padding: 7px 0;
        background: transparent;
      }
      .mobile-account-top {
        align-items: baseline;
        gap: 8px;
      }
      .mobile-account-main {
        gap: 2px;
      }
      .mobile-account-email {
        font-size: 12px;
        line-height: 1.25;
      }
      .mobile-account-tags .plan-tag {
        min-height: 14px;
        padding: 0 4px;
        font-size: 9px;
      }
      .mobile-metric-value {
        font-size: 11px;
      }
      .mobile-account-foot {
        gap: 0;
      }
      .mobile-metric-label {
        font-size: 9px;
      }
      .mobile-account-extra {
        font-size: 10px;
      }
    }
    @media (max-width: 420px) {
      .progress-head-actions {
        gap: 4px;
      }
      .progress-head-actions .action-button {
        flex: 1 1 calc(50% - 3px);
      }
      .progress-inline-stats,
      .summary,
      .meta-panel {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      .mobile-account-inline {
        gap: 4px;
      }
      .mobile-metric {
        gap: 3px;
      }
      .mobile-metric + .mobile-metric::before {
        margin-right: 1px;
      }
    }
    @media (prefers-reduced-motion: reduce) {
      *,
      *::before,
      *::after {
        animation: none !important;
        transition: none !important;
        scroll-behavior: auto !important;
      }
    }
  """.strip()


def _render_index_header() -> str:
    return f"""
    <div class="page-header">
      <a href="#accounts-panel" class="skip-link">跳到账号列表</a>

      <section class="topbar surface">
        <div class="title-group">
          <div class="eyebrow">Operations Dashboard</div>
          <h1 class="title">usage-monitor</h1>
          <p class="subtitle">定时采集账号主额度状态，页面改为 SSE 实时推送；支持手动开始下一轮和安全停止本轮，两个动作都需要二次确认。</p>
        </div>
        <div class="meta-panel" aria-label="页面状态">
          <div class="meta-item">
            <span class="meta-label">当前筛选</span>
            <span class="meta-value" id="current-filter">active</span>
          </div>
          <div class="meta-item">
            <span class="meta-label">最近刷新</span>
            <span class="meta-value mono" id="generated-at">-</span>
          </div>
        </div>
      </section>

      <div id="error-banner" class="error-banner" role="alert" aria-live="assertive"></div>

      <section class="progress-panel" aria-label="本轮进度">
        <div class="progress-main surface surface-accent">
          <div class="progress-head">
            <div class="progress-title-block">
              <div class="section-kicker">Collector Runtime</div>
              <div class="progress-title-row">
                <div class="card-label">本轮进度</div>
                <div class="progress-refresh-hint">SSE 实时推送</div>
              </div>
            </div>
            <div class="progress-head-actions">
              <span id="progress-phase" class="status-pill status-phase-idle">idle</span>
              <button id="scan-trigger-button" type="button" class="action-button" disabled>手动开始扫描</button>
              <button id="scan-stop-button" type="button" class="action-button action-button-danger" disabled>停止本轮</button>
            </div>
          </div>
          <div class="progress-track" aria-hidden="true">
            <div id="progress-bar" class="progress-bar"></div>
          </div>
          <div class="progress-summary">
            <div>
              <div id="progress-count" class="progress-count">-</div>
              <div id="progress-subsummary" class="progress-subsummary">等待采集开始</div>
            </div>
            <div id="progress-percent" class="progress-percent">0%</div>
          </div>
          <div class="progress-inline-stats" aria-label="进度统计">
            <div class="inline-stat">
              <span class="inline-stat-label">扫描到</span>
              <strong id="progress-total-scanned">-</strong>
            </div>
            <div class="inline-stat">
              <span class="inline-stat-label">待查询</span>
              <strong id="progress-total-candidates">-</strong>
            </div>
            <div class="inline-stat">
              <span class="inline-stat-label">跳过</span>
              <strong id="progress-skipped">-</strong>
            </div>
          </div>
          <div id="progress-note" class="progress-note" role="status" aria-live="polite"></div>
          <div id="scan-action-note" class="action-note" role="status" aria-live="polite">空闲或 sleeping 时可手动开始；运行中可点击“停止本轮”安全结束当前轮次。</div>
        </div>
        <div class="progress-side surface">
          <div class="progress-side-card">
            <div class="card-label">当前/最近账号</div>
            <div id="progress-account" class="progress-item-value mono">-</div>
          </div>
          <div class="progress-side-card">
            <div class="card-label">来源文件</div>
            <div id="progress-source" class="progress-item-value mono">-</div>
          </div>
          <div class="progress-side-card">
            <div class="card-label">本轮开始</div>
            <div id="progress-started" class="progress-item-value mono">-</div>
          </div>
          <div class="progress-side-card">
            <div class="card-label">下轮开始</div>
            <div id="progress-next-round" class="progress-item-value mono">-</div>
          </div>
          <div class="progress-side-card">
            <div class="card-label">最近心跳</div>
            <div id="progress-heartbeat" class="progress-item-value mono">-</div>
          </div>
          <div class="progress-side-card">
            <div class="card-label">本轮结束</div>
            <div id="progress-finished" class="progress-item-value mono">-</div>
          </div>
        </div>
      </section>

      <section class="summary-block" aria-labelledby="summary-title">
        <div class="section-heading">
          <div>
            <div class="section-kicker">Overview</div>
            <h2 id="summary-title" class="section-title">账号总览</h2>
            <p class="section-description">点击统计卡片可快速筛选下方列表，保留当前运维视角。</p>
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
        <div>
          <div class="section-kicker">Accounts</div>
          <h2 id="accounts-panel-title" class="panel-title">账号列表</h2>
        </div>
        <div class="panel-head-meta">
          <span id="table-summary-filter" class="table-filter-pill">filter: active</span>
          <span id="table-row-count" class="table-row-count">0 条</span>
        </div>
      </div>
      <div id="sticky-table-shell" class="sticky-table-shell" aria-hidden="true" hidden>
        <div class="sticky-table-toolbar">
          <span class="sticky-table-toolbar-label">快捷筛选</span>
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
    const STICKY_QUICK_FILTERS = ["all", "active", "available", "exhausted", "invalid"];
    const initialDashboardPayload = {initial_dashboard_js};
    const initialProgressPayload = {initial_progress_js};
    const state = {{
      filter: initialDashboardPayload.filter || "active",
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
      summary: initialDashboardPayload.summary || {{}},
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
      scanning: "正在扫描 tokens 目录，整理账号列表。",
      querying: "正在逐个查询账号额度状态。",
      reconciling: "正在收尾整理本轮运行结果。",
      sleeping: "本轮已结束，等待下一个自动周期。",
      error: "运行出现异常，可手动开始下一轮重试。"
    }};

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
      document.getElementById("table-summary-filter").textContent = `filter: ${{labels.filter[state.filter] || state.filter}}`;
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

    function renderDateTimeCell(value) {{
      const text = String(value || "").trim();
      if (!text || text === "-") {{
        return '<div class="cell-time"><span class="cell-time-date">-</span></div>';
      }}
      const parts = text.split(" ");
      if (parts.length >= 2) {{
        const datePart = parts[0] || "-";
        const timePart = parts.slice(1).join(" ") || "";
        return `
          <div class="cell-time">
            <span class="cell-time-date">${{escapeHtml(datePart)}}</span>
            <span class="cell-time-clock">${{escapeHtml(timePart)}}</span>
          </div>
        `;
      }}
      return `<div class="cell-time"><span class="cell-time-date">${{escapeHtml(text)}}</span></div>`;
    }}

    function formatCompactDateTimeText(value) {{
      const text = String(value || "").trim();
      if (!text || text === "-") {{
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
        const resetText = formatCompactDateTimeText(item.reset_at_shanghai || "-");
        const checkedText = formatCompactDateTimeText(item.last_checked_at_shanghai || "-");
        const sourceValue = String(item.source_file_name || item.source_file || "").trim();
        const extraParts = [];
        if (item.note && String(item.note).trim() && String(item.note).trim() !== "-") {{
          extraParts.push(`<b>备</b> ${{escapeHtml(String(item.note).trim())}}`);
        }}
        if (sourceValue && sourceValue !== "-") {{
          extraParts.push(`<b>源</b> ${{escapeHtml(sourceValue)}}`);
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
                  ${{renderMobileMetric("重", resetText, "mono")}}
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
            class="sticky-filter-chip${{active ? " is-active" : ""}}"
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
      return withPrefix(`/api/events?filter=${{encodeURIComponent(state.filter)}}`);
    }}

    function parseEventPayload(event) {{
      try {{
        return JSON.parse(event.data || "{{}}");
      }} catch (_error) {{
        return null;
      }}
    }}

    function applyDashboardPayload(payload) {{
      if (!payload || typeof payload !== "object") {{
        return;
      }}
      state.items = Array.isArray(payload.items) ? payload.items : [];
      renderSummary(payload.summary || {{}});
      renderRows(state.items);
      renderSortButtons();
      document.getElementById("generated-at").textContent = payload.generated_at_shanghai || payload.generated_at || "-";
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
        showError("当前浏览器不支持 SSE 实时推送。");
        return;
      }}

      closeEventStream();
      if (showBusy) {{
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
        applyDashboardPayload(payload);
      }});

      source.onerror = () => {{
        if (state.eventSource !== source) {{
          return;
        }}
        state.eventStreamConnected = false;
        setDashboardBusy(false);
        if (!state.reconnectMessageShown) {{
          showError("SSE 连接中断，正在自动重连...");
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
          const requestedAt = payload.manual_stop_requested_at_shanghai || payload.manual_stop_requested_at_utc || "";
          setActionNote(
            requestedAt
              ? `停止请求已提交：${{requestedAt}}；当前账号处理完成后将停止本轮。`
              : "停止请求已提交；当前账号处理完成后将停止本轮。"
          );
          return;
        }}
        if (payload.manual_trigger_pending) {{
          const requestedAt = payload.manual_trigger_requested_at_shanghai || payload.manual_trigger_requested_at_utc || "";
          setActionNote(
            requestedAt
              ? `开始请求已提交：${{requestedAt}}；collector 将尽快开始下一轮。`
              : "开始请求已提交；collector 将尽快开始下一轮。"
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
      document.getElementById("current-filter").textContent = labels.filter[filter] || filter;
      updateTableMeta(Array.isArray(state.items) ? state.items.length : 0);
      renderStickyQuickFilters();
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
      document.getElementById("progress-subsummary").textContent =
        `扫描到 ${{totalScanned}} 个账号，待查询 ${{totalCandidates}} 个，跳过 ${{skippedCandidates}} 个`;
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
      document.getElementById("progress-started").textContent = payload.round_started_at_shanghai || "-";
      document.getElementById("progress-next-round").textContent = payload.next_round_at_shanghai || "-";
      document.getElementById("progress-heartbeat").textContent = payload.last_heartbeat_at_shanghai || "-";
      document.getElementById("progress-finished").textContent = payload.round_finished_at_shanghai || "-";
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

    function renderSummary(summary) {{
      state.summary = summary || {{}};
      const order = ["total", "active", "available", "exhausted", "unknown", "invalid", "source_missing"];
      const html = order.map((key) => {{
        const filterKey = key === "total" ? "all" : key;
        const active = filterKey === state.filter;
        return `
        <button
          type="button"
          class="card summary-card summary-card-${{key}}${{active ? " is-active" : ""}}"
          data-filter="${{filterKey}}"
          aria-pressed="${{active ? "true" : "false"}}"
        >
          <div class="card-header">
            <div class="card-label">${{labels.summary[key] || key}}</div>
          </div>
          <div class="card-value">${{summary[key] ?? 0}}</div>
          <div class="card-meta">${{summaryDescriptions[key] || ""}}</div>
        </button>
      `;
      }}).join("");
      document.getElementById("summary").innerHTML = html;
      renderStickyQuickFilters(state.summary);
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
      updateTableMeta(Array.isArray(items) ? items.length : 0);
      const sortedItems = Array.isArray(items) ? sortItems(items) : [];
      const layoutMode = getCurrentLayoutMode();
      state.lastRenderedMode = layoutMode;

      if (layoutMode === "mobile") {{
        renderMobileRows(sortedItems);
      }}
      if (!Array.isArray(items) || items.length === 0) {{
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
            ${{renderDateTimeCell(item.reset_at_shanghai || "-")}}
          </td>
          <td data-label="最近查询" class="mono col-last-checked">
            ${{renderDateTimeCell(item.last_checked_at_shanghai || "-")}}
          </td>
          <td data-label="备注" class="col-note" title="${{escapeHtml(item.note || "-")}}">
            <div class="cell-primary">${{escapeHtml(item.note || "-")}}</div>
          </td>
          <td data-label="来源文件" class="mono col-source" title="${{escapeHtml(item.source_file || "-")}}">
            <div class="cell-primary">${{escapeHtml(item.source_file_name || "-")}}</div>
          </td>
        </tr>
      `).join("");
      tbody.innerHTML = rows;
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
      setActiveFilter(button.dataset.filter || "all");
      connectEventStream(true);
    }});

    document.getElementById("sticky-filter-list").addEventListener("click", (event) => {{
      const button = event.target.closest("[data-sticky-filter]");
      if (!button) {{
        return;
      }}
      setActiveFilter(button.dataset.stickyFilter || "all");
      connectEventStream(true);
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
    setActiveFilter(state.filter);
    applyDashboardPayload(initialDashboardPayload);
    applyProgressPayload(initialProgressPayload);
    updateControlButtons(initialProgressPayload);
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
                initial_dashboard_payload=build_dashboard_payload(settings, "active", database),
                initial_progress_payload=build_progress_payload(settings, database),
            ).encode("utf-8")
            if gzip_enabled and len(html) >= settings.web_gzip_min_bytes:
                return gzip.compress(html, compresslevel=1)
            return html

        return page_cache.get_or_build(cache_key, _builder)

    def _iter_event_stream(current_filter: str):
        initial_revisions = database.fetch_change_state()
        yield f"retry: {EVENT_STREAM_RETRY_MS}\n\n".encode("utf-8")
        yield _encode_sse_event("progress", build_progress_payload(settings, database))
        yield _encode_sse_event("dashboard", build_dashboard_payload(settings, current_filter, database))

        last_accounts_revision = int(initial_revisions["accounts_revision"])
        last_runtime_revision = int(initial_revisions["runtime_revision"])
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
                    yield _encode_sse_event(
                        "dashboard",
                        build_dashboard_payload(settings, current_filter, database),
                    )
                    emitted = True

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
            current_filter = _normalize_filter_from_query(environ.get("QUERY_STRING", ""))
            return _event_stream_response(start_response, _iter_event_stream(current_filter))

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
