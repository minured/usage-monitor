"""采集器主流程与账号状态落库逻辑。"""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from typing import Any

from .config import Settings, load_settings
from .cpa_status import CPAStatusClient, CPAStatusError
from .db import RUNTIME_KEY, UsageDatabase
from .models import (
    COLLECTOR_PHASE_ERROR,
    COLLECTOR_PHASE_IDLE,
    COLLECTOR_PHASE_QUERYING,
    COLLECTOR_PHASE_RECONCILING,
    COLLECTOR_PHASE_SCANNING,
    COLLECTOR_PHASE_SLEEPING,
    LIFECYCLE_ACTIVE,
    LIFECYCLE_INVALID,
    LIFECYCLE_SOURCE_MISSING,
    QUOTA_AVAILABLE,
    QUOTA_EXHAUSTED,
    QUOTA_UNKNOWN,
    TokenRecord,
)
from .openai_api import HTTPStatusError, InvalidResponseError, OpenAIUsageClient, TransportError
from .timeutil import iso_after_seconds, iso_from_unix, parse_utc, utc_now, utc_now_iso
from .tokens import move_token_to_auth_invalid, scan_tokens_dir, write_refreshed_tokens


logger = logging.getLogger("usage_monitor.collector")


def configure_logging(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="定时采集 ChatGPT usage")
    parser.add_argument(
        "--once",
        action="store_true",
        help="只执行一轮采集",
    )
    return parser.parse_args(argv)


def _clean_detail(text: str, limit: int = 500) -> str:
    compact = " ".join(text.strip().split())
    if len(compact) <= limit:
        return compact
    return f"{compact[: limit - 3]}..."


def _record_label(record: TokenRecord) -> str:
    return f"{record.account_id} / {record.plan_type} / {record.chatgpt_user_id}"


def _to_bool_or_none(value: Any) -> int | None:
    if value is True:
        return 1
    if value is False:
        return 0
    return None


def classify_usage_payload(payload: dict[str, Any]) -> dict[str, Any]:
    rate_limit = payload.get("rate_limit")
    if not isinstance(rate_limit, dict):
        return {
            "quota_status": QUOTA_UNKNOWN,
            "rate_limit_allowed": None,
            "rate_limit_reached": None,
            "used_percent": None,
            "reset_at_utc": None,
            "plan_type": str(payload.get("plan_type") or "").strip(),
        }

    primary_window = rate_limit.get("primary_window")
    if not isinstance(primary_window, dict):
        primary_window = {}

    allowed = rate_limit.get("allowed")
    limit_reached = rate_limit.get("limit_reached")
    if allowed is True and limit_reached is False:
        quota_status = QUOTA_AVAILABLE
    elif allowed is False or limit_reached is True:
        quota_status = QUOTA_EXHAUSTED
    else:
        quota_status = QUOTA_UNKNOWN

    used_percent = primary_window.get("used_percent")
    if isinstance(used_percent, bool):
        used_percent = None
    elif isinstance(used_percent, (int, float)):
        used_percent = float(used_percent)
    else:
        used_percent = None

    return {
        "quota_status": quota_status,
        "rate_limit_allowed": _to_bool_or_none(allowed),
        "rate_limit_reached": _to_bool_or_none(limit_reached),
        "used_percent": used_percent,
        "reset_at_utc": iso_from_unix(primary_window.get("reset_at")),
        "plan_type": str(payload.get("plan_type") or "").strip(),
    }


class CollectorService:
    def __init__(
        self,
        settings: Settings,
        database: UsageDatabase | None = None,
        api_client: OpenAIUsageClient | Any | None = None,
        cpa_status_client: CPAStatusClient | Any | None = None,
        sleeper: Any = time.sleep,
    ):
        self.settings = settings
        self.database = database or UsageDatabase(settings.db_path)
        self.api_client = api_client or OpenAIUsageClient(
            timeout=settings.request_timeout_seconds,
            user_agent=settings.user_agent,
        )
        self.cpa_status_client = cpa_status_client or self._build_cpa_status_client()
        self._last_cpa_status_sync_monotonic: float | None = None
        self.sleeper = sleeper
        self.runtime_state: dict[str, Any] | None = None

    def run_forever(self) -> None:
        while True:
            self._ensure_scheduled_sleep()
            self._sync_cpa_status_if_due(force=True)
            logger.info(
                "等待调度时间到达后开始下一轮（sleeping 阶段支持手动触发提前开始）",
            )
            if self._wait_for_next_round():
                logger.info("已消费手动触发请求，立即开始下一轮采集")
            try:
                self.run_once()
                self._mark_sleeping()
            except Exception as exc:  # noqa: BLE001
                logger.exception("本轮采集异常退出")
                self._safe_mark_error(exc)

    def run_once(self) -> None:
        self.database.initialize()
        self._mark_round_started()
        records, warnings = scan_tokens_dir(self.settings.tokens_dir)
        for message in warnings:
            logger.warning(message)

        existing_by_dimension = self.database.fetch_accounts_index()
        scanned_dimension_keys = {record.dimension_key for record in records}
        scanned_source_files = {
            str(record.source_file): record.dimension_key
            for record in records
        }
        candidates = self._select_candidates(records, existing_by_dimension)
        self._mark_query_plan(
            total_scanned=len(records),
            total_candidates=len(candidates),
            skipped_candidates=max(len(records) - len(candidates), 0),
        )

        logger.info(
            "本轮扫描到 %s 个账号维度，待查询 %s 个账号维度",
            len(records),
            len(candidates),
        )

        stopped_early = False
        for index, record in enumerate(candidates):
            self._sync_cpa_status_if_due()
            if self._should_stop_current_round("开始下一个账号前"):
                stopped_early = True
                break

            existing = existing_by_dimension.get(record.dimension_key)
            self._mark_current_record(index=index, record=record)
            self._process_record(record, existing)
            self._mark_processed(index + 1)

            if self._should_stop_current_round("当前账号处理完成后"):
                stopped_early = True
                break

            if index + 1 < len(candidates):
                if self._sleep_between_accounts():
                    stopped_early = True
                    break

        if stopped_early:
            logger.info("检测到手动停止请求，本轮剩余账号不再继续查询")

        self._mark_reconciling()
        self._mark_source_missing(existing_by_dimension, scanned_dimension_keys, scanned_source_files)
        self._mark_idle()
        self._sync_cpa_status_if_due(force=True)

    def _build_cpa_status_client(self) -> CPAStatusClient | None:
        if not self.settings.cpa_status_enabled:
            return None
        if not self.settings.cpa_status_url or not self.settings.cpa_management_key:
            logger.warning("CPA 状态同步已启用，但 URL 或 management key 为空，已跳过")
            return None
        return CPAStatusClient(
            url=self.settings.cpa_status_url,
            management_key=self.settings.cpa_management_key,
            timeout=self.settings.cpa_status_timeout_seconds,
        )

    def _seconds_until_next_cpa_status_sync(self) -> float | None:
        if self.cpa_status_client is None:
            return None
        if self._last_cpa_status_sync_monotonic is None:
            return 0.0
        interval = max(float(self.settings.cpa_status_sync_seconds), 5.0)
        elapsed = time.monotonic() - self._last_cpa_status_sync_monotonic
        return max(interval - elapsed, 0.0)

    def _sync_cpa_status_if_due(self, *, force: bool = False) -> None:
        if self.cpa_status_client is None:
            return
        remaining = self._seconds_until_next_cpa_status_sync()
        if not force and remaining is not None and remaining > 0:
            return
        self._last_cpa_status_sync_monotonic = time.monotonic()
        try:
            self.database.initialize()
            statuses = self.cpa_status_client.fetch_auth_statuses()
            result = self.database.sync_cpa_statuses(
                statuses,
                cpa_stale_seconds=self.settings.cpa_status_stale_seconds,
            )
        except CPAStatusError as exc:
            logger.warning("CPA 状态同步失败: %s", exc)
            return
        except Exception:  # noqa: BLE001
            logger.exception("CPA 状态同步异常")
            return
        logger.info(
            "CPA 状态同步完成：statuses=%s matched=%s changed=%s cleared=%s",
            result["statuses"],
            result["matched"],
            result["changed"],
            result["cleared"],
        )

    def _select_candidates(
        self,
        records: list[TokenRecord],
        existing_by_dimension: dict[str, dict[str, Any]],
    ) -> list[TokenRecord]:
        candidates: list[TokenRecord] = []
        for record in records:
            existing = existing_by_dimension.get(record.dimension_key)
            if self._skip_unchanged_invalid(existing, record):
                logger.info(
                    "跳过未更新的 invalid 账号 %s (%s)",
                    _record_label(record),
                    record.source_file_name,
                )
                continue
            candidates.append(record)
        return self._sort_candidates(candidates, existing_by_dimension)

    @staticmethod
    def _sort_candidates(
        candidates: list[TokenRecord],
        existing_by_dimension: dict[str, dict[str, Any]],
    ) -> list[TokenRecord]:
        prioritized: list[TokenRecord] = []
        remaining: list[TokenRecord] = []

        for record in candidates:
            existing = existing_by_dimension.get(record.dimension_key)
            if existing is None or record.source_mtime_ns > int(existing.get("source_mtime_ns") or 0):
                prioritized.append(record)
            else:
                remaining.append(record)

        prioritized.sort(key=lambda item: (-item.source_mtime_ns, item.source_file_name))
        remaining.sort(key=lambda item: item.source_file_name)
        return prioritized + remaining

    @staticmethod
    def _skip_unchanged_invalid(existing: dict[str, Any] | None, record: TokenRecord) -> bool:
        if not existing:
            return False
        if existing.get("lifecycle_status") != LIFECYCLE_INVALID:
            return False
        same_source = (
            str(existing.get("source_file") or "") == str(record.source_file)
            and int(existing.get("source_mtime_ns") or 0) == record.source_mtime_ns
        )
        if not same_source:
            return False
        return int(existing.get("last_http_status") or 0) != 401

    def _process_record(self, record: TokenRecord, existing: dict[str, Any] | None) -> None:
        try:
            payload = self.api_client.fetch_usage(record)
            self._save_success(record, existing, payload)
            return
        except HTTPStatusError as exc:
            if exc.status == 401:
                self._handle_401(record, existing, exc)
                return
            if exc.status == 403:
                self._handle_403(record, existing, exc)
                return
            if exc.status == 429 or 500 <= exc.status <= 599:
                self._save_transient(record, existing, exc.status, str(exc))
                return
            self._save_invalid(
                record,
                existing,
                http_status=exc.status,
                reason_code=f"http_{exc.status}",
                reason_detail=str(exc),
            )
            return
        except InvalidResponseError as exc:
            self._save_transient(record, existing, getattr(exc, "status", 200), str(exc))
            return
        except TransportError as exc:
            self._save_transient(record, existing, None, str(exc))
            return

    def _handle_401(
        self,
        record: TokenRecord,
        existing: dict[str, Any] | None,
        original_error: HTTPStatusError,
    ) -> None:
        try:
            refreshed = self.api_client.refresh_tokens(record)
            record = write_refreshed_tokens(record, refreshed)
        except Exception as exc:  # noqa: BLE001
            detail = _clean_detail(f"{original_error}; refresh 失败: {exc}")
            self._save_invalid_401(
                record,
                existing,
                reason_code="refresh_failed",
                reason_detail=detail,
            )
            return

        try:
            payload = self.api_client.fetch_usage(record)
            self._save_success(record, existing, payload)
        except HTTPStatusError as exc:
            if exc.status == 401:
                self._save_invalid_401(
                    record,
                    existing,
                    reason_code="unauthorized_after_refresh",
                    reason_detail=_clean_detail(str(exc)),
                )
                return
            if exc.status == 403:
                self._handle_403(record, existing, exc)
                return
            if exc.status == 429 or 500 <= exc.status <= 599:
                self._save_transient(record, existing, exc.status, str(exc))
                return
            self._save_invalid(
                record,
                existing,
                http_status=exc.status,
                reason_code=f"http_{exc.status}",
                reason_detail=_clean_detail(str(exc)),
            )
        except InvalidResponseError as exc:
            self._save_transient(record, existing, getattr(exc, "status", 200), str(exc))
        except TransportError as exc:
            self._save_transient(record, existing, None, str(exc))

    def _save_invalid_401(
        self,
        record: TokenRecord,
        existing: dict[str, Any] | None,
        reason_code: str,
        reason_detail: str,
    ) -> None:
        previous_count = int(existing.get("consecutive_401_count") or 0) if existing else 0
        current_count = previous_count + 1
        final_record = record
        if current_count >= 2:
            try:
                final_record = move_token_to_auth_invalid(record, self.settings.auth_invalid_dir)
                logger.warning(
                    "账号 %s 连续两轮 401，已剪切 token 文件到 %s",
                    _record_label(final_record),
                    final_record.source_file,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("账号 %s 连续两轮 401，但剪切 token 文件失败", _record_label(record))
        self._save_invalid(
            final_record,
            existing,
            http_status=401,
            reason_code=reason_code,
            reason_detail=reason_detail,
            consecutive_401_count=current_count,
        )

    def _handle_403(
        self,
        record: TokenRecord,
        existing: dict[str, Any] | None,
        error: HTTPStatusError,
    ) -> None:
        previous_count = int(existing.get("consecutive_403_count") or 0) if existing else 0
        current_count = previous_count + 1
        if current_count >= 2:
            self._save_invalid(
                record,
                existing,
                http_status=403,
                reason_code="forbidden_twice",
                reason_detail="连续两轮 403",
                consecutive_403_count=current_count,
            )
            return
        self._save_transient(
            record,
            existing,
            http_status=403,
            error_detail=_clean_detail(str(error)),
            consecutive_403_count=current_count,
        )

    def _base_payload(
        self,
        record: TokenRecord,
        existing: dict[str, Any] | None,
    ) -> dict[str, Any]:
        now = utc_now_iso()
        return {
            "dimension_key": record.dimension_key,
            "account_id": record.account_id,
            "email": record.email,
            "source_file": str(record.source_file),
            "source_mtime_ns": record.source_mtime_ns,
            "source_created_at_utc": record.source_created_at_utc,
            "lifecycle_status": LIFECYCLE_ACTIVE,
            "quota_status": QUOTA_UNKNOWN,
            "plan_type": record.plan_type,
            "chatgpt_user_id": record.chatgpt_user_id,
            "used_percent": None,
            "rate_limit_allowed": None,
            "rate_limit_reached": None,
            "reset_at_utc": None,
            "last_checked_at_utc": now,
            "last_success_at_utc": existing.get("last_success_at_utc") if existing else None,
            "last_http_status": None,
            "consecutive_403_count": 0,
            "consecutive_401_count": 0,
            "invalid_reason_code": None,
            "invalid_reason_detail": None,
            "last_error_detail": None,
            "updated_at_utc": now,
        }

    def _save_success(
        self,
        record: TokenRecord,
        existing: dict[str, Any] | None,
        payload: dict[str, Any],
    ) -> None:
        classified = classify_usage_payload(payload)
        row = self._base_payload(record, existing)
        row.update(
            {
                "quota_status": classified["quota_status"],
                "used_percent": classified["used_percent"],
                "rate_limit_allowed": classified["rate_limit_allowed"],
                "rate_limit_reached": classified["rate_limit_reached"],
                "reset_at_utc": classified["reset_at_utc"],
                "last_http_status": 200,
                "last_success_at_utc": row["last_checked_at_utc"],
            }
        )
        self.database.upsert_account(row)
        logger.info(
            "账号 %s 查询成功，状态=%s，来源=%s",
            _record_label(record),
            row["quota_status"],
            record.source_file_name,
        )

    def _save_transient(
        self,
        record: TokenRecord,
        existing: dict[str, Any] | None,
        http_status: int | None,
        error_detail: str,
        consecutive_403_count: int = 0,
        consecutive_401_count: int = 0,
    ) -> None:
        row = self._base_payload(record, existing)
        row.update(
            {
                "quota_status": QUOTA_UNKNOWN,
                "last_http_status": http_status,
                "consecutive_403_count": consecutive_403_count,
                "consecutive_401_count": consecutive_401_count,
                "last_error_detail": _clean_detail(error_detail),
            }
        )
        self.database.upsert_account(row)
        logger.warning(
            "账号 %s 本轮查询失败，保留 active/unknown: %s",
            _record_label(record),
            row["last_error_detail"],
        )

    def _save_invalid(
        self,
        record: TokenRecord,
        existing: dict[str, Any] | None,
        http_status: int | None,
        reason_code: str,
        reason_detail: str,
        consecutive_403_count: int = 0,
        consecutive_401_count: int = 0,
    ) -> None:
        row = self._base_payload(record, existing)
        row.update(
            {
                "lifecycle_status": LIFECYCLE_INVALID,
                "quota_status": QUOTA_UNKNOWN,
                "last_http_status": http_status,
                "consecutive_403_count": consecutive_403_count,
                "consecutive_401_count": consecutive_401_count,
                "invalid_reason_code": reason_code,
                "invalid_reason_detail": _clean_detail(reason_detail),
                "last_error_detail": _clean_detail(reason_detail),
            }
        )
        self.database.upsert_account(row)
        logger.warning(
            "账号 %s 已标记为 invalid: %s",
            _record_label(record),
            row["invalid_reason_detail"],
        )

    def _mark_source_missing(
        self,
        existing_by_dimension: dict[str, dict[str, Any]],
        scanned_dimension_keys: set[str],
        scanned_source_files: dict[str, str],
    ) -> None:
        for dimension_key, existing in existing_by_dimension.items():
            if dimension_key in scanned_dimension_keys:
                continue

            account_id = str(existing.get("account_id") or "")
            plan_type = str(existing.get("plan_type") or "")
            chatgpt_user_id = str(existing.get("chatgpt_user_id") or "")
            source_file = str(existing.get("source_file") or "")
            mapped_dimension_key = scanned_source_files.get(source_file)
            if mapped_dimension_key:
                self.database.delete_account(dimension_key)
                logger.info(
                    "账号 %s / %s / %s 的旧维度记录已被新维度 %s 替代，已自动清理",
                    account_id,
                    plan_type,
                    chatgpt_user_id,
                    mapped_dimension_key,
                )
                continue

            note = f"源文件缺失: {source_file}" if source_file else "源文件缺失"

            if str(existing.get("lifecycle_status") or "") == LIFECYCLE_INVALID:
                source_path_raw = source_file.strip()
                if source_path_raw:
                    try:
                        if Path(source_path_raw).exists() and not mapped_dimension_key:
                            logger.info(
                                "账号 %s / %s / %s 已是 invalid，且记录中的源文件仍存在，保持 invalid",
                                account_id,
                                plan_type,
                                chatgpt_user_id,
                            )
                            continue
                    except OSError:
                        pass
            row = {
                "dimension_key": dimension_key,
                "account_id": account_id,
                "email": str(existing.get("email") or ""),
                "source_file": source_file,
                "source_mtime_ns": int(existing.get("source_mtime_ns") or 0),
                "source_created_at_utc": str(existing.get("source_created_at_utc") or ""),
                "lifecycle_status": LIFECYCLE_SOURCE_MISSING,
                "quota_status": QUOTA_UNKNOWN,
                "plan_type": plan_type,
                "chatgpt_user_id": chatgpt_user_id,
                "used_percent": None,
                "rate_limit_allowed": None,
                "rate_limit_reached": None,
                "reset_at_utc": None,
                "last_checked_at_utc": existing.get("last_checked_at_utc"),
                "last_success_at_utc": existing.get("last_success_at_utc"),
                "last_http_status": existing.get("last_http_status"),
                "consecutive_403_count": 0,
                "consecutive_401_count": 0,
                "invalid_reason_code": "source_missing",
                "invalid_reason_detail": note,
                "last_error_detail": note,
                "updated_at_utc": utc_now_iso(),
            }
            self.database.upsert_account(row)
            logger.warning(
                "账号 %s / %s / %s 源文件异常，已标记 source_missing",
                account_id,
                plan_type,
                chatgpt_user_id,
            )

    def _runtime_defaults(self) -> dict[str, Any]:
        return self.database.runtime_defaults()

    def _ensure_runtime_state(self) -> dict[str, Any]:
        existing = self.database.ensure_runtime_row()
        runtime = self._runtime_defaults()
        runtime.update(existing)
        self.runtime_state = runtime
        return runtime

    def _update_runtime(self, **changes: Any) -> None:
        runtime = self._ensure_runtime_state()
        now = utc_now_iso()
        runtime.update(changes)
        runtime["runtime_key"] = RUNTIME_KEY
        runtime["last_heartbeat_at_utc"] = now
        runtime["updated_at_utc"] = now
        self.database.upsert_runtime(runtime)

    def _mark_round_started(self) -> None:
        self._update_runtime(
            phase=COLLECTOR_PHASE_SCANNING,
            round_started_at_utc=utc_now_iso(),
            round_finished_at_utc=None,
            next_round_at_utc=None,
            total_scanned=0,
            total_candidates=0,
            processed_candidates=0,
            skipped_candidates=0,
            current_index=0,
            current_account_id=None,
            current_account_email=None,
            current_source_file=None,
            manual_trigger_requested_at=None,
            manual_stop_requested_at=None,
            last_error_detail=None,
        )

    def _mark_query_plan(
        self,
        *,
        total_scanned: int,
        total_candidates: int,
        skipped_candidates: int,
    ) -> None:
        self._update_runtime(
            phase=COLLECTOR_PHASE_QUERYING if total_candidates > 0 else COLLECTOR_PHASE_RECONCILING,
            total_scanned=total_scanned,
            total_candidates=total_candidates,
            processed_candidates=0,
            skipped_candidates=skipped_candidates,
            current_index=0,
            current_account_id=None,
            current_account_email=None,
            current_source_file=None,
            manual_stop_requested_at=None,
        )

    def _mark_current_record(self, *, index: int, record: TokenRecord) -> None:
        self._update_runtime(
            phase=COLLECTOR_PHASE_QUERYING,
            current_index=index + 1,
            current_account_id=record.account_id,
            current_account_email=record.email,
            current_source_file=str(record.source_file),
            processed_candidates=index,
        )

    def _mark_processed(self, processed_candidates: int) -> None:
        self._update_runtime(
            phase=COLLECTOR_PHASE_QUERYING,
            processed_candidates=processed_candidates,
        )

    def _mark_reconciling(self) -> None:
        total_candidates = int(self._ensure_runtime_state().get("total_candidates") or 0)
        self._update_runtime(
            phase=COLLECTOR_PHASE_RECONCILING,
            processed_candidates=total_candidates,
            current_index=0,
            current_account_id=None,
            current_account_email=None,
            current_source_file=None,
            manual_stop_requested_at=None,
        )

    def _mark_idle(self) -> None:
        total_candidates = int(self._ensure_runtime_state().get("total_candidates") or 0)
        self._update_runtime(
            phase=COLLECTOR_PHASE_IDLE,
            round_finished_at_utc=utc_now_iso(),
            next_round_at_utc=None,
            processed_candidates=total_candidates,
            current_index=0,
            current_account_id=None,
            current_account_email=None,
            current_source_file=None,
            manual_stop_requested_at=None,
        )

    def _mark_sleeping(self) -> None:
        self._update_runtime(
            phase=COLLECTOR_PHASE_SLEEPING,
            next_round_at_utc=iso_after_seconds(self.settings.round_interval_seconds),
            current_index=0,
            current_account_id=None,
            current_account_email=None,
            current_source_file=None,
            manual_stop_requested_at=None,
        )

    def _ensure_scheduled_sleep(self) -> None:
        self.database.initialize()
        runtime = self.database.ensure_runtime_row()
        next_round_at = parse_utc(str(runtime.get("next_round_at_utc") or ""))
        # 进程重启或重新部署时不能立即采集；没有调度时间才创建下一次调度。
        self._update_runtime(
            phase=COLLECTOR_PHASE_SLEEPING,
            next_round_at_utc=str(runtime.get("next_round_at_utc") or "") if next_round_at else iso_after_seconds(self.settings.round_interval_seconds),
            current_index=0,
            current_account_id=None,
            current_account_email=None,
            current_source_file=None,
            manual_stop_requested_at=None,
        )

    def _wait_for_next_round(self) -> bool:
        poll_interval = max(float(self.settings.manual_trigger_poll_seconds), 0.2)

        while True:
            self._sync_cpa_status_if_due()
            requested_at = self.database.consume_manual_scan_request()
            if requested_at:
                logger.info("检测到手动触发请求：%s", requested_at)
                return True

            runtime = self.database.ensure_runtime_row()
            next_round_at = parse_utc(str(runtime.get("next_round_at_utc") or ""))
            if next_round_at is None:
                self._ensure_scheduled_sleep()
                continue

            remaining_seconds = (next_round_at - utc_now()).total_seconds()
            if remaining_seconds <= 0:
                return False

            sleep_seconds = min(poll_interval, remaining_seconds)
            cpa_sync_remaining = self._seconds_until_next_cpa_status_sync()
            if cpa_sync_remaining is not None:
                sleep_seconds = min(sleep_seconds, max(cpa_sync_remaining, 0.2))
            self.sleeper(sleep_seconds)

    def _sleep_between_accounts(self) -> bool:
        remaining_seconds = max(float(self.settings.per_account_interval_seconds), 0.0)
        if remaining_seconds <= 0:
            return self._should_stop_current_round("账号间隔前")

        poll_interval = min(0.2, remaining_seconds)
        while remaining_seconds > 0:
            self._sync_cpa_status_if_due()
            if self._should_stop_current_round("账号间隔期间"):
                return True
            current_sleep = min(poll_interval, remaining_seconds)
            self.sleeper(current_sleep)
            remaining_seconds = max(remaining_seconds - current_sleep, 0.0)
        return self._should_stop_current_round("账号间隔结束后")

    def _should_stop_current_round(self, checkpoint: str) -> bool:
        requested_at = self.database.consume_manual_stop_request()
        if not requested_at:
            return False
        logger.info("检测到手动停止请求（%s）：%s", checkpoint, requested_at)
        return True

    def _safe_mark_error(self, exc: Exception) -> None:
        try:
            self.database.initialize()
            self._update_runtime(
                phase=COLLECTOR_PHASE_ERROR,
                round_finished_at_utc=utc_now_iso(),
                next_round_at_utc=iso_after_seconds(self.settings.round_interval_seconds),
                current_index=0,
                current_account_id=None,
                current_account_email=None,
                current_source_file=None,
                manual_stop_requested_at=None,
                last_error_detail=_clean_detail(str(exc)),
            )
        except Exception:  # noqa: BLE001
            logger.exception("更新运行时状态失败")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = load_settings()
    configure_logging(settings.log_level)
    logger.info("tokens 目录: %s", settings.tokens_dir)
    logger.info("数据库路径: %s", settings.db_path)

    service = CollectorService(settings=settings)
    if args.once:
        service.run_once()
        return 0

    service.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
