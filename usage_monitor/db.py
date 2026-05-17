"""SQLite 持久化与运行时状态管理。"""

from __future__ import annotations

import sqlite3
import threading
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable

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
    build_dimension_key,
    normalize_chatgpt_user_id,
    normalize_plan_type,
)
from .timeutil import format_utc, parse_utc, utc_now, utc_now_iso


TABLE_NAME = "accounts_latest"
RUNTIME_TABLE_NAME = "collector_runtime"
SUMMARY_HISTORY_TABLE_NAME = "summary_history"
RUNTIME_KEY = "collector"
SUMMARY_HISTORY_HOURS = 168
DEFAULT_CPA_STATUS_STALE_SECONDS = 120.0
CPA_QUOTA_VALUES = (QUOTA_AVAILABLE, QUOTA_EXHAUSTED, QUOTA_UNKNOWN)
CPA_STATUS_COLUMN_DEFINITIONS = {
    "cpa_quota_status": "TEXT",
    "cpa_reset_at_utc": "TEXT",
    "cpa_synced_at_utc": "TEXT",
    "cpa_status": "TEXT",
    "cpa_status_message": "TEXT",
}
SUMMARY_KEYS = (
    "total",
    "active",
    "available",
    "exhausted",
    "unknown",
    "invalid",
    "source_missing",
)
FILTERS = {
    "all",
    "active",
    "available",
    "exhausted",
    "unknown",
    "invalid",
    "source_missing",
}
MANUAL_START_ALLOWED_PHASES = {
    COLLECTOR_PHASE_IDLE,
    COLLECTOR_PHASE_SLEEPING,
    COLLECTOR_PHASE_ERROR,
}
MANUAL_STOP_ALLOWED_PHASES = {
    COLLECTOR_PHASE_SCANNING,
    COLLECTOR_PHASE_QUERYING,
    COLLECTOR_PHASE_RECONCILING,
}


SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    dimension_key TEXT PRIMARY KEY,
    account_id TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL DEFAULT '',
    source_file TEXT NOT NULL DEFAULT '',
    source_mtime_ns INTEGER NOT NULL DEFAULT 0,
    lifecycle_status TEXT NOT NULL,
    quota_status TEXT NOT NULL,
    plan_type TEXT NOT NULL DEFAULT '',
    chatgpt_user_id TEXT NOT NULL DEFAULT '',
    used_percent REAL,
    rate_limit_allowed INTEGER,
    rate_limit_reached INTEGER,
    reset_at_utc TEXT,
    last_checked_at_utc TEXT,
    last_success_at_utc TEXT,
    last_http_status INTEGER,
    consecutive_403_count INTEGER NOT NULL DEFAULT 0,
    consecutive_401_count INTEGER NOT NULL DEFAULT 0,
    invalid_reason_code TEXT,
    invalid_reason_detail TEXT,
    last_error_detail TEXT,
    cpa_quota_status TEXT,
    cpa_reset_at_utc TEXT,
    cpa_synced_at_utc TEXT,
    cpa_status TEXT,
    cpa_status_message TEXT,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {RUNTIME_TABLE_NAME} (
    runtime_key TEXT PRIMARY KEY,
    phase TEXT NOT NULL,
    accounts_revision INTEGER NOT NULL DEFAULT 0,
    runtime_revision INTEGER NOT NULL DEFAULT 0,
    round_started_at_utc TEXT,
    round_finished_at_utc TEXT,
    next_round_at_utc TEXT,
    total_scanned INTEGER NOT NULL DEFAULT 0,
    total_candidates INTEGER NOT NULL DEFAULT 0,
    processed_candidates INTEGER NOT NULL DEFAULT 0,
    skipped_candidates INTEGER NOT NULL DEFAULT 0,
    current_index INTEGER NOT NULL DEFAULT 0,
    current_account_id TEXT,
    current_account_email TEXT,
    current_source_file TEXT,
    manual_trigger_requested_at TEXT,
    manual_stop_requested_at TEXT,
    last_error_detail TEXT,
    last_heartbeat_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {SUMMARY_HISTORY_TABLE_NAME} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at_utc TEXT NOT NULL,
    accounts_revision INTEGER NOT NULL DEFAULT 0,
    total INTEGER NOT NULL DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 0,
    available INTEGER NOT NULL DEFAULT 0,
    exhausted INTEGER NOT NULL DEFAULT 0,
    unknown INTEGER NOT NULL DEFAULT 0,
    invalid INTEGER NOT NULL DEFAULT 0,
    source_missing INTEGER NOT NULL DEFAULT 0
);
"""


UPSERT_COLUMNS = [
    "dimension_key",
    "account_id",
    "email",
    "source_file",
    "source_mtime_ns",
    "lifecycle_status",
    "quota_status",
    "plan_type",
    "chatgpt_user_id",
    "used_percent",
    "rate_limit_allowed",
    "rate_limit_reached",
    "reset_at_utc",
    "last_checked_at_utc",
    "last_success_at_utc",
    "last_http_status",
    "consecutive_403_count",
    "consecutive_401_count",
    "invalid_reason_code",
    "invalid_reason_detail",
    "last_error_detail",
    "updated_at_utc",
]


ACCOUNTS_CREATE_SQL = f"""
CREATE TABLE {TABLE_NAME} (
    dimension_key TEXT PRIMARY KEY,
    account_id TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL DEFAULT '',
    source_file TEXT NOT NULL DEFAULT '',
    source_mtime_ns INTEGER NOT NULL DEFAULT 0,
    lifecycle_status TEXT NOT NULL,
    quota_status TEXT NOT NULL,
    plan_type TEXT NOT NULL DEFAULT '',
    chatgpt_user_id TEXT NOT NULL DEFAULT '',
    used_percent REAL,
    rate_limit_allowed INTEGER,
    rate_limit_reached INTEGER,
    reset_at_utc TEXT,
    last_checked_at_utc TEXT,
    last_success_at_utc TEXT,
    last_http_status INTEGER,
    consecutive_403_count INTEGER NOT NULL DEFAULT 0,
    consecutive_401_count INTEGER NOT NULL DEFAULT 0,
    invalid_reason_code TEXT,
    invalid_reason_detail TEXT,
    last_error_detail TEXT,
    cpa_quota_status TEXT,
    cpa_reset_at_utc TEXT,
    cpa_synced_at_utc TEXT,
    cpa_status TEXT,
    cpa_status_message TEXT,
    updated_at_utc TEXT NOT NULL
)
"""


RUNTIME_UPSERT_COLUMNS = [
    "runtime_key",
    "phase",
    "accounts_revision",
    "runtime_revision",
    "round_started_at_utc",
    "round_finished_at_utc",
    "next_round_at_utc",
    "total_scanned",
    "total_candidates",
    "processed_candidates",
    "skipped_candidates",
    "current_index",
    "current_account_id",
    "current_account_email",
    "current_source_file",
    "manual_trigger_requested_at",
    "manual_stop_requested_at",
    "last_error_detail",
    "last_heartbeat_at_utc",
    "updated_at_utc",
]


class UsageDatabase:
    _initialized_paths: set[str] = set()
    _initialize_lock = threading.Lock()

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self._cache_key = str(self.db_path.expanduser().resolve())
        self._parent_dir_ready = False
        self._initialized = False

    def _connect(self) -> sqlite3.Connection:
        if not self._parent_dir_ready:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._parent_dir_ready = True
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _initialize_cache_key(self) -> str:
        return self._cache_key

    def initialize(self) -> None:
        if self._initialized and self.db_path.exists():
            return

        cache_key = self._initialize_cache_key()
        if cache_key in self._initialized_paths and self.db_path.exists():
            self._initialized = True
            return

        with self._initialize_lock:
            if cache_key in self._initialized_paths and self.db_path.exists():
                self._initialized = True
                return

            with self._connect() as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.executescript(SCHEMA)
                self._ensure_accounts_schema(conn)
                self._ensure_runtime_schema(conn)
                self._ensure_summary_history_schema(conn)
                self._ensure_indexes(conn)
                self._seed_summary_history_if_empty(conn)
                conn.commit()

            self._initialized_paths.add(cache_key)
            self._initialized = True


    @staticmethod
    def _accounts_columns(conn: sqlite3.Connection) -> set[str]:
        return {
            str(row["name"])
            for row in conn.execute(f"PRAGMA table_info({TABLE_NAME})").fetchall()
        }

    @classmethod
    def _ensure_accounts_schema(cls, conn: sqlite3.Connection) -> None:
        existing_columns = cls._accounts_columns(conn)
        if "dimension_key" not in existing_columns or "chatgpt_user_id" not in existing_columns:
            cls._rebuild_accounts_table(conn)
            existing_columns = cls._accounts_columns(conn)
        if "consecutive_401_count" not in existing_columns:
            conn.execute(
                f"ALTER TABLE {TABLE_NAME} ADD COLUMN consecutive_401_count INTEGER NOT NULL DEFAULT 0"
            )
        for column, definition in CPA_STATUS_COLUMN_DEFINITIONS.items():
            if column not in existing_columns:
                conn.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN {column} {definition}")

    @classmethod
    def _rebuild_accounts_table(cls, conn: sqlite3.Connection) -> None:
        legacy_table_name = f"{TABLE_NAME}__legacy"
        suffix = 2
        while conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (legacy_table_name,),
        ).fetchone():
            legacy_table_name = f"{TABLE_NAME}__legacy_{suffix}"
            suffix += 1

        conn.execute(f"ALTER TABLE {TABLE_NAME} RENAME TO {legacy_table_name}")
        conn.execute(ACCOUNTS_CREATE_SQL)

        legacy_rows = conn.execute(f"SELECT * FROM {legacy_table_name}").fetchall()
        placeholders = ", ".join("?" for _ in UPSERT_COLUMNS)
        update_clause = ", ".join(
            f"{column}=excluded.{column}"
            for column in UPSERT_COLUMNS
            if column != "dimension_key"
        )
        insert_sql = (
            f"INSERT INTO {TABLE_NAME} ({', '.join(UPSERT_COLUMNS)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT(dimension_key) DO UPDATE SET {update_clause}"
        )

        for row in legacy_rows:
            payload = {key: row[key] for key in row.keys()}
            payload["plan_type"] = normalize_plan_type(payload.get("plan_type"))
            payload["chatgpt_user_id"] = normalize_chatgpt_user_id(payload.get("chatgpt_user_id"))
            payload["dimension_key"] = build_dimension_key(
                str(payload.get("account_id") or ""),
                payload["plan_type"],
                payload["chatgpt_user_id"],
            )
            if payload.get("consecutive_403_count") is None:
                payload["consecutive_403_count"] = 0
            if payload.get("consecutive_401_count") is None:
                payload["consecutive_401_count"] = 0
            values = [payload.get(column) for column in UPSERT_COLUMNS]
            conn.execute(insert_sql, values)

        conn.execute(f"DROP TABLE {legacy_table_name}")

    @staticmethod
    def _ensure_runtime_schema(conn: sqlite3.Connection) -> None:
        existing_columns = {
            str(row["name"])
            for row in conn.execute(f"PRAGMA table_info({RUNTIME_TABLE_NAME})").fetchall()
        }
        if "accounts_revision" not in existing_columns:
            conn.execute(
                f"ALTER TABLE {RUNTIME_TABLE_NAME} ADD COLUMN accounts_revision INTEGER NOT NULL DEFAULT 0"
            )
        if "runtime_revision" not in existing_columns:
            conn.execute(
                f"ALTER TABLE {RUNTIME_TABLE_NAME} ADD COLUMN runtime_revision INTEGER NOT NULL DEFAULT 0"
            )
        if "manual_trigger_requested_at" not in existing_columns:
            conn.execute(
                f"ALTER TABLE {RUNTIME_TABLE_NAME} ADD COLUMN manual_trigger_requested_at TEXT"
            )
        if "manual_stop_requested_at" not in existing_columns:
            conn.execute(
                f"ALTER TABLE {RUNTIME_TABLE_NAME} ADD COLUMN manual_stop_requested_at TEXT"
            )

    @staticmethod
    def _ensure_summary_history_schema(conn: sqlite3.Connection) -> None:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SUMMARY_HISTORY_TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                captured_at_utc TEXT NOT NULL,
                accounts_revision INTEGER NOT NULL DEFAULT 0,
                total INTEGER NOT NULL DEFAULT 0,
                active INTEGER NOT NULL DEFAULT 0,
                available INTEGER NOT NULL DEFAULT 0,
                exhausted INTEGER NOT NULL DEFAULT 0,
                unknown INTEGER NOT NULL DEFAULT 0,
                invalid INTEGER NOT NULL DEFAULT 0,
                source_missing INTEGER NOT NULL DEFAULT 0
            )
            """
        )

    @staticmethod
    def _ensure_indexes(conn: sqlite3.Connection) -> None:
        conn.executescript(
            f"""
            CREATE INDEX IF NOT EXISTS idx_accounts_latest_sort_all
            ON {TABLE_NAME} (
                last_checked_at_utc DESC,
                updated_at_utc DESC,
                email ASC
            );

            CREATE INDEX IF NOT EXISTS idx_accounts_latest_lifecycle_sort
            ON {TABLE_NAME} (
                lifecycle_status,
                last_checked_at_utc DESC,
                updated_at_utc DESC,
                email ASC
            );

            CREATE INDEX IF NOT EXISTS idx_accounts_latest_lifecycle_quota_sort
            ON {TABLE_NAME} (
                lifecycle_status,
                quota_status,
                last_checked_at_utc DESC,
                updated_at_utc DESC,
                email ASC
            );

            CREATE INDEX IF NOT EXISTS idx_summary_history_captured
            ON {SUMMARY_HISTORY_TABLE_NAME} (id DESC, captured_at_utc DESC);
            """
        )

    @staticmethod
    def _cpa_status_overlay_enabled(cpa_stale_seconds: float | None) -> bool:
        """判断 Dashboard 是否允许使用 CPA overlay。"""

        if cpa_stale_seconds is None:
            return True
        try:
            stale_seconds = float(cpa_stale_seconds)
        except (TypeError, ValueError):
            return True
        return stale_seconds > 0

    @staticmethod
    def _cpa_cutoff_utc(cpa_stale_seconds: float | None) -> str:
        if cpa_stale_seconds is None:
            cpa_stale_seconds = DEFAULT_CPA_STATUS_STALE_SECONDS
        try:
            stale_seconds = float(cpa_stale_seconds)
        except (TypeError, ValueError):
            stale_seconds = DEFAULT_CPA_STATUS_STALE_SECONDS
        if stale_seconds <= 0:
            return "9999-12-31T23:59:59Z"
        return format_utc(utc_now() - timedelta(seconds=stale_seconds))

    @classmethod
    def _effective_accounts_sql(cls, cpa_stale_seconds: float | None) -> tuple[str, tuple[Any, ...]]:
        cutoff_utc = cls._cpa_cutoff_utc(cpa_stale_seconds)
        now_utc = format_utc(utc_now())
        cpa_overlay_enabled = 1 if cls._cpa_status_overlay_enabled(cpa_stale_seconds) else 0
        freshness_expr = (
            "? = 1 "
            "AND lifecycle_status = ? "
            "AND cpa_synced_at_utc IS NOT NULL "
            "AND cpa_synced_at_utc >= ? "
            "AND cpa_quota_status IN (?, ?, ?)"
        )
        # CPA 的 active 是运行时瞬态状态；官方扫描或 CPA 已耗尽且 reset 未到时，
        # 不允许它把账号反向冲成 available，避免刷新 auth 文件时出现假恢复。
        sticky_exhausted_expr = (
            "quota_status = ? "
            "AND reset_at_utc IS NOT NULL "
            "AND reset_at_utc != '' "
            "AND reset_at_utc > ?"
        )
        cpa_sticky_exhausted_expr = (
            "? = 1 "
            "AND cpa_quota_status = ? "
            "AND cpa_reset_at_utc IS NOT NULL "
            "AND cpa_reset_at_utc != '' "
            "AND cpa_reset_at_utc > ?"
        )
        # CPA 的 usage_limit_reached 可能在 auth-files 中长期残留；如果它自带的
        # reset 时间已经过去，就不能继续覆盖官方扫描出的 available。没有 reset
        # 的 CPA exhausted 仍按当前状态处理，避免漏掉 CPA 已知但未携带时间的耗尽。
        cpa_current_exhausted_expr = (
            "? = 1 "
            "AND cpa_quota_status = ? "
            "AND (cpa_reset_at_utc IS NULL "
            "OR cpa_reset_at_utc = '' "
            "OR cpa_reset_at_utc > ?)"
        )
        freshness_params: tuple[Any, ...] = (
            cpa_overlay_enabled,
            LIFECYCLE_ACTIVE,
            cutoff_utc,
            *CPA_QUOTA_VALUES,
        )
        sticky_exhausted_params: tuple[Any, ...] = (QUOTA_EXHAUSTED, now_utc)
        cpa_sticky_exhausted_params: tuple[Any, ...] = (cpa_overlay_enabled, QUOTA_EXHAUSTED, now_utc)
        cpa_current_exhausted_params: tuple[Any, ...] = (cpa_overlay_enabled, QUOTA_EXHAUSTED, now_utc)
        exhausted_literal = QUOTA_EXHAUSTED.replace("'", "''")
        sql = f"""
            SELECT
                base.*,
                CASE
                    WHEN base.cpa_exhausted_still_waiting
                    THEN '{exhausted_literal}'
                    WHEN base.cpa_status_is_fresh
                        AND base.cpa_exhausted_is_current
                    THEN base.cpa_quota_status
                    WHEN base.cpa_status_is_fresh
                        AND base.cpa_quota_status != '{exhausted_literal}'
                        AND NOT base.official_exhausted_still_waiting
                    THEN base.cpa_quota_status
                    ELSE base.quota_status
                END AS effective_quota_status,
                CASE
                    WHEN base.cpa_exhausted_still_waiting
                    THEN base.cpa_reset_at_utc
                    WHEN base.cpa_status_is_fresh
                        AND base.cpa_exhausted_is_current
                        AND base.cpa_reset_at_utc IS NOT NULL
                        AND base.cpa_reset_at_utc != ''
                    THEN base.cpa_reset_at_utc
                    WHEN base.official_exhausted_still_waiting
                    THEN base.reset_at_utc
                    WHEN base.cpa_status_is_fresh
                        AND base.cpa_quota_status != '{exhausted_literal}'
                        AND base.cpa_reset_at_utc IS NOT NULL
                        AND base.cpa_reset_at_utc != ''
                    THEN base.cpa_reset_at_utc
                    ELSE base.reset_at_utc
                END AS effective_reset_at_utc
            FROM (
                SELECT
                    *,
                    ({freshness_expr}) AS cpa_status_is_fresh,
                    ({sticky_exhausted_expr}) AS official_exhausted_still_waiting,
                    ({cpa_sticky_exhausted_expr}) AS cpa_exhausted_still_waiting,
                    ({cpa_current_exhausted_expr}) AS cpa_exhausted_is_current
                FROM {TABLE_NAME}
            ) AS base
        """
        return (
            sql,
            freshness_params
            + sticky_exhausted_params
            + cpa_sticky_exhausted_params
            + cpa_current_exhausted_params,
        )

    def _fetch_summary_counts_in_transaction(
        self,
        conn: sqlite3.Connection,
        cpa_stale_seconds: float | None = None,
    ) -> dict[str, int]:
        effective_sql, effective_params = self._effective_accounts_sql(cpa_stale_seconds)
        summary_row = conn.execute(
            f"""
            WITH effective_accounts AS ({effective_sql})
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN lifecycle_status = ? THEN 1 ELSE 0 END) AS active,
                SUM(CASE WHEN lifecycle_status = ? AND effective_quota_status = ? THEN 1 ELSE 0 END) AS available,
                SUM(CASE WHEN lifecycle_status = ? AND effective_quota_status = ? THEN 1 ELSE 0 END) AS exhausted,
                SUM(CASE WHEN lifecycle_status = ? AND effective_quota_status = ? THEN 1 ELSE 0 END) AS unknown,
                SUM(CASE WHEN lifecycle_status = ? THEN 1 ELSE 0 END) AS invalid,
                SUM(CASE WHEN lifecycle_status = ? THEN 1 ELSE 0 END) AS source_missing
            FROM effective_accounts
            """,
            (
                *effective_params,
                LIFECYCLE_ACTIVE,
                LIFECYCLE_ACTIVE,
                QUOTA_AVAILABLE,
                LIFECYCLE_ACTIVE,
                QUOTA_EXHAUSTED,
                LIFECYCLE_ACTIVE,
                QUOTA_UNKNOWN,
                LIFECYCLE_INVALID,
                LIFECYCLE_SOURCE_MISSING,
            ),
        ).fetchone()
        return {key: int(summary_row[key] or 0) for key in SUMMARY_KEYS}

    def _insert_summary_history_snapshot_in_transaction(
        self,
        conn: sqlite3.Connection,
        summary: dict[str, int],
    ) -> None:
        runtime_row = self._ensure_runtime_row_in_transaction(conn)
        conn.execute(
            f"""
            INSERT INTO {SUMMARY_HISTORY_TABLE_NAME} (
                captured_at_utc,
                accounts_revision,
                total,
                active,
                available,
                exhausted,
                unknown,
                invalid,
                source_missing
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                utc_now_iso(),
                int(runtime_row["accounts_revision"] or 0),
                *(int(summary.get(key) or 0) for key in SUMMARY_KEYS),
            ),
        )

    def _record_summary_history_if_changed_in_transaction(
        self,
        conn: sqlite3.Connection,
        cpa_stale_seconds: float | None = None,
    ) -> None:
        summary = self._fetch_summary_counts_in_transaction(conn, cpa_stale_seconds)
        latest_row = conn.execute(
            f"""
            SELECT exhausted
            FROM {SUMMARY_HISTORY_TABLE_NAME}
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        if latest_row is not None and int(latest_row["exhausted"] or 0) == int(summary["exhausted"]):
            return
        self._insert_summary_history_snapshot_in_transaction(conn, summary)

    def _seed_summary_history_if_empty(self, conn: sqlite3.Connection) -> None:
        existing_count = int(
            conn.execute(f"SELECT COUNT(*) FROM {SUMMARY_HISTORY_TABLE_NAME}").fetchone()[0] or 0
        )
        if existing_count > 0:
            return
        account_count = int(conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0] or 0)
        if account_count <= 0:
            return
        self._insert_summary_history_snapshot_in_transaction(
            conn,
            self._fetch_summary_counts_in_transaction(conn),
        )

    def ping(self) -> None:
        with self._connect() as conn:
            conn.execute("SELECT 1")

    def fetch_accounts_index(self) -> dict[str, dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(f"SELECT * FROM {TABLE_NAME}").fetchall()
        return {
            str(row["dimension_key"]): {key: row[key] for key in row.keys()}
            for row in rows
        }

    def delete_account(self, dimension_key: str) -> None:
        with self._connect() as conn:
            self._ensure_runtime_row_in_transaction(conn)
            conn.execute(
                f"DELETE FROM {TABLE_NAME} WHERE dimension_key = ?",
                (str(dimension_key or ""),),
            )
            self._bump_accounts_revision_in_transaction(conn)
            self._record_summary_history_if_changed_in_transaction(conn)
            conn.commit()

    def upsert_account(self, payload: dict[str, Any]) -> None:
        normalized_payload = dict(payload)
        normalized_payload["plan_type"] = normalize_plan_type(normalized_payload.get("plan_type"))
        normalized_payload["chatgpt_user_id"] = normalize_chatgpt_user_id(
            normalized_payload.get("chatgpt_user_id")
        )
        normalized_payload["dimension_key"] = normalized_payload.get("dimension_key") or build_dimension_key(
            str(normalized_payload.get("account_id") or ""),
            normalized_payload["plan_type"],
            normalized_payload["chatgpt_user_id"],
        )
        for column in ("consecutive_403_count", "consecutive_401_count"):
            if normalized_payload.get(column) is None:
                normalized_payload[column] = 0

        values = [normalized_payload.get(column) for column in UPSERT_COLUMNS]
        placeholders = ", ".join("?" for _ in UPSERT_COLUMNS)
        update_clause = ", ".join(
            f"{column}=excluded.{column}"
            for column in UPSERT_COLUMNS
            if column != "dimension_key"
        )
        sql = (
            f"INSERT INTO {TABLE_NAME} ({', '.join(UPSERT_COLUMNS)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT(dimension_key) DO UPDATE SET {update_clause}"
        )
        with self._connect() as conn:
            self._ensure_runtime_row_in_transaction(conn)
            conn.execute(sql, values)
            self._bump_accounts_revision_in_transaction(conn)
            self._record_summary_history_if_changed_in_transaction(conn)
            conn.commit()

    def sync_cpa_statuses(
        self,
        statuses: Iterable[Any],
        *,
        cpa_stale_seconds: float | None = None,
    ) -> dict[str, int]:
        """同步 CPA 只读状态到 Monitor 自己的 overlay 字段。"""

        by_file: dict[str, dict[str, Any]] = {}
        by_email: dict[str, dict[str, Any]] = {}
        total_statuses = 0
        for status in statuses:
            payload = status.as_db_payload() if hasattr(status, "as_db_payload") else dict(status)
            source_file_name = Path(str(payload.get("source_file_name") or "")).name.lower()
            email = str(payload.get("email") or "").strip().lower()
            if not source_file_name and not email:
                continue
            total_statuses += 1
            normalized = {
                "quota_status": str(payload.get("quota_status") or QUOTA_UNKNOWN),
                "reset_at_utc": payload.get("reset_at_utc"),
                "cpa_status": str(payload.get("cpa_status") or "unknown"),
                "cpa_status_message": str(payload.get("cpa_status_message") or ""),
            }
            if source_file_name:
                by_file[source_file_name] = normalized
            if email:
                by_email[email] = normalized

        now_dt = utc_now()
        now = format_utc(now_dt)

        def is_future_utc(value: Any) -> bool:
            parsed = parse_utc(str(value or ""))
            return parsed is not None and parsed > now_dt

        matched_rows = 0
        changed_rows = 0
        cleared_rows = 0
        with self._connect() as conn:
            self._ensure_runtime_row_in_transaction(conn)
            rows = conn.execute(
                f"""
                SELECT
                    dimension_key,
                    email,
                    source_file,
                    lifecycle_status,
                    quota_status,
                    reset_at_utc,
                    cpa_quota_status,
                    cpa_reset_at_utc,
                    cpa_synced_at_utc,
                    cpa_status,
                    cpa_status_message
                FROM {TABLE_NAME}
                """
            ).fetchall()
            for row in rows:
                source_file_name = Path(str(row["source_file"] or "")).name.lower()
                email = str(row["email"] or "").strip().lower()
                payload = None
                if str(row["lifecycle_status"] or "") == LIFECYCLE_ACTIVE:
                    payload = by_file.get(source_file_name) or by_email.get(email)
                existing_cpa_reset_at = row["cpa_reset_at_utc"]
                cpa_exhausted_still_waiting = (
                    str(row["cpa_quota_status"] or "") == QUOTA_EXHAUSTED
                    and is_future_utc(existing_cpa_reset_at)
                )
                official_reset_fallback = row["reset_at_utc"] if is_future_utc(row["reset_at_utc"]) else None

                if payload is not None:
                    matched_rows += 1
                    incoming_quota_status = str(payload["quota_status"] or QUOTA_UNKNOWN)
                    incoming_reset_at = payload["reset_at_utc"]
                    if incoming_quota_status == QUOTA_EXHAUSTED and not incoming_reset_at:
                        incoming_reset_at = existing_cpa_reset_at if is_future_utc(existing_cpa_reset_at) else official_reset_fallback
                    if cpa_exhausted_still_waiting and incoming_quota_status != QUOTA_EXHAUSTED:
                        next_values = (
                            QUOTA_EXHAUSTED,
                            existing_cpa_reset_at,
                            now,
                            row["cpa_status"] or "error",
                            row["cpa_status_message"] or "The usage limit has been reached",
                        )
                    else:
                        next_values = (
                            incoming_quota_status,
                            incoming_reset_at,
                            now,
                            payload["cpa_status"],
                            payload["cpa_status_message"],
                        )
                else:
                    if cpa_exhausted_still_waiting:
                        next_values = (
                            QUOTA_EXHAUSTED,
                            existing_cpa_reset_at,
                            now,
                            row["cpa_status"] or "error",
                            row["cpa_status_message"] or "The usage limit has been reached",
                        )
                    else:
                        next_values = (None, None, None, None, None)
                        if row["cpa_synced_at_utc"] is not None:
                            cleared_rows += 1

                current_values = (
                    row["cpa_quota_status"],
                    row["cpa_reset_at_utc"],
                    row["cpa_synced_at_utc"],
                    row["cpa_status"],
                    row["cpa_status_message"],
                )
                if current_values == next_values:
                    continue

                conn.execute(
                    f"""
                    UPDATE {TABLE_NAME}
                    SET
                        cpa_quota_status = ?,
                        cpa_reset_at_utc = ?,
                        cpa_synced_at_utc = ?,
                        cpa_status = ?,
                        cpa_status_message = ?
                    WHERE dimension_key = ?
                    """,
                    (*next_values, row["dimension_key"]),
                )
                changed_rows += 1

            if changed_rows > 0:
                self._bump_accounts_revision_in_transaction(conn)
                self._record_summary_history_if_changed_in_transaction(conn, cpa_stale_seconds)
            conn.commit()

        return {
            "statuses": total_statuses,
            "matched": matched_rows,
            "changed": changed_rows,
            "cleared": cleared_rows,
        }

    def upsert_runtime(self, payload: dict[str, Any]) -> None:
        normalized_payload = dict(payload)
        runtime_key = str(normalized_payload.get("runtime_key") or RUNTIME_KEY)
        placeholders = ", ".join("?" for _ in RUNTIME_UPSERT_COLUMNS)
        update_clause = ", ".join(
            f"{column}=excluded.{column}"
            for column in RUNTIME_UPSERT_COLUMNS
            if column != "runtime_key"
        )
        sql = (
            f"INSERT INTO {RUNTIME_TABLE_NAME} ({', '.join(RUNTIME_UPSERT_COLUMNS)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT(runtime_key) DO UPDATE SET {update_clause}"
        )
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT accounts_revision, runtime_revision
                FROM {RUNTIME_TABLE_NAME}
                WHERE runtime_key = ?
                """,
                (runtime_key,),
            ).fetchone()
            normalized_payload["runtime_key"] = runtime_key
            normalized_payload["accounts_revision"] = (
                int(row["accounts_revision"] or 0)
                if row is not None
                else int(normalized_payload.get("accounts_revision") or 0)
            )
            normalized_payload["runtime_revision"] = (
                int(row["runtime_revision"] or 0) + 1
                if row is not None
                else max(int(normalized_payload.get("runtime_revision") or 0), 0) + 1
            )
            values = [normalized_payload.get(column) for column in RUNTIME_UPSERT_COLUMNS]
            conn.execute(sql, values)
            conn.commit()

    def fetch_runtime(self) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT * FROM {RUNTIME_TABLE_NAME} WHERE runtime_key = ?",
                (RUNTIME_KEY,),
            ).fetchone()
        if row is None:
            return None
        return {key: row[key] for key in row.keys()}

    def fetch_exhausted_history(
        self,
        hours: int = SUMMARY_HISTORY_HOURS,
        *,
        now_utc: str | None = None,
    ) -> list[dict[str, Any]]:
        try:
            safe_hours = int(hours)
        except (TypeError, ValueError):
            safe_hours = SUMMARY_HISTORY_HOURS
        safe_hours = max(1, min(safe_hours, 1000))

        parsed_now = parse_utc(now_utc) if now_utc else None
        actual_end_at = parsed_now or utc_now()
        end_bucket_at = actual_end_at.replace(minute=0, second=0, microsecond=0)
        start_bucket_at = end_bucket_at - timedelta(hours=safe_hours - 1)

        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT captured_at_utc, exhausted
                FROM {SUMMARY_HISTORY_TABLE_NAME}
                WHERE captured_at_utc >= ? AND captured_at_utc <= ?
                ORDER BY captured_at_utc ASC, id ASC
                """,
                (format_utc(start_bucket_at), format_utc(actual_end_at)),
            ).fetchall()

        events: list[tuple[Any, int]] = []
        for row in rows:
            captured_at = parse_utc(str(row["captured_at_utc"] or ""))
            if captured_at is not None:
                events.append((captured_at, max(0, int(row["exhausted"] or 0))))

        points: list[dict[str, Any]] = []
        event_index = 0
        current_value = 0
        for offset in range(safe_hours):
            bucket_at = start_bucket_at + timedelta(hours=offset)
            bucket_limit = bucket_at + timedelta(hours=1)
            if offset == safe_hours - 1:
                while event_index < len(events) and events[event_index][0] <= actual_end_at:
                    current_value = int(events[event_index][1])
                    event_index += 1
            else:
                while event_index < len(events) and events[event_index][0] < bucket_limit:
                    current_value = int(events[event_index][1])
                    event_index += 1
            points.append(
                {
                    "captured_at_utc": format_utc(bucket_at),
                    "exhausted": current_value,
                }
            )
        return points

    def fetch_exhausted_recovery_projection(
        self,
        hours: int = SUMMARY_HISTORY_HOURS,
        *,
        now_utc: str | None = None,
        cpa_stale_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        """按当前 exhausted 账号的重置时间生成未来额度恢复曲线。"""
        try:
            safe_hours = int(hours)
        except (TypeError, ValueError):
            safe_hours = SUMMARY_HISTORY_HOURS
        safe_hours = max(1, min(safe_hours, 1000))

        parsed_now = parse_utc(now_utc) if now_utc else None
        window_start_at = parsed_now or utc_now()
        window_end_at = window_start_at + timedelta(hours=safe_hours)
        effective_sql, effective_params = self._effective_accounts_sql(cpa_stale_seconds)

        with self._connect() as conn:
            rows = conn.execute(
                f"""
                WITH effective_accounts AS ({effective_sql})
                SELECT effective_reset_at_utc
                FROM effective_accounts
                WHERE lifecycle_status = ? AND effective_quota_status = ?
                """,
                (*effective_params, LIFECYCLE_ACTIVE, QUOTA_EXHAUSTED),
            ).fetchall()

        reset_events: list[Any] = []
        for row in rows:
            reset_at = parse_utc(str(row["effective_reset_at_utc"] or ""))
            if reset_at is None:
                continue
            if window_start_at < reset_at <= window_end_at:
                reset_events.append(reset_at)
        reset_events.sort()

        current_exhausted = len(rows)
        points: list[dict[str, Any]] = []
        event_index = 0
        recovered = 0
        for offset in range(safe_hours + 1):
            projected_at = window_start_at + timedelta(hours=offset)
            while event_index < len(reset_events) and reset_events[event_index] <= projected_at:
                recovered += 1
                event_index += 1
            points.append(
                {
                    "projected_at_utc": format_utc(projected_at),
                    "exhausted": max(0, current_exhausted - recovered),
                    "recovered": recovered,
                }
            )
        return points

    def fetch_dashboard(
        self,
        filter_name: str = "all",
        *,
        cpa_stale_seconds: float | None = None,
    ) -> tuple[dict[str, int], list[sqlite3.Row]]:
        current_filter = filter_name if filter_name in FILTERS else "all"
        effective_sql, effective_params = self._effective_accounts_sql(cpa_stale_seconds)
        with self._connect() as conn:
            summary_row = conn.execute(
                f"""
                WITH effective_accounts AS ({effective_sql})
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN lifecycle_status = ? THEN 1 ELSE 0 END) AS active,
                    SUM(CASE WHEN lifecycle_status = ? AND effective_quota_status = ? THEN 1 ELSE 0 END) AS available,
                    SUM(CASE WHEN lifecycle_status = ? AND effective_quota_status = ? THEN 1 ELSE 0 END) AS exhausted,
                    SUM(CASE WHEN lifecycle_status = ? AND effective_quota_status = ? THEN 1 ELSE 0 END) AS unknown,
                    SUM(CASE WHEN lifecycle_status = ? THEN 1 ELSE 0 END) AS invalid,
                    SUM(CASE WHEN lifecycle_status = ? THEN 1 ELSE 0 END) AS source_missing
                FROM effective_accounts
                """,
                (
                    *effective_params,
                    LIFECYCLE_ACTIVE,
                    LIFECYCLE_ACTIVE,
                    QUOTA_AVAILABLE,
                    LIFECYCLE_ACTIVE,
                    QUOTA_EXHAUSTED,
                    LIFECYCLE_ACTIVE,
                    QUOTA_UNKNOWN,
                    LIFECYCLE_INVALID,
                    LIFECYCLE_SOURCE_MISSING,
                ),
            ).fetchone()

            params: Iterable[Any] = ()
            where_clause = ""
            if current_filter == "active":
                where_clause = "WHERE lifecycle_status = ?"
                params = (LIFECYCLE_ACTIVE,)
            elif current_filter in {QUOTA_AVAILABLE, QUOTA_EXHAUSTED, QUOTA_UNKNOWN}:
                where_clause = "WHERE lifecycle_status = ? AND effective_quota_status = ?"
                params = (LIFECYCLE_ACTIVE, current_filter)
            elif current_filter == LIFECYCLE_INVALID:
                where_clause = "WHERE lifecycle_status = ?"
                params = (LIFECYCLE_INVALID,)
            elif current_filter == LIFECYCLE_SOURCE_MISSING:
                where_clause = "WHERE lifecycle_status = ?"
                params = (LIFECYCLE_SOURCE_MISSING,)

            rows = conn.execute(
                f"""
                WITH effective_accounts AS ({effective_sql})
                SELECT
                    dimension_key,
                    email,
                    lifecycle_status,
                    quota_status AS official_quota_status,
                    effective_quota_status AS quota_status,
                    used_percent,
                    reset_at_utc AS official_reset_at_utc,
                    effective_reset_at_utc AS reset_at_utc,
                    last_checked_at_utc,
                    invalid_reason_detail,
                    last_error_detail,
                    source_file,
                    plan_type,
                    cpa_quota_status,
                    cpa_reset_at_utc,
                    cpa_synced_at_utc,
                    cpa_status,
                    cpa_status_message
                FROM effective_accounts
                {where_clause}
                ORDER BY
                    last_checked_at_utc DESC,
                    updated_at_utc DESC,
                    email ASC
                """,
                (*effective_params, *tuple(params)),
            ).fetchall()

        summary = {
            key: int(summary_row[key] or 0)
            for key in (
                "total",
                "active",
                "available",
                "exhausted",
                "unknown",
                "invalid",
                "source_missing",
            )
        }
        return summary, list(rows)

    @staticmethod
    def runtime_defaults() -> dict[str, Any]:
        now = utc_now_iso()
        return {
            "runtime_key": RUNTIME_KEY,
            "phase": COLLECTOR_PHASE_IDLE,
            "accounts_revision": 0,
            "runtime_revision": 0,
            "round_started_at_utc": None,
            "round_finished_at_utc": None,
            "next_round_at_utc": None,
            "total_scanned": 0,
            "total_candidates": 0,
            "processed_candidates": 0,
            "skipped_candidates": 0,
            "current_index": 0,
            "current_account_id": None,
            "current_account_email": None,
            "current_source_file": None,
            "manual_trigger_requested_at": None,
            "manual_stop_requested_at": None,
            "last_error_detail": None,
            "last_heartbeat_at_utc": now,
            "updated_at_utc": now,
        }

    def ensure_runtime_row(self) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT * FROM {RUNTIME_TABLE_NAME} WHERE runtime_key = ?",
                (RUNTIME_KEY,),
            ).fetchone()
            if row is None:
                payload = self.runtime_defaults()
                values = [payload.get(column) for column in RUNTIME_UPSERT_COLUMNS]
                placeholders = ", ".join("?" for _ in RUNTIME_UPSERT_COLUMNS)
                conn.execute(
                    f"""
                    INSERT INTO {RUNTIME_TABLE_NAME} ({', '.join(RUNTIME_UPSERT_COLUMNS)})
                    VALUES ({placeholders})
                    """,
                    values,
                )
                conn.commit()
                return payload
        runtime = self.fetch_runtime()
        return runtime or self.runtime_defaults()

    def _ensure_runtime_row_in_transaction(self, conn: sqlite3.Connection) -> sqlite3.Row:
        row = conn.execute(
            f"SELECT * FROM {RUNTIME_TABLE_NAME} WHERE runtime_key = ?",
            (RUNTIME_KEY,),
        ).fetchone()
        if row is not None:
            return row

        payload = self.runtime_defaults()
        values = [payload.get(column) for column in RUNTIME_UPSERT_COLUMNS]
        placeholders = ", ".join("?" for _ in RUNTIME_UPSERT_COLUMNS)
        conn.execute(
            f"""
            INSERT INTO {RUNTIME_TABLE_NAME} ({', '.join(RUNTIME_UPSERT_COLUMNS)})
            VALUES ({placeholders})
            """,
            values,
        )
        row = conn.execute(
            f"SELECT * FROM {RUNTIME_TABLE_NAME} WHERE runtime_key = ?",
            (RUNTIME_KEY,),
        ).fetchone()
        assert row is not None
        return row

    def request_manual_scan(self) -> dict[str, Any]:
        return self._request_control(
            request_column="manual_trigger_requested_at",
            allowed_phases=MANUAL_START_ALLOWED_PHASES,
        )

    def request_manual_stop(self) -> dict[str, Any]:
        return self._request_control(
            request_column="manual_stop_requested_at",
            allowed_phases=MANUAL_STOP_ALLOWED_PHASES,
        )

    def _request_control(
        self,
        *,
        request_column: str,
        allowed_phases: set[str],
    ) -> dict[str, Any]:
        requested_at = utc_now_iso()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self._ensure_runtime_row_in_transaction(conn)
            current_phase = str(row["phase"] or COLLECTOR_PHASE_IDLE)
            pending_requested_at = str(row[request_column] or "").strip()

            if pending_requested_at:
                conn.commit()
                return {
                    "accepted": True,
                    "status": "already_requested",
                    "phase": current_phase,
                    request_column: pending_requested_at,
                }

            if current_phase not in allowed_phases:
                conn.commit()
                return {
                    "accepted": False,
                    "status": "busy",
                    "phase": current_phase,
                    request_column: "",
                }

            conn.execute(
                f"""
                UPDATE {RUNTIME_TABLE_NAME}
                SET
                    {request_column} = ?,
                    runtime_revision = COALESCE(runtime_revision, 0) + 1,
                    updated_at_utc = ?
                WHERE runtime_key = ?
                """,
                (requested_at, requested_at, RUNTIME_KEY),
            )
            conn.commit()
            return {
                "accepted": True,
                "status": "accepted",
                "phase": current_phase,
                request_column: requested_at,
            }

    def consume_manual_scan_request(self) -> str | None:
        return self._consume_control_request("manual_trigger_requested_at")

    def consume_manual_stop_request(self) -> str | None:
        return self._consume_control_request("manual_stop_requested_at")

    def _consume_control_request(self, request_column: str) -> str | None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                f"SELECT {request_column} FROM {RUNTIME_TABLE_NAME} WHERE runtime_key = ?",
                (RUNTIME_KEY,),
            ).fetchone()
            if row is None:
                conn.commit()
                return None

            requested_at = str(row[request_column] or "").strip()
            if not requested_at:
                conn.commit()
                return None

            conn.execute(
                f"""
                UPDATE {RUNTIME_TABLE_NAME}
                SET
                    {request_column} = NULL,
                    runtime_revision = COALESCE(runtime_revision, 0) + 1,
                    updated_at_utc = ?
                WHERE runtime_key = ?
                """,
                (utc_now_iso(), RUNTIME_KEY),
            )
            conn.commit()
            return requested_at

    def fetch_change_state(self) -> dict[str, int]:
        with self._connect() as conn:
            row = self._ensure_runtime_row_in_transaction(conn)
            return {
                "accounts_revision": int(row["accounts_revision"] or 0),
                "runtime_revision": int(row["runtime_revision"] or 0),
            }

    @staticmethod
    def _bump_accounts_revision_in_transaction(conn: sqlite3.Connection) -> None:
        conn.execute(
            f"""
            UPDATE {RUNTIME_TABLE_NAME}
            SET
                accounts_revision = COALESCE(accounts_revision, 0) + 1,
                updated_at_utc = ?
            WHERE runtime_key = ?
            """,
            (utc_now_iso(), RUNTIME_KEY),
        )
