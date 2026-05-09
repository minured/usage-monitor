"""SQLite 持久化与运行时状态管理。"""

from __future__ import annotations

import sqlite3
import threading
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
from .timeutil import utc_now_iso


TABLE_NAME = "accounts_latest"
RUNTIME_TABLE_NAME = "collector_runtime"
RUNTIME_KEY = "collector"
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
                self._ensure_indexes(conn)
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
            """
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
            conn.commit()

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

    def fetch_dashboard(self, filter_name: str = "all") -> tuple[dict[str, int], list[sqlite3.Row]]:
        current_filter = filter_name if filter_name in FILTERS else "all"
        with self._connect() as conn:
            summary_row = conn.execute(
                f"""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN lifecycle_status = ? THEN 1 ELSE 0 END) AS active,
                    SUM(CASE WHEN lifecycle_status = ? AND quota_status = ? THEN 1 ELSE 0 END) AS available,
                    SUM(CASE WHEN lifecycle_status = ? AND quota_status = ? THEN 1 ELSE 0 END) AS exhausted,
                    SUM(CASE WHEN lifecycle_status = ? AND quota_status = ? THEN 1 ELSE 0 END) AS unknown,
                    SUM(CASE WHEN lifecycle_status = ? THEN 1 ELSE 0 END) AS invalid,
                    SUM(CASE WHEN lifecycle_status = ? THEN 1 ELSE 0 END) AS source_missing
                FROM {TABLE_NAME}
                """,
                (
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
                where_clause = "WHERE lifecycle_status = ? AND quota_status = ?"
                params = (LIFECYCLE_ACTIVE, current_filter)
            elif current_filter == LIFECYCLE_INVALID:
                where_clause = "WHERE lifecycle_status = ?"
                params = (LIFECYCLE_INVALID,)
            elif current_filter == LIFECYCLE_SOURCE_MISSING:
                where_clause = "WHERE lifecycle_status = ?"
                params = (LIFECYCLE_SOURCE_MISSING,)

            rows = conn.execute(
                f"""
                SELECT
                    dimension_key,
                    email,
                    lifecycle_status,
                    quota_status,
                    used_percent,
                    reset_at_utc,
                    last_checked_at_utc,
                    invalid_reason_detail,
                    last_error_detail,
                    source_file,
                    plan_type
                FROM {TABLE_NAME}
                {where_clause}
                ORDER BY
                    last_checked_at_utc DESC,
                    updated_at_utc DESC,
                    email ASC
                """,
                tuple(params),
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
