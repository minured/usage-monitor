"""usage-monitor 核心行为测试。"""

from __future__ import annotations

import base64
from dataclasses import replace
import gzip
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from usage_monitor.collector import CollectorService
from usage_monitor.cpa_status import parse_auth_file_status
from usage_monitor.config import DEFAULT_USER_AGENT, Settings
from usage_monitor.db import UsageDatabase
from usage_monitor.models import (
    COLLECTOR_PHASE_IDLE,
    COLLECTOR_PHASE_QUERYING,
    COLLECTOR_PHASE_SCANNING,
    COLLECTOR_PHASE_SLEEPING,
    LIFECYCLE_ACTIVE,
    LIFECYCLE_INVALID,
    LIFECYCLE_SOURCE_MISSING,
    QUOTA_AVAILABLE,
    QUOTA_EXHAUSTED,
    QUOTA_UNKNOWN,
    RefreshedTokens,
    build_dimension_key,
)
from usage_monitor.openai_api import HTTPStatusError, InvalidResponseError, TransportError
from usage_monitor.timeutil import format_shanghai
from usage_monitor.tokens import scan_tokens_dir
from usage_monitor.web import (
    _dashboard_patch_has_live_changes,
    build_dashboard_overview_payload,
    build_dashboard_patch_payload,
    build_dashboard_payload,
    build_progress_payload,
    create_app,
)


def make_usage_payload(*, allowed: bool = True, limit_reached: bool = False, used_percent: int = 1) -> dict:
    return {
        "plan_type": "free",
        "rate_limit": {
            "allowed": allowed,
            "limit_reached": limit_reached,
            "primary_window": {
                "used_percent": used_percent,
                "reset_at": 1774343228,
            },
        },
    }


def make_fake_jwt(
    *,
    account_id: str,
    plan_type: str,
    chatgpt_user_id: str | None = None,
    email: str = "demo@example.com",
) -> str:
    header = {"alg": "none", "typ": "JWT"}
    resolved_user_id = chatgpt_user_id or f"user-{account_id}"
    payload = {
        "https://api.openai.com/auth": {
            "chatgpt_account_id": account_id,
            "chatgpt_plan_type": plan_type,
            "chatgpt_user_id": resolved_user_id,
        },
        "https://api.openai.com/profile": {
            "email": email,
        },
    }

    def _encode_segment(value: dict[str, object]) -> str:
        raw = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

    return f"{_encode_segment(header)}.{_encode_segment(payload)}."


class FakeCPAStatusClient:
    def __init__(self, statuses):
        self.statuses = statuses
        self.calls = 0

    def fetch_auth_statuses(self):
        self.calls += 1
        return self.statuses


class ScriptedClient:
    def __init__(self, fetch_actions: dict[str, list], refresh_actions: dict[str, list] | None = None):
        self.fetch_actions = fetch_actions
        self.refresh_actions = refresh_actions or {}

    def fetch_usage(self, record):
        queue = self.fetch_actions.get(record.dimension_key)
        if queue is None:
            queue = self.fetch_actions.setdefault(record.account_id, [])
        if not queue:
            raise AssertionError(f"账号 {record.account_id} / {record.plan_type} 没有预设 fetch 动作")
        action = queue.pop(0)
        if isinstance(action, Exception):
            raise action
        if callable(action):
            return action(record)
        return action

    def refresh_tokens(self, record):
        queue = self.refresh_actions.get(record.dimension_key)
        if queue is None:
            queue = self.refresh_actions.setdefault(record.account_id, [])
        if not queue:
            raise AssertionError(f"账号 {record.account_id} / {record.plan_type} 没有预设 refresh 动作")
        action = queue.pop(0)
        if isinstance(action, Exception):
            raise action
        if callable(action):
            return action(record)
        return action


class UsageMonitorTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.tokens_dir = root / "tokens"
        self.tokens_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = root / "usage-monitor.sqlite3"
        self.auth_invalid_dir = root / "authInvalid"
        self.settings = Settings(
            project_root=root,
            subproject_root=root / "usage-monitor",
            tokens_dir=self.tokens_dir,
            db_path=self.db_path,
            auth_invalid_dir=self.auth_invalid_dir,
            url_prefix="",
            per_account_interval_seconds=0.0,
            round_interval_seconds=1.0,
            request_timeout_seconds=10,
            web_host="127.0.0.1",
            web_port=8765,
            log_level="INFO",
            user_agent=DEFAULT_USER_AGENT,
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write_token(
        self,
        file_name: str = "token0001_demo.json",
        *,
        account_id: str = "acct-1",
        email: str = "demo@example.com",
        plan_type: str = "free",
        access_token: str = "access-old",
        refresh_token: str = "refresh-old",
        id_token: str | None = None,
        chatgpt_user_id: str | None = None,
    ) -> Path:
        path = self.tokens_dir / file_name
        payload = {
            "account_id": account_id,
            "email": email,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expired": "2026-03-18T00:00:00Z",
            "last_refresh": "2026-03-18T00:00:00Z",
            "id_token": id_token
            or make_fake_jwt(
                account_id=account_id,
                plan_type=plan_type,
                chatgpt_user_id=chatgpt_user_id,
                email=email,
            ),
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return path

    def _build_service(self, client: ScriptedClient) -> CollectorService:
        return CollectorService(
            settings=self.settings,
            api_client=client,
            sleeper=lambda *_args, **_kwargs: None,
        )

    def _load_row(
        self,
        account_id: str = "acct-1",
        plan_type: str = "free",
        chatgpt_user_id: str | None = None,
    ) -> dict:
        database = UsageDatabase(self.settings.db_path)
        return database.fetch_accounts_index()[
            build_dimension_key(account_id, plan_type, chatgpt_user_id or f"user-{account_id}")
        ]

    def _account_payload(
        self,
        account_id: str,
        quota_status: str,
        *,
        checked_at: str,
        lifecycle_status: str = LIFECYCLE_ACTIVE,
        reset_at_utc: str | None = "2026-03-18T01:00:00Z",
    ) -> dict:
        return {
            "account_id": account_id,
            "email": f"{account_id}@example.com",
            "source_file": f"/tmp/{account_id}.json",
            "source_mtime_ns": 1,
            "lifecycle_status": lifecycle_status,
            "quota_status": quota_status,
            "plan_type": "team",
            "used_percent": 100.0 if quota_status == QUOTA_EXHAUSTED else 10.0,
            "rate_limit_allowed": 0 if quota_status == QUOTA_EXHAUSTED else 1,
            "rate_limit_reached": 1 if quota_status == QUOTA_EXHAUSTED else 0,
            "reset_at_utc": reset_at_utc,
            "last_checked_at_utc": checked_at,
            "last_success_at_utc": checked_at,
            "last_http_status": 200,
            "consecutive_403_count": 0,
            "invalid_reason_code": None,
            "invalid_reason_detail": None,
            "last_error_detail": None,
            "updated_at_utc": checked_at,
        }

    def test_success_updates_active_and_available(self) -> None:
        self._write_token()
        service = self._build_service(
            ScriptedClient(fetch_actions={"acct-1": [make_usage_payload(used_percent=12)]})
        )

        service.run_once()

        row = self._load_row()
        self.assertEqual(row["lifecycle_status"], LIFECYCLE_ACTIVE)
        self.assertEqual(row["quota_status"], QUOTA_AVAILABLE)
        self.assertEqual(row["last_http_status"], 200)
        self.assertEqual(row["used_percent"], 12.0)
        self.assertTrue(row["last_checked_at_utc"])
        self.assertEqual(row["last_checked_at_utc"], row["last_success_at_utc"])

    def test_401_refresh_success_rewrites_token_file(self) -> None:
        token_path = self._write_token()
        service = self._build_service(
            ScriptedClient(
                fetch_actions={
                    "acct-1": [
                        HTTPStatusError(401, "Unauthorized", {"error": "expired"}),
                        make_usage_payload(used_percent=7),
                    ]
                },
                refresh_actions={
                    "acct-1": [
                        RefreshedTokens(
                            access_token="access-new",
                            refresh_token="refresh-new",
                            last_refresh="2026-03-18T08:00:00Z",
                            expired="2026-03-18T16:00:00Z",
                            id_token=make_fake_jwt(
                                account_id="acct-1",
                                plan_type="free",
                                chatgpt_user_id="user-acct-1",
                                email="demo@example.com",
                            ),
                        )
                    ]
                },
            )
        )

        service.run_once()

        row = self._load_row()
        self.assertEqual(row["lifecycle_status"], LIFECYCLE_ACTIVE)
        self.assertEqual(row["quota_status"], QUOTA_AVAILABLE)

        payload = json.loads(token_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["access_token"], "access-new")
        self.assertEqual(payload["refresh_token"], "refresh-new")
        self.assertTrue(payload["id_token"].startswith("ey"))

    def test_first_401_refresh_failure_marks_invalid_without_moving(self) -> None:
        self._write_token()
        service = self._build_service(
            ScriptedClient(
                fetch_actions={"acct-1": [HTTPStatusError(401, "Unauthorized", {"error": "expired"})]},
                refresh_actions={"acct-1": [InvalidResponseError("refresh 失败")]},
            )
        )

        service.run_once()

        row = self._load_row()
        self.assertEqual(row["lifecycle_status"], LIFECYCLE_INVALID)
        self.assertEqual(row["invalid_reason_code"], "refresh_failed")
        self.assertEqual(row["consecutive_401_count"], 1)
        self.assertEqual(row["last_http_status"], 401)
        self.assertTrue((self.tokens_dir / "token0001_demo.json").exists())
        self.assertFalse(any(self.auth_invalid_dir.glob("*.json")))

    def test_second_consecutive_401_moves_file_without_changing_invalid_reason(self) -> None:
        token_path = self._write_token()
        service = self._build_service(
            ScriptedClient(
                fetch_actions={
                    "acct-1": [
                        HTTPStatusError(401, "Unauthorized", {"error": "expired"}),
                        HTTPStatusError(401, "Unauthorized", {"error": "expired"}),
                    ]
                },
                refresh_actions={
                    "acct-1": [
                        InvalidResponseError("refresh 失败"),
                        InvalidResponseError("refresh 失败"),
                    ]
                },
            )
        )

        service.run_once()
        service.run_once()

        row = self._load_row()
        moved_files = sorted(self.auth_invalid_dir.glob("*.json"))
        self.assertEqual(row["lifecycle_status"], LIFECYCLE_INVALID)
        self.assertEqual(row["invalid_reason_code"], "refresh_failed")
        self.assertEqual(row["consecutive_401_count"], 2)
        self.assertEqual(row["last_http_status"], 401)
        self.assertIn("refresh 失败", row["invalid_reason_detail"])
        self.assertFalse(token_path.exists())
        self.assertEqual(len(moved_files), 1)
        self.assertEqual(Path(row["source_file"]).resolve(), moved_files[0].resolve())

    def test_moved_invalid_file_does_not_become_source_missing(self) -> None:
        self._write_token()
        service = self._build_service(
            ScriptedClient(
                fetch_actions={
                    "acct-1": [
                        HTTPStatusError(401, "Unauthorized", {"error": "expired"}),
                        HTTPStatusError(401, "Unauthorized", {"error": "expired"}),
                    ]
                },
                refresh_actions={
                    "acct-1": [
                        InvalidResponseError("refresh 失败"),
                        InvalidResponseError("refresh 失败"),
                    ]
                },
            )
        )

        service.run_once()
        service.run_once()

        followup_service = self._build_service(ScriptedClient(fetch_actions={}))
        followup_service.run_once()

        row = self._load_row()
        self.assertEqual(row["lifecycle_status"], LIFECYCLE_INVALID)
        self.assertEqual(row["invalid_reason_code"], "refresh_failed")

    def test_403_twice_marks_invalid(self) -> None:
        self._write_token()
        service = self._build_service(
            ScriptedClient(
                fetch_actions={
                    "acct-1": [
                        HTTPStatusError(403, "Forbidden", {"error": "blocked"}),
                        HTTPStatusError(403, "Forbidden", {"error": "blocked"}),
                    ]
                }
            )
        )

        service.run_once()
        first_row = self._load_row()
        self.assertEqual(first_row["lifecycle_status"], LIFECYCLE_ACTIVE)
        self.assertEqual(first_row["quota_status"], QUOTA_UNKNOWN)
        self.assertEqual(first_row["consecutive_403_count"], 1)

        service.run_once()
        second_row = self._load_row()
        self.assertEqual(second_row["lifecycle_status"], LIFECYCLE_INVALID)
        self.assertEqual(second_row["invalid_reason_code"], "forbidden_twice")
        self.assertEqual(second_row["consecutive_403_count"], 2)

    def test_transient_errors_become_unknown(self) -> None:
        self._write_token()
        service = self._build_service(
            ScriptedClient(
                fetch_actions={
                    "acct-1": [
                        HTTPStatusError(429, "Too Many Requests", {"error": "slow down"}),
                    ]
                }
            )
        )

        service.run_once()
        row = self._load_row()
        self.assertEqual(row["lifecycle_status"], LIFECYCLE_ACTIVE)
        self.assertEqual(row["quota_status"], QUOTA_UNKNOWN)
        self.assertEqual(row["last_http_status"], 429)

    def test_transport_error_becomes_unknown(self) -> None:
        self._write_token()
        service = self._build_service(
            ScriptedClient(fetch_actions={"acct-1": [TransportError("timeout")]})
        )

        service.run_once()
        row = self._load_row()
        self.assertEqual(row["lifecycle_status"], LIFECYCLE_ACTIVE)
        self.assertEqual(row["quota_status"], QUOTA_UNKNOWN)
        self.assertIn("timeout", row["last_error_detail"])

    def test_source_missing_is_marked(self) -> None:
        token_path = self._write_token()
        service = self._build_service(
            ScriptedClient(fetch_actions={"acct-1": [make_usage_payload()]})
        )

        service.run_once()
        token_path.unlink()

        service = self._build_service(ScriptedClient(fetch_actions={}))
        service.run_once()

        row = self._load_row()
        self.assertEqual(row["lifecycle_status"], LIFECYCLE_SOURCE_MISSING)
        self.assertEqual(row["invalid_reason_code"], "source_missing")

    def test_dimension_changed_old_unknown_row_is_auto_cleaned(self) -> None:
        token_path = self._write_token()
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        database.upsert_account(
            {
                "dimension_key": build_dimension_key("acct-1", "free", "unknown"),
                "account_id": "acct-1",
                "email": "demo@example.com",
                "source_file": str(token_path.resolve()),
                "source_mtime_ns": token_path.stat().st_mtime_ns,
                "lifecycle_status": LIFECYCLE_SOURCE_MISSING,
                "quota_status": QUOTA_UNKNOWN,
                "plan_type": "free",
                "chatgpt_user_id": "unknown",
                "used_percent": None,
                "rate_limit_allowed": None,
                "rate_limit_reached": None,
                "reset_at_utc": None,
                "last_checked_at_utc": "2026-03-18T00:00:00Z",
                "last_success_at_utc": None,
                "last_http_status": None,
                "consecutive_403_count": 0,
                "consecutive_401_count": 0,
                "invalid_reason_code": "source_missing",
                "invalid_reason_detail": "源文件维度已变化",
                "last_error_detail": "源文件维度已变化",
                "updated_at_utc": "2026-03-18T00:00:00Z",
            }
        )

        service = self._build_service(
            ScriptedClient(fetch_actions={"acct-1": [make_usage_payload()]})
        )
        service.run_once()

        rows = database.fetch_accounts_index()
        self.assertIn(build_dimension_key("acct-1", "free", "user-acct-1"), rows)
        self.assertNotIn(build_dimension_key("acct-1", "free", "unknown"), rows)
        self.assertEqual(len(rows), 1)

    def test_scan_tokens_dir_keeps_same_account_different_plan_types(self) -> None:
        self._write_token(
            file_name="token_team.json",
            account_id="acct-shared",
            email="team@example.com",
            plan_type="team",
        )
        self._write_token(
            file_name="token_free.json",
            account_id="acct-shared",
            email="free@example.com",
            plan_type="free",
        )

        records, warnings = scan_tokens_dir(self.tokens_dir)

        self.assertEqual(len(records), 2)
        self.assertFalse(warnings)
        self.assertEqual(
            sorted((record.account_id, record.plan_type) for record in records),
            [("acct-shared", "free"), ("acct-shared", "team")],
        )

    def test_scan_tokens_dir_keeps_same_account_same_plan_different_user_ids(self) -> None:
        self._write_token(
            file_name="token_team_user_1.json",
            account_id="acct-shared",
            email="team-1@example.com",
            plan_type="team",
            chatgpt_user_id="user-team-1",
        )
        self._write_token(
            file_name="token_team_user_2.json",
            account_id="acct-shared",
            email="team-2@example.com",
            plan_type="team",
            chatgpt_user_id="user-team-2",
        )

        records, warnings = scan_tokens_dir(self.tokens_dir)

        self.assertEqual(len(records), 2)
        self.assertFalse(warnings)
        self.assertEqual(
            sorted((record.account_id, record.plan_type, record.chatgpt_user_id) for record in records),
            [
                ("acct-shared", "team", "user-team-1"),
                ("acct-shared", "team", "user-team-2"),
            ],
        )

    def test_dashboard_summary_and_filter(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        rows = [
            {
                "account_id": "a1",
                "email": "a1@example.com",
                "source_file": "/tmp/a1.json",
                "source_mtime_ns": 1,
                "lifecycle_status": LIFECYCLE_ACTIVE,
                "quota_status": QUOTA_AVAILABLE,
                "plan_type": "free",
                "used_percent": 10.0,
                "rate_limit_allowed": 1,
                "rate_limit_reached": 0,
                "reset_at_utc": "2026-03-18T00:00:00Z",
                "last_checked_at_utc": "2026-03-18T00:00:00Z",
                "last_success_at_utc": "2026-03-18T00:00:00Z",
                "last_http_status": 200,
                "consecutive_403_count": 0,
                "invalid_reason_code": None,
                "invalid_reason_detail": None,
                "last_error_detail": None,
                "updated_at_utc": "2026-03-18T00:00:00Z",
            },
            {
                "account_id": "a2",
                "email": "a2@example.com",
                "source_file": "/tmp/a2.json",
                "source_mtime_ns": 1,
                "lifecycle_status": LIFECYCLE_ACTIVE,
                "quota_status": QUOTA_EXHAUSTED,
                "plan_type": "free",
                "used_percent": 100.0,
                "rate_limit_allowed": 0,
                "rate_limit_reached": 1,
                "reset_at_utc": "2026-03-18T00:00:00Z",
                "last_checked_at_utc": "2026-03-18T00:01:00Z",
                "last_success_at_utc": "2026-03-18T00:01:00Z",
                "last_http_status": 200,
                "consecutive_403_count": 0,
                "invalid_reason_code": None,
                "invalid_reason_detail": None,
                "last_error_detail": None,
                "updated_at_utc": "2026-03-18T00:01:00Z",
            },
            {
                "account_id": "a3",
                "email": "a3@example.com",
                "source_file": "/tmp/a3.json",
                "source_mtime_ns": 1,
                "lifecycle_status": LIFECYCLE_INVALID,
                "quota_status": QUOTA_UNKNOWN,
                "plan_type": "free",
                "used_percent": None,
                "rate_limit_allowed": None,
                "rate_limit_reached": None,
                "reset_at_utc": None,
                "last_checked_at_utc": "2026-03-18T00:02:00Z",
                "last_success_at_utc": None,
                "last_http_status": 401,
                "consecutive_403_count": 0,
                "invalid_reason_code": "refresh_failed",
                "invalid_reason_detail": "refresh 失败",
                "last_error_detail": "refresh 失败",
                "updated_at_utc": "2026-03-18T00:02:00Z",
            },
            {
                "account_id": "a4",
                "email": "a4@example.com",
                "source_file": "/tmp/a4.json",
                "source_mtime_ns": 1,
                "lifecycle_status": LIFECYCLE_SOURCE_MISSING,
                "quota_status": QUOTA_UNKNOWN,
                "plan_type": "free",
                "used_percent": None,
                "rate_limit_allowed": None,
                "rate_limit_reached": None,
                "reset_at_utc": None,
                "last_checked_at_utc": "2026-03-18T00:03:00Z",
                "last_success_at_utc": None,
                "last_http_status": None,
                "consecutive_403_count": 0,
                "invalid_reason_code": "source_missing",
                "invalid_reason_detail": "源文件缺失",
                "last_error_detail": "源文件缺失",
                "updated_at_utc": "2026-03-18T00:03:00Z",
            },
            {
                "account_id": "a5",
                "email": "a5@example.com",
                "source_file": "/tmp/a5.json",
                "source_mtime_ns": 1,
                "lifecycle_status": LIFECYCLE_ACTIVE,
                "quota_status": QUOTA_UNKNOWN,
                "plan_type": "free",
                "used_percent": None,
                "rate_limit_allowed": None,
                "rate_limit_reached": None,
                "reset_at_utc": None,
                "last_checked_at_utc": "2026-03-18T00:04:00Z",
                "last_success_at_utc": None,
                "last_http_status": 429,
                "consecutive_403_count": 0,
                "invalid_reason_code": None,
                "invalid_reason_detail": None,
                "last_error_detail": "429",
                "updated_at_utc": "2026-03-18T00:04:00Z",
            },
        ]
        for row in rows:
            database.upsert_account(row)

        payload = build_dashboard_payload(self.settings, "active")
        self.assertEqual(payload["summary"]["total"], 5)
        self.assertEqual(payload["summary"]["active"], 3)
        self.assertEqual(payload["summary"]["available"], 1)
        self.assertEqual(payload["summary"]["exhausted"], 1)
        self.assertEqual(payload["summary"]["unknown"], 1)
        self.assertEqual(payload["summary"]["invalid"], 1)
        self.assertEqual(payload["summary"]["source_missing"], 1)
        self.assertGreaterEqual(payload["accounts_revision"], 1)
        self.assertEqual(len(payload["items"]), 3)

        payload_by_email = {item["email"]: item for item in payload["items"]}
        self.assertEqual(payload_by_email["a1@example.com"]["remaining_percent_text"], "90%")
        self.assertEqual(payload_by_email["a2@example.com"]["remaining_percent_text"], "0%")
        self.assertEqual(payload_by_email["a5@example.com"]["remaining_percent_text"], "-")
        self.assertNotIn("account_id", payload["items"][0])
        self.assertNotIn("chatgpt_user_id", payload["items"][0])
        self.assertNotIn("reset_at_shanghai", payload["items"][0])
        self.assertNotIn("last_checked_at_shanghai", payload["items"][0])
        self.assertNotIn("generated_at_shanghai", payload)

        available_payload = build_dashboard_payload(self.settings, "available")
        self.assertEqual([item["email"] for item in available_payload["items"]], ["a1@example.com"])

        exhausted_payload = build_dashboard_payload(self.settings, "exhausted")
        self.assertEqual([item["email"] for item in exhausted_payload["items"]], ["a2@example.com"])

        unknown_payload = build_dashboard_payload(self.settings, "unknown")
        self.assertEqual([item["email"] for item in unknown_payload["items"]], ["a5@example.com"])

        invalid_payload = build_dashboard_payload(self.settings, "invalid")
        self.assertEqual([item["email"] for item in invalid_payload["items"]], ["a3@example.com"])

        source_missing_payload = build_dashboard_payload(self.settings, "source_missing")
        self.assertEqual([item["email"] for item in source_missing_payload["items"]], ["a4@example.com"])

    def test_exhausted_history_records_hourly_sliding_window(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        empty_history = database.fetch_exhausted_history(hours=4, now_utc="2026-03-18T03:45:00Z")
        self.assertEqual(len(empty_history), 4)
        self.assertEqual(
            [point["captured_at_utc"] for point in empty_history],
            [
                "2026-03-18T00:00:00Z",
                "2026-03-18T01:00:00Z",
                "2026-03-18T02:00:00Z",
                "2026-03-18T03:00:00Z",
            ],
        )
        self.assertEqual([point["exhausted"] for point in empty_history], [0, 0, 0, 0])

        database.upsert_account(
            self._account_payload(
                "hist-1",
                QUOTA_AVAILABLE,
                checked_at="2026-03-18T00:00:00Z",
            )
        )
        with sqlite3.connect(self.settings.db_path) as conn:
            conn.execute(
                "UPDATE summary_history SET captured_at_utc = ? WHERE id = (SELECT MAX(id) FROM summary_history)",
                ("2026-03-18T00:10:00Z",),
            )
        first_history = database.fetch_exhausted_history(hours=4, now_utc="2026-03-18T03:45:00Z")
        self.assertEqual([point["exhausted"] for point in first_history], [0, 0, 0, 0])

        database.upsert_account(
            self._account_payload(
                "hist-1",
                QUOTA_EXHAUSTED,
                checked_at="2026-03-18T00:05:00Z",
            )
        )
        with sqlite3.connect(self.settings.db_path) as conn:
            conn.execute(
                "UPDATE summary_history SET captured_at_utc = ? WHERE id = (SELECT MAX(id) FROM summary_history)",
                ("2026-03-18T02:20:00Z",),
            )
        second_history = database.fetch_exhausted_history(hours=4, now_utc="2026-03-18T03:45:00Z")
        self.assertEqual([point["exhausted"] for point in second_history], [0, 0, 1, 1])

        database.upsert_account(
            self._account_payload(
                "hist-1",
                QUOTA_EXHAUSTED,
                checked_at="2026-03-18T00:10:00Z",
            )
        )
        self.assertEqual(
            database.fetch_exhausted_history(hours=4, now_utc="2026-03-18T03:45:00Z"),
            second_history,
        )

        payload = build_dashboard_payload(self.settings, "all", database)
        overview_payload = build_dashboard_overview_payload(self.settings, "all", database)
        self.assertEqual(len(payload["exhausted_history"]), 168)
        self.assertEqual(payload["exhausted_history"][-1]["exhausted"], 0)
        self.assertEqual(len(payload["exhausted_recovery"]), 169)
        self.assertEqual(overview_payload["exhausted_history"], payload["exhausted_history"])
        self.assertEqual(overview_payload["exhausted_recovery"], payload["exhausted_recovery"])

    def test_exhausted_recovery_projection_uses_current_window_only(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        for account_id, reset_at in (
            ("recover-soon", "2026-03-18T04:00:00Z"),
            ("recover-later", "2026-03-18T05:45:00Z"),
            ("recover-outside", "2026-03-18T08:00:00Z"),
            ("recover-unknown", None),
        ):
            database.upsert_account(
                self._account_payload(
                    account_id,
                    QUOTA_EXHAUSTED,
                    checked_at="2026-03-18T03:30:00Z",
                    reset_at_utc=reset_at,
                )
            )
        database.upsert_account(
            self._account_payload(
                "available-ignored",
                QUOTA_AVAILABLE,
                checked_at="2026-03-18T03:30:00Z",
                reset_at_utc="2026-03-18T04:00:00Z",
            )
        )

        projection = database.fetch_exhausted_recovery_projection(
            hours=3,
            now_utc="2026-03-18T03:45:00Z",
        )

        self.assertEqual(
            [point["projected_at_utc"] for point in projection],
            [
                "2026-03-18T03:45:00Z",
                "2026-03-18T04:45:00Z",
                "2026-03-18T05:45:00Z",
                "2026-03-18T06:45:00Z",
            ],
        )
        self.assertEqual([point["exhausted"] for point in projection], [4, 3, 2, 2])
        self.assertEqual([point["recovered"] for point in projection], [0, 1, 2, 2])

    def test_cpa_auth_file_status_parser_handles_usage_limit(self) -> None:
        status = parse_auth_file_status(
            {
                "name": "codex-two@example.com-free.json",
                "email": "two@example.com",
                "status": "error",
                "status_message": json.dumps(
                    {
                        "error": {
                            "type": "usage_limit_reached",
                            "message": "The usage limit has been reached",
                        }
                    }
                ),
                "next_retry_after": "2026-05-20T13:19:21.000018342+08:00",
            }
        )

        self.assertEqual(status.source_file_name, "codex-two@example.com-free.json")
        self.assertEqual(status.email, "two@example.com")
        self.assertEqual(status.quota_status, QUOTA_EXHAUSTED)
        self.assertEqual(status.reset_at_utc, "2026-05-20T05:19:21Z")
        self.assertIn("usage limit", status.cpa_status_message.lower())

    def test_cpa_auth_file_status_parser_uses_resets_at_from_status_message(self) -> None:
        status = parse_auth_file_status(
            {
                "name": "codex-reset@example.com-free.json",
                "email": "reset@example.com",
                "status": "error",
                "status_message": json.dumps(
                    {
                        "error": {
                            "type": "usage_limit_reached",
                            "message": "The usage limit has been reached",
                            "resets_at": 1893456000,
                        }
                    }
                ),
            }
        )

        self.assertEqual(status.quota_status, QUOTA_EXHAUSTED)
        self.assertEqual(status.reset_at_utc, "2030-01-01T00:00:00Z")

    def test_cpa_status_overlay_updates_dashboard_summary(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        database.upsert_account(
            self._account_payload(
                "a1",
                QUOTA_AVAILABLE,
                checked_at="2026-03-18T00:00:00Z",
            )
        )
        database.upsert_account(
            self._account_payload(
                "a2",
                QUOTA_AVAILABLE,
                checked_at="2026-03-18T00:01:00Z",
            )
        )

        result = database.sync_cpa_statuses(
            [
                {
                    "source_file_name": "a1.json",
                    "email": "a1@example.com",
                    "quota_status": QUOTA_AVAILABLE,
                    "reset_at_utc": None,
                    "cpa_status": "active",
                    "cpa_status_message": "",
                },
                {
                    "source_file_name": "a2.json",
                    "email": "a2@example.com",
                    "quota_status": QUOTA_EXHAUSTED,
                    # 使用遥远的未来时间，确保测试不依赖运行当天日期。
                    "reset_at_utc": "9999-12-31T23:59:59Z",
                    "cpa_status": "error",
                    "cpa_status_message": "The usage limit has been reached",
                },
            ],
            cpa_stale_seconds=120,
        )
        self.assertEqual(result["matched"], 2)

        cpa_settings = replace(self.settings, cpa_status_enabled=True, cpa_status_stale_seconds=120)
        payload = build_dashboard_payload(cpa_settings, "exhausted", database)

        self.assertEqual(payload["summary"]["available"], 1)
        self.assertEqual(payload["summary"]["exhausted"], 1)
        self.assertEqual([item["email"] for item in payload["items"]], ["a2@example.com"])
        self.assertEqual(payload["items"][0]["quota_status"], QUOTA_EXHAUSTED)
        self.assertEqual(payload["items"][0]["remaining_percent_text"], "0%")
        self.assertEqual(payload["items"][0]["reset_at_utc"], "9999-12-31T23:59:59Z")

        official_payload = build_dashboard_payload(self.settings, "exhausted", database)
        self.assertEqual(official_payload["summary"]["exhausted"], 0)
        self.assertEqual(official_payload["items"], [])

    def test_cpa_active_does_not_unexhaust_account_before_official_reset(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        exhausted_payload = self._account_payload(
            "a1",
            QUOTA_EXHAUSTED,
            checked_at="2026-03-18T00:00:00Z",
        )
        # 使用遥远的未来时间，确保测试不依赖运行当天日期。
        exhausted_payload["reset_at_utc"] = "9999-12-31T23:59:59Z"
        database.upsert_account(exhausted_payload)
        database.upsert_account(
            self._account_payload(
                "a2",
                QUOTA_AVAILABLE,
                checked_at="2026-03-18T00:01:00Z",
            )
        )

        database.sync_cpa_statuses(
            [
                {
                    "source_file_name": "a1.json",
                    "email": "a1@example.com",
                    "quota_status": QUOTA_AVAILABLE,
                    "reset_at_utc": None,
                    "cpa_status": "active",
                    "cpa_status_message": "",
                },
                {
                    "source_file_name": "a2.json",
                    "email": "a2@example.com",
                    "quota_status": QUOTA_AVAILABLE,
                    "reset_at_utc": None,
                    "cpa_status": "active",
                    "cpa_status_message": "",
                },
            ],
            cpa_stale_seconds=120,
        )

        cpa_settings = replace(self.settings, cpa_status_enabled=True, cpa_status_stale_seconds=120)
        payload = build_dashboard_payload(cpa_settings, "exhausted", database)

        self.assertEqual(payload["summary"]["available"], 1)
        self.assertEqual(payload["summary"]["exhausted"], 1)
        self.assertEqual([item["email"] for item in payload["items"]], ["a1@example.com"])
        self.assertEqual(payload["items"][0]["quota_status"], QUOTA_EXHAUSTED)
        self.assertEqual(payload["items"][0]["reset_at_utc"], "9999-12-31T23:59:59Z")

    def test_cpa_origin_exhausted_survives_later_active_before_reset(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        database.upsert_account(
            self._account_payload(
                "a1",
                QUOTA_AVAILABLE,
                checked_at="2026-03-18T00:00:00Z",
            )
        )

        database.sync_cpa_statuses(
            [
                {
                    "source_file_name": "a1.json",
                    "email": "a1@example.com",
                    "quota_status": QUOTA_EXHAUSTED,
                    "reset_at_utc": "9999-12-31T23:59:59Z",
                    "cpa_status": "error",
                    "cpa_status_message": "The usage limit has been reached",
                }
            ],
            cpa_stale_seconds=120,
        )
        database.sync_cpa_statuses(
            [
                {
                    "source_file_name": "a1.json",
                    "email": "a1@example.com",
                    "quota_status": QUOTA_AVAILABLE,
                    "reset_at_utc": None,
                    "cpa_status": "active",
                    "cpa_status_message": "",
                }
            ],
            cpa_stale_seconds=120,
        )

        cpa_settings = replace(self.settings, cpa_status_enabled=True, cpa_status_stale_seconds=120)
        payload = build_dashboard_payload(cpa_settings, "exhausted", database)

        self.assertEqual(payload["summary"]["available"], 0)
        self.assertEqual(payload["summary"]["exhausted"], 1)
        self.assertEqual(payload["items"][0]["quota_status"], QUOTA_EXHAUSTED)
        self.assertEqual(payload["items"][0]["reset_at_utc"], "9999-12-31T23:59:59Z")

    def test_stale_cpa_origin_exhausted_stays_exhausted_before_reset(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        database.upsert_account(
            self._account_payload(
                "a1",
                QUOTA_AVAILABLE,
                checked_at="2026-03-18T00:00:00Z",
            )
        )
        database.sync_cpa_statuses(
            [
                {
                    "source_file_name": "a1.json",
                    "email": "a1@example.com",
                    "quota_status": QUOTA_EXHAUSTED,
                    "reset_at_utc": "9999-12-31T23:59:59Z",
                    "cpa_status": "error",
                    "cpa_status_message": "The usage limit has been reached",
                }
            ],
            cpa_stale_seconds=120,
        )
        with sqlite3.connect(self.settings.db_path) as conn:
            conn.execute(
                "UPDATE accounts_latest SET cpa_synced_at_utc = ?",
                ("2000-01-01T00:00:00Z",),
            )
            conn.commit()

        cpa_settings = replace(self.settings, cpa_status_enabled=True, cpa_status_stale_seconds=1)
        payload = build_dashboard_payload(cpa_settings, "exhausted", database)

        self.assertEqual(payload["summary"]["available"], 0)
        self.assertEqual(payload["summary"]["exhausted"], 1)
        self.assertEqual(payload["items"][0]["reset_at_utc"], "9999-12-31T23:59:59Z")

    def test_expired_cpa_exhausted_does_not_override_official_available(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        database.upsert_account(
            self._account_payload(
                "a1",
                QUOTA_AVAILABLE,
                checked_at="2026-03-18T00:00:00Z",
            )
        )

        database.sync_cpa_statuses(
            [
                {
                    "source_file_name": "a1.json",
                    "email": "a1@example.com",
                    "quota_status": QUOTA_EXHAUSTED,
                    "reset_at_utc": "2000-01-01T00:00:00Z",
                    "cpa_status": "error",
                    "cpa_status_message": "The usage limit has been reached",
                }
            ],
            cpa_stale_seconds=120,
        )

        cpa_settings = replace(self.settings, cpa_status_enabled=True, cpa_status_stale_seconds=120)
        payload = build_dashboard_payload(cpa_settings, "all", database)
        exhausted_payload = build_dashboard_payload(cpa_settings, "exhausted", database)

        self.assertEqual(payload["summary"]["available"], 1)
        self.assertEqual(payload["summary"]["exhausted"], 0)
        self.assertEqual(exhausted_payload["items"], [])
        self.assertEqual(payload["items"][0]["quota_status"], QUOTA_AVAILABLE)
        self.assertEqual(payload["items"][0]["remaining_percent_text"], "90%")
        self.assertEqual(payload["items"][0]["reset_at_utc"], "2026-03-18T01:00:00Z")

    def test_dashboard_overview_payload_omits_items(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        database.upsert_account(
            {
                "account_id": "acct-1",
                "email": "one@example.com",
                "source_file": "/tmp/one.json",
                "source_mtime_ns": 1,
                "lifecycle_status": LIFECYCLE_ACTIVE,
                "quota_status": QUOTA_AVAILABLE,
                "plan_type": "team",
                "used_percent": 10.0,
                "rate_limit_allowed": 1,
                "rate_limit_reached": 0,
                "reset_at_utc": "2026-03-18T00:00:00Z",
                "last_checked_at_utc": "2026-03-18T00:00:00Z",
                "last_success_at_utc": "2026-03-18T00:00:00Z",
                "last_http_status": 200,
                "consecutive_403_count": 0,
                "invalid_reason_code": None,
                "invalid_reason_detail": None,
                "last_error_detail": None,
                "updated_at_utc": "2026-03-18T00:00:00Z",
            }
        )

        payload = build_dashboard_overview_payload(self.settings, "all")
        self.assertGreaterEqual(payload["accounts_revision"], 1)
        self.assertEqual(payload["filter"], "all")
        self.assertEqual(payload["summary"]["total"], 1)
        self.assertNotIn("items", payload)

    def test_dashboard_patch_payload_tracks_upserts_and_removals(self) -> None:
        previous_payload = {
            "generated_at": "2026-03-18T00:00:00Z",
            "accounts_revision": 7,
            "filter": "active",
            "summary": {"total": 2, "active": 1, "available": 1, "exhausted": 0, "unknown": 0, "invalid": 1, "source_missing": 0},
            "items": [
                {
                    "dimension_key": "acct-1::team::user-1",
                    "email": "one@example.com",
                    "lifecycle_status": "active",
                    "quota_status": "available",
                    "remaining_percent_text": "90%",
                    "remaining_percent_value": 90.0,
                    "reset_at_utc": "2026-03-18T00:00:00Z",
                    "last_checked_at_utc": "2026-03-18T00:00:00Z",
                    "note": "-",
                    "source_file": "/tmp/one.json",
                    "source_file_name": "one.json",
                    "plan_type": "team",
                }
            ],
        }
        current_payload = {
            "generated_at": "2026-03-18T00:05:00Z",
            "accounts_revision": 8,
            "filter": "active",
            "summary": {"total": 2, "active": 1, "available": 0, "exhausted": 1, "unknown": 0, "invalid": 1, "source_missing": 0},
            "exhausted_history": [{"captured_at_utc": "2026-03-18T00:05:00Z", "exhausted": 1}],
            "items": [
                {
                    "dimension_key": "acct-2::team::user-2",
                    "email": "two@example.com",
                    "lifecycle_status": "active",
                    "quota_status": "exhausted",
                    "remaining_percent_text": "0%",
                    "remaining_percent_value": 0.0,
                    "reset_at_utc": "2026-03-18T00:05:00Z",
                    "last_checked_at_utc": "2026-03-18T00:05:00Z",
                    "note": "-",
                    "source_file": "/tmp/two.json",
                    "source_file_name": "two.json",
                    "plan_type": "team",
                }
            ],
        }

        patch_payload = build_dashboard_patch_payload(previous_payload, current_payload)
        self.assertEqual(patch_payload["filter"], "active")
        self.assertEqual(patch_payload["generated_at"], "2026-03-18T00:05:00Z")
        self.assertEqual(patch_payload["accounts_revision"], 8)
        self.assertEqual(patch_payload["exhausted_history"], [{"captured_at_utc": "2026-03-18T00:05:00Z", "exhausted": 1}])
        self.assertEqual(patch_payload["removed_dimension_keys"], ["acct-1::team::user-1"])
        self.assertEqual([item["dimension_key"] for item in patch_payload["upserted_items"]], ["acct-2::team::user-2"])
        self.assertTrue(_dashboard_patch_has_live_changes(patch_payload, previous_payload))

    def test_dashboard_patch_live_changes_include_chart_windows(self) -> None:
        previous_payload = {
            "summary": {"total": 1, "active": 1, "available": 0, "exhausted": 1, "unknown": 0, "invalid": 0, "source_missing": 0},
            "items": [],
            "exhausted_history": [{"captured_at_utc": "2026-03-18T00:00:00Z", "exhausted": 1}],
            "exhausted_recovery": [{"projected_at_utc": "2026-03-18T00:00:00Z", "exhausted": 1, "recovered": 0}],
        }
        same_payload = {
            **previous_payload,
            "generated_at": "2026-03-18T00:01:00Z",
            "accounts_revision": 1,
            "filter": "all",
        }
        same_patch = build_dashboard_patch_payload(previous_payload, same_payload)
        self.assertFalse(_dashboard_patch_has_live_changes(same_patch, previous_payload))

        moved_payload = {
            **same_payload,
            "exhausted_recovery": [{"projected_at_utc": "2026-03-18T00:01:00Z", "exhausted": 1, "recovered": 0}],
        }
        moved_patch = build_dashboard_patch_payload(previous_payload, moved_payload)
        self.assertTrue(_dashboard_patch_has_live_changes(moved_patch, previous_payload))

    def test_dashboard_api_cache_refreshes_when_accounts_revision_changes(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        database.upsert_account(
            {
                "account_id": "acct-1",
                "email": "one@example.com",
                "source_file": "/tmp/one.json",
                "source_mtime_ns": 1,
                "lifecycle_status": LIFECYCLE_ACTIVE,
                "quota_status": QUOTA_AVAILABLE,
                "plan_type": "team",
                "used_percent": 10.0,
                "rate_limit_allowed": 1,
                "rate_limit_reached": 0,
                "reset_at_utc": "2026-03-18T00:00:00Z",
                "last_checked_at_utc": "2026-03-18T00:00:00Z",
                "last_success_at_utc": "2026-03-18T00:00:00Z",
                "last_http_status": 200,
                "consecutive_403_count": 0,
                "invalid_reason_code": None,
                "invalid_reason_detail": None,
                "last_error_detail": None,
                "updated_at_utc": "2026-03-18T00:00:00Z",
            }
        )
        app = create_app(self.settings)

        def request_dashboard() -> dict[str, object]:
            captured: dict[str, object] = {}

            def start_response(status, headers):
                captured["status"] = status
                captured["headers"] = headers

            body = b"".join(
                app(
                    {
                        "REQUEST_METHOD": "GET",
                        "PATH_INFO": "/api/dashboard",
                        "QUERY_STRING": "filter=all",
                    },
                    start_response,
                )
            ).decode("utf-8")
            self.assertEqual(captured["status"], "200 OK")
            return json.loads(body)

        first_payload = request_dashboard()
        database.upsert_account(
            {
                "account_id": "acct-2",
                "email": "two@example.com",
                "source_file": "/tmp/two.json",
                "source_mtime_ns": 1,
                "lifecycle_status": LIFECYCLE_ACTIVE,
                "quota_status": QUOTA_AVAILABLE,
                "plan_type": "team",
                "used_percent": 20.0,
                "rate_limit_allowed": 1,
                "rate_limit_reached": 0,
                "reset_at_utc": "2026-03-18T00:00:00Z",
                "last_checked_at_utc": "2026-03-18T00:05:00Z",
                "last_success_at_utc": "2026-03-18T00:05:00Z",
                "last_http_status": 200,
                "consecutive_403_count": 0,
                "invalid_reason_code": None,
                "invalid_reason_detail": None,
                "last_error_detail": None,
                "updated_at_utc": "2026-03-18T00:05:00Z",
            }
        )
        second_payload = request_dashboard()
        third_payload = request_dashboard()

        self.assertEqual(first_payload["summary"]["total"], 1)
        self.assertEqual(second_payload["summary"]["total"], 2)
        self.assertEqual(third_payload["summary"]["total"], 2)

    def test_change_state_revisions_increment_with_account_and_runtime_updates(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()

        initial_state = database.fetch_change_state()
        self.assertEqual(initial_state["accounts_revision"], 0)
        self.assertEqual(initial_state["runtime_revision"], 0)

        database.upsert_account(
            {
                "account_id": "acct-1",
                "email": "one@example.com",
                "source_file": "/tmp/one.json",
                "source_mtime_ns": 1,
                "lifecycle_status": LIFECYCLE_ACTIVE,
                "quota_status": QUOTA_AVAILABLE,
                "plan_type": "team",
                "used_percent": 10.0,
                "rate_limit_allowed": 1,
                "rate_limit_reached": 0,
                "reset_at_utc": "2026-03-18T00:00:00Z",
                "last_checked_at_utc": "2026-03-18T00:00:00Z",
                "last_success_at_utc": "2026-03-18T00:00:00Z",
                "last_http_status": 200,
                "consecutive_403_count": 0,
                "invalid_reason_code": None,
                "invalid_reason_detail": None,
                "last_error_detail": None,
                "updated_at_utc": "2026-03-18T00:00:00Z",
            }
        )
        after_account = database.fetch_change_state()
        self.assertGreater(after_account["accounts_revision"], initial_state["accounts_revision"])
        self.assertEqual(after_account["runtime_revision"], initial_state["runtime_revision"])

        runtime = database.runtime_defaults()
        runtime.update({"phase": COLLECTOR_PHASE_QUERYING})
        database.upsert_runtime(runtime)
        after_runtime = database.fetch_change_state()
        self.assertEqual(after_runtime["accounts_revision"], after_account["accounts_revision"])
        self.assertGreater(after_runtime["runtime_revision"], after_account["runtime_revision"])

    def test_events_api_returns_sse_snapshot(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        database.upsert_account(
            {
                "account_id": "acct-1",
                "email": "one@example.com",
                "source_file": "/tmp/one.json",
                "source_mtime_ns": 1,
                "lifecycle_status": LIFECYCLE_ACTIVE,
                "quota_status": QUOTA_AVAILABLE,
                "plan_type": "team",
                "used_percent": 10.0,
                "rate_limit_allowed": 1,
                "rate_limit_reached": 0,
                "reset_at_utc": "2026-03-18T00:00:00Z",
                "last_checked_at_utc": "2026-03-18T00:00:00Z",
                "last_success_at_utc": "2026-03-18T00:00:00Z",
                "last_http_status": 200,
                "consecutive_403_count": 0,
                "invalid_reason_code": None,
                "invalid_reason_detail": None,
                "last_error_detail": None,
                "updated_at_utc": "2026-03-18T00:00:00Z",
            }
        )
        app = create_app(self.settings)

        captured: dict[str, object] = {}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = headers

        stream = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/api/events",
                "QUERY_STRING": "filter=all",
            },
            start_response,
        )
        iterator = iter(stream)
        first_chunk = next(iterator).decode("utf-8")
        second_chunk = next(iterator).decode("utf-8")
        third_chunk = next(iterator).decode("utf-8")
        if hasattr(stream, "close"):
            stream.close()

        self.assertEqual(captured["status"], "200 OK")
        header_map = dict(captured["headers"])
        self.assertEqual(header_map["Content-Type"], "text/event-stream; charset=utf-8")
        self.assertEqual(header_map["X-Accel-Buffering"], "no")
        self.assertIn("retry: 1500", first_chunk)
        self.assertIn("event: progress", second_chunk)
        self.assertIn("event: dashboard", third_chunk)

    def test_events_api_can_skip_initial_dashboard_when_revision_matches(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        database.upsert_account(
            {
                "account_id": "acct-1",
                "email": "one@example.com",
                "source_file": "/tmp/one.json",
                "source_mtime_ns": 1,
                "lifecycle_status": LIFECYCLE_ACTIVE,
                "quota_status": QUOTA_AVAILABLE,
                "plan_type": "team",
                "used_percent": 10.0,
                "rate_limit_allowed": 1,
                "rate_limit_reached": 0,
                "reset_at_utc": "2026-03-18T00:00:00Z",
                "last_checked_at_utc": "2026-03-18T00:00:00Z",
                "last_success_at_utc": "2026-03-18T00:00:00Z",
                "last_http_status": 200,
                "consecutive_403_count": 0,
                "invalid_reason_code": None,
                "invalid_reason_detail": None,
                "last_error_detail": None,
                "updated_at_utc": "2026-03-18T00:00:00Z",
            }
        )
        revision = build_dashboard_payload(self.settings, "all", database)["accounts_revision"]
        short_ping_settings = Settings(
            project_root=self.settings.project_root,
            subproject_root=self.settings.subproject_root,
            tokens_dir=self.settings.tokens_dir,
            db_path=self.settings.db_path,
            auth_invalid_dir=self.settings.auth_invalid_dir,
            url_prefix=self.settings.url_prefix,
            per_account_interval_seconds=self.settings.per_account_interval_seconds,
            round_interval_seconds=self.settings.round_interval_seconds,
            request_timeout_seconds=self.settings.request_timeout_seconds,
            web_host=self.settings.web_host,
            web_port=self.settings.web_port,
            log_level=self.settings.log_level,
            user_agent=self.settings.user_agent,
            manual_trigger_poll_seconds=self.settings.manual_trigger_poll_seconds,
            sse_poll_seconds=0.1,
            sse_ping_seconds=1.0,
            web_gzip_min_bytes=self.settings.web_gzip_min_bytes,
        )
        app = create_app(short_ping_settings)

        captured: dict[str, object] = {}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = headers

        stream = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/api/events",
                "QUERY_STRING": f"filter=all&skip_initial_dashboard=1&known_accounts_revision={revision}",
            },
            start_response,
        )
        iterator = iter(stream)
        first_chunk = next(iterator).decode("utf-8")
        second_chunk = next(iterator).decode("utf-8")
        third_chunk = next(iterator).decode("utf-8")
        if hasattr(stream, "close"):
            stream.close()

        self.assertEqual(captured["status"], "200 OK")
        header_map = dict(captured["headers"])
        self.assertEqual(header_map["Content-Type"], "text/event-stream; charset=utf-8")
        self.assertEqual(header_map["X-Accel-Buffering"], "no")
        self.assertIn("retry: 1500", first_chunk)
        self.assertIn("event: progress", second_chunk)
        self.assertIn(": ping", third_chunk)
        self.assertNotIn("event: dashboard", third_chunk)

    def test_dashboard_api_supports_gzip(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        database.upsert_account(
            {
                "account_id": "acct-1",
                "email": "one@example.com",
                "source_file": "/tmp/one.json",
                "source_mtime_ns": 1,
                "lifecycle_status": LIFECYCLE_ACTIVE,
                "quota_status": QUOTA_AVAILABLE,
                "plan_type": "team",
                "used_percent": 10.0,
                "rate_limit_allowed": 1,
                "rate_limit_reached": 0,
                "reset_at_utc": "2026-03-18T00:00:00Z",
                "last_checked_at_utc": "2026-03-18T00:00:00Z",
                "last_success_at_utc": "2026-03-18T00:00:00Z",
                "last_http_status": 200,
                "consecutive_403_count": 0,
                "invalid_reason_code": None,
                "invalid_reason_detail": None,
                "last_error_detail": None,
                "updated_at_utc": "2026-03-18T00:00:00Z",
            }
        )
        gzip_settings = Settings(
            project_root=self.settings.project_root,
            subproject_root=self.settings.subproject_root,
            tokens_dir=self.settings.tokens_dir,
            db_path=self.settings.db_path,
            auth_invalid_dir=self.settings.auth_invalid_dir,
            url_prefix=self.settings.url_prefix,
            per_account_interval_seconds=self.settings.per_account_interval_seconds,
            round_interval_seconds=self.settings.round_interval_seconds,
            request_timeout_seconds=self.settings.request_timeout_seconds,
            web_host=self.settings.web_host,
            web_port=self.settings.web_port,
            log_level=self.settings.log_level,
            user_agent=self.settings.user_agent,
            manual_trigger_poll_seconds=self.settings.manual_trigger_poll_seconds,
            sse_poll_seconds=self.settings.sse_poll_seconds,
            sse_ping_seconds=self.settings.sse_ping_seconds,
            web_gzip_min_bytes=1,
        )
        app = create_app(gzip_settings)

        captured: dict[str, object] = {}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = headers

        body = b"".join(
            app(
                {
                    "REQUEST_METHOD": "GET",
                    "PATH_INFO": "/api/dashboard",
                    "QUERY_STRING": "filter=all",
                    "HTTP_ACCEPT_ENCODING": "gzip, deflate",
                },
                start_response,
            )
        )
        header_map = dict(captured["headers"])

        self.assertEqual(captured["status"], "200 OK")
        self.assertEqual(header_map["Content-Encoding"], "gzip")
        payload = json.loads(gzip.decompress(body).decode("utf-8"))
        self.assertEqual(payload["summary"]["total"], 1)

    def test_initialize_migrates_legacy_account_id_primary_key(self) -> None:
        conn = sqlite3.connect(self.settings.db_path)
        conn.execute(
            """
            CREATE TABLE accounts_latest (
                account_id TEXT PRIMARY KEY,
                email TEXT NOT NULL DEFAULT '',
                source_file TEXT NOT NULL DEFAULT '',
                source_mtime_ns INTEGER NOT NULL DEFAULT 0,
                lifecycle_status TEXT NOT NULL,
                quota_status TEXT NOT NULL,
                plan_type TEXT NOT NULL DEFAULT '',
                used_percent REAL,
                rate_limit_allowed INTEGER,
                rate_limit_reached INTEGER,
                reset_at_utc TEXT,
                last_checked_at_utc TEXT,
                last_success_at_utc TEXT,
                last_http_status INTEGER,
                consecutive_403_count INTEGER NOT NULL DEFAULT 0,
                invalid_reason_code TEXT,
                invalid_reason_detail TEXT,
                last_error_detail TEXT,
                updated_at_utc TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO accounts_latest (
                account_id,
                email,
                source_file,
                source_mtime_ns,
                lifecycle_status,
                quota_status,
                plan_type,
                used_percent,
                rate_limit_allowed,
                rate_limit_reached,
                reset_at_utc,
                last_checked_at_utc,
                last_success_at_utc,
                last_http_status,
                consecutive_403_count,
                invalid_reason_code,
                invalid_reason_detail,
                last_error_detail,
                updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-acct",
                "legacy@example.com",
                "/tmp/legacy.json",
                1,
                LIFECYCLE_ACTIVE,
                QUOTA_AVAILABLE,
                "team",
                12.0,
                1,
                0,
                None,
                "2026-03-18T00:00:00Z",
                "2026-03-18T00:00:00Z",
                200,
                0,
                None,
                None,
                None,
                "2026-03-18T00:00:00Z",
            ),
        )
        conn.commit()
        conn.close()

        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        rows = database.fetch_accounts_index()
        migrated_key = build_dimension_key("legacy-acct", "team", "unknown")

        self.assertIn(migrated_key, rows)
        self.assertEqual(rows[migrated_key]["account_id"], "legacy-acct")
        self.assertEqual(rows[migrated_key]["plan_type"], "team")
        self.assertEqual(rows[migrated_key]["chatgpt_user_id"], "unknown")
        self.assertEqual(rows[migrated_key]["consecutive_401_count"], 0)

    def test_initialize_migrates_pair_dimension_table_to_triplet_unknown_user(self) -> None:
        conn = sqlite3.connect(self.settings.db_path)
        conn.execute(
            """
            CREATE TABLE accounts_latest (
                dimension_key TEXT PRIMARY KEY,
                account_id TEXT NOT NULL DEFAULT '',
                email TEXT NOT NULL DEFAULT '',
                source_file TEXT NOT NULL DEFAULT '',
                source_mtime_ns INTEGER NOT NULL DEFAULT 0,
                lifecycle_status TEXT NOT NULL,
                quota_status TEXT NOT NULL,
                plan_type TEXT NOT NULL DEFAULT '',
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
        )
        conn.execute(
            """
            INSERT INTO accounts_latest (
                dimension_key,
                account_id,
                email,
                source_file,
                source_mtime_ns,
                lifecycle_status,
                quota_status,
                plan_type,
                used_percent,
                rate_limit_allowed,
                rate_limit_reached,
                reset_at_utc,
                last_checked_at_utc,
                last_success_at_utc,
                last_http_status,
                consecutive_403_count,
                consecutive_401_count,
                invalid_reason_code,
                invalid_reason_detail,
                last_error_detail,
                updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-pair-key",
                "pair-acct",
                "pair@example.com",
                "/tmp/pair.json",
                2,
                LIFECYCLE_ACTIVE,
                QUOTA_AVAILABLE,
                "team",
                8.0,
                1,
                0,
                None,
                "2026-03-18T01:00:00Z",
                "2026-03-18T01:00:00Z",
                200,
                0,
                0,
                None,
                None,
                None,
                "2026-03-18T01:00:00Z",
            ),
        )
        conn.commit()
        conn.close()

        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        rows = database.fetch_accounts_index()
        migrated_key = build_dimension_key("pair-acct", "team", "unknown")

        self.assertIn(migrated_key, rows)
        self.assertEqual(rows[migrated_key]["chatgpt_user_id"], "unknown")

    def test_run_once_updates_runtime_progress(self) -> None:
        self._write_token(file_name="token0001_demo.json", account_id="acct-1", email="one@example.com")
        self._write_token(file_name="token0002_demo.json", account_id="acct-2", email="two@example.com")
        service = self._build_service(
            ScriptedClient(
                fetch_actions={
                    "acct-1": [make_usage_payload(used_percent=12)],
                    "acct-2": [make_usage_payload(used_percent=32)],
                }
            )
        )

        service.run_once()

        runtime = UsageDatabase(self.settings.db_path).fetch_runtime()
        self.assertIsNotNone(runtime)
        assert runtime is not None
        self.assertEqual(runtime["phase"], COLLECTOR_PHASE_IDLE)
        self.assertEqual(runtime["total_scanned"], 2)
        self.assertEqual(runtime["total_candidates"], 2)
        self.assertEqual(runtime["processed_candidates"], 2)
        self.assertEqual(runtime["skipped_candidates"], 0)
        self.assertEqual(runtime["current_index"], 0)
        self.assertFalse(runtime["current_account_email"])
        self.assertTrue(runtime["round_started_at_utc"])
        self.assertTrue(runtime["round_finished_at_utc"])
        self.assertTrue(runtime["last_heartbeat_at_utc"])

    def test_progress_payload_formats_runtime_state(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        database.upsert_runtime(
            {
                "runtime_key": "collector",
                "phase": COLLECTOR_PHASE_QUERYING,
                "round_started_at_utc": "2026-03-18T00:00:00Z",
                "round_finished_at_utc": None,
                "next_round_at_utc": "2026-03-18T06:00:00Z",
                "total_scanned": 6,
                "total_candidates": 4,
                "processed_candidates": 1,
                "skipped_candidates": 2,
                "current_index": 2,
                "current_account_id": "acct-2",
                "current_account_email": "two@example.com",
                "current_source_file": "/tmp/token0002_demo.json",
                "last_error_detail": "",
                "last_heartbeat_at_utc": "2026-03-18T00:00:30Z",
                "updated_at_utc": "2026-03-18T00:00:30Z",
            }
        )

        payload = build_progress_payload(self.settings)
        self.assertEqual(payload["phase"], COLLECTOR_PHASE_QUERYING)
        self.assertEqual(payload["total_scanned"], 6)
        self.assertEqual(payload["total_candidates"], 4)
        self.assertEqual(payload["processed_candidates"], 1)
        self.assertEqual(payload["current_account_email"], "two@example.com")
        self.assertEqual(payload["current_source_file_name"], "token0002_demo.json")
        self.assertEqual(payload["round_started_at_utc"], "2026-03-18T00:00:00Z")
        self.assertEqual(payload["last_heartbeat_at_utc"], "2026-03-18T00:00:30Z")
        self.assertNotIn("round_started_at_shanghai", payload)
        self.assertNotIn("last_heartbeat_at_shanghai", payload)
        self.assertEqual(payload["poll_interval_ms"], 1000)
        self.assertEqual(payload["progress_percent"], 25.0)
        self.assertFalse(payload["can_manual_start"])
        self.assertTrue(payload["can_manual_stop"])

    def test_progress_payload_sleeping_uses_slow_poll(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        database.upsert_runtime(
            {
                "runtime_key": "collector",
                "phase": COLLECTOR_PHASE_SLEEPING,
                "round_started_at_utc": "2026-03-18T00:00:00Z",
                "round_finished_at_utc": "2026-03-18T00:03:00Z",
                "next_round_at_utc": "2026-03-18T06:00:00Z",
                "total_scanned": 3,
                "total_candidates": 3,
                "processed_candidates": 3,
                "skipped_candidates": 0,
                "current_index": 0,
                "current_account_id": None,
                "current_account_email": None,
                "current_source_file": None,
                "last_error_detail": "",
                "last_heartbeat_at_utc": "2026-03-18T00:03:00Z",
                "updated_at_utc": "2026-03-18T00:03:00Z",
            }
        )

        payload = build_progress_payload(self.settings)
        self.assertEqual(payload["phase"], COLLECTOR_PHASE_SLEEPING)
        self.assertEqual(payload["poll_interval_ms"], 30000)
        self.assertEqual(payload["progress_percent"], 100.0)
        self.assertTrue(payload["can_manual_start"])
        self.assertFalse(payload["can_manual_stop"])

    def test_progress_payload_includes_manual_trigger_state(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        runtime = database.runtime_defaults()
        runtime.update(
            {
                "phase": COLLECTOR_PHASE_SLEEPING,
                "manual_trigger_requested_at": "2026-03-18T00:05:00Z",
            }
        )
        database.upsert_runtime(runtime)

        payload = build_progress_payload(self.settings)
        self.assertTrue(payload["manual_trigger_pending"])
        self.assertEqual(payload["manual_trigger_requested_at_utc"], "2026-03-18T00:05:00Z")
        self.assertNotIn("manual_trigger_requested_at_shanghai", payload)
        self.assertFalse(payload["can_manual_start"])
        self.assertEqual(payload["poll_interval_ms"], 1000)

    def test_progress_payload_includes_manual_stop_state(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        runtime = database.runtime_defaults()
        runtime.update(
            {
                "phase": COLLECTOR_PHASE_SCANNING,
                "manual_stop_requested_at": "2026-03-18T00:06:00Z",
            }
        )
        database.upsert_runtime(runtime)

        payload = build_progress_payload(self.settings)
        self.assertTrue(payload["manual_stop_pending"])
        self.assertEqual(payload["manual_stop_requested_at_utc"], "2026-03-18T00:06:00Z")
        self.assertNotIn("manual_stop_requested_at_shanghai", payload)
        self.assertFalse(payload["can_manual_stop"])
        self.assertEqual(payload["poll_interval_ms"], 1000)

    def test_manual_scan_request_accepts_sleeping_phase(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        runtime = database.runtime_defaults()
        runtime.update({"phase": COLLECTOR_PHASE_SLEEPING})
        database.upsert_runtime(runtime)
        app = create_app(self.settings)

        captured: dict[str, object] = {}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = headers

        body = b"".join(
            app(
                {
                    "REQUEST_METHOD": "POST",
                    "PATH_INFO": "/api/scan",
                    "QUERY_STRING": "",
                },
                start_response,
            )
        ).decode("utf-8")
        payload = json.loads(body)

        self.assertEqual(captured["status"], "202 Accepted")
        self.assertEqual(payload["status"], "accepted")
        self.assertTrue(payload["manual_trigger_pending"])

        runtime_row = UsageDatabase(self.settings.db_path).fetch_runtime()
        assert runtime_row is not None
        self.assertTrue(runtime_row["manual_trigger_requested_at"])

    def test_manual_scan_request_rejects_busy_phase(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        runtime = database.runtime_defaults()
        runtime.update({"phase": COLLECTOR_PHASE_QUERYING})
        database.upsert_runtime(runtime)
        app = create_app(self.settings)

        captured: dict[str, object] = {}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = headers

        body = b"".join(
            app(
                {
                    "REQUEST_METHOD": "POST",
                    "PATH_INFO": "/api/scan",
                    "QUERY_STRING": "",
                },
                start_response,
            )
        ).decode("utf-8")
        payload = json.loads(body)

        self.assertEqual(captured["status"], "409 Conflict")
        self.assertEqual(payload["status"], "busy")
        self.assertFalse(payload["accepted"])

        runtime_row = UsageDatabase(self.settings.db_path).fetch_runtime()
        assert runtime_row is not None
        self.assertFalse(runtime_row["manual_trigger_requested_at"])

    def test_manual_stop_request_accepts_querying_phase(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        runtime = database.runtime_defaults()
        runtime.update({"phase": COLLECTOR_PHASE_QUERYING})
        database.upsert_runtime(runtime)
        app = create_app(self.settings)

        captured: dict[str, object] = {}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = headers

        body = b"".join(
            app(
                {
                    "REQUEST_METHOD": "POST",
                    "PATH_INFO": "/api/scan/stop",
                    "QUERY_STRING": "",
                },
                start_response,
            )
        ).decode("utf-8")
        payload = json.loads(body)

        self.assertEqual(captured["status"], "202 Accepted")
        self.assertEqual(payload["status"], "accepted")
        self.assertTrue(payload["manual_stop_pending"])

        runtime_row = UsageDatabase(self.settings.db_path).fetch_runtime()
        assert runtime_row is not None
        self.assertTrue(runtime_row["manual_stop_requested_at"])

    def test_manual_stop_request_rejects_sleeping_phase(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        runtime = database.runtime_defaults()
        runtime.update({"phase": COLLECTOR_PHASE_SLEEPING})
        database.upsert_runtime(runtime)
        app = create_app(self.settings)

        captured: dict[str, object] = {}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = headers

        body = b"".join(
            app(
                {
                    "REQUEST_METHOD": "POST",
                    "PATH_INFO": "/api/scan/stop",
                    "QUERY_STRING": "",
                },
                start_response,
            )
        ).decode("utf-8")
        payload = json.loads(body)

        self.assertEqual(captured["status"], "409 Conflict")
        self.assertEqual(payload["status"], "busy")
        self.assertFalse(payload["accepted"])

        runtime_row = UsageDatabase(self.settings.db_path).fetch_runtime()
        assert runtime_row is not None
        self.assertFalse(runtime_row["manual_stop_requested_at"])

    def test_wait_for_next_round_consumes_manual_trigger(self) -> None:
        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        runtime = database.runtime_defaults()
        runtime.update(
            {
                "phase": COLLECTOR_PHASE_SLEEPING,
                "manual_trigger_requested_at": "2026-03-18T00:05:00Z",
            }
        )
        database.upsert_runtime(runtime)

        service = self._build_service(ScriptedClient(fetch_actions={}))

        self.assertTrue(service._wait_for_next_round())

        runtime_row = UsageDatabase(self.settings.db_path).fetch_runtime()
        assert runtime_row is not None
        self.assertFalse(runtime_row["manual_trigger_requested_at"])

    def test_run_forever_waits_for_schedule_before_first_scan(self) -> None:
        self._write_token(file_name="token0001_demo.json", account_id="acct-1", email="one@example.com")
        fetch_calls: list[str] = []

        def fetch_action(record):
            fetch_calls.append(record.dimension_key)
            return make_usage_payload(used_percent=12)

        def interrupting_sleeper(*_args, **_kwargs):
            raise KeyboardInterrupt

        service = CollectorService(
            settings=self.settings,
            api_client=ScriptedClient(fetch_actions={"acct-1": [fetch_action]}),
            sleeper=interrupting_sleeper,
        )

        with self.assertRaises(KeyboardInterrupt):
            service.run_forever()

        self.assertEqual(fetch_calls, [])
        runtime_row = UsageDatabase(self.settings.db_path).fetch_runtime()
        assert runtime_row is not None
        self.assertEqual(runtime_row["phase"], COLLECTOR_PHASE_SLEEPING)
        self.assertTrue(runtime_row["next_round_at_utc"])

    def test_run_forever_syncs_cpa_status_without_openai_scan(self) -> None:
        self._write_token(file_name="token0001_demo.json", account_id="acct-1", email="one@example.com")
        fetch_calls: list[str] = []
        cpa_client = FakeCPAStatusClient([])

        def fetch_action(record):
            fetch_calls.append(record.dimension_key)
            return make_usage_payload(used_percent=12)

        def interrupting_sleeper(*_args, **_kwargs):
            raise KeyboardInterrupt

        service = CollectorService(
            settings=replace(
                self.settings,
                cpa_status_enabled=True,
                cpa_status_url="http://127.0.0.1:8317/v0/management/auth-files",
                cpa_management_key="test-key",
                cpa_status_sync_seconds=30,
            ),
            api_client=ScriptedClient(fetch_actions={"acct-1": [fetch_action]}),
            cpa_status_client=cpa_client,
            sleeper=interrupting_sleeper,
        )

        with self.assertRaises(KeyboardInterrupt):
            service.run_forever()

        self.assertEqual(fetch_calls, [])
        self.assertEqual(cpa_client.calls, 1)

    def test_run_once_syncs_cpa_status_during_scan_when_due(self) -> None:
        self._write_token(file_name="token0001_demo.json", account_id="acct-1", email="one@example.com")
        self._write_token(file_name="token0002_demo.json", account_id="acct-2", email="two@example.com")
        cpa_client = FakeCPAStatusClient([])
        service = CollectorService(
            settings=replace(
                self.settings,
                cpa_status_enabled=True,
                cpa_status_url="http://127.0.0.1:8317/v0/management/auth-files",
                cpa_management_key="test-key",
                cpa_status_sync_seconds=5,
            ),
            api_client=ScriptedClient(
                fetch_actions={
                    "acct-1": [make_usage_payload(used_percent=12)],
                    "acct-2": [make_usage_payload(used_percent=18)],
                }
            ),
            cpa_status_client=cpa_client,
            sleeper=lambda *_args, **_kwargs: None,
        )
        service._last_cpa_status_sync_monotonic = 0.0

        service.run_once()

        self.assertGreaterEqual(cpa_client.calls, 2)

    def test_run_once_stops_after_current_record_when_stop_requested_during_processing(self) -> None:
        self._write_token(file_name="token0002_demo.json", account_id="acct-2", email="two@example.com")
        self._write_token(file_name="token0001_demo.json", account_id="acct-1", email="one@example.com")

        def first_action(_record):
            UsageDatabase(self.settings.db_path).request_manual_stop()
            return make_usage_payload(used_percent=12)

        service = self._build_service(
            ScriptedClient(
                fetch_actions={
                    "acct-1": [first_action],
                    "acct-2": [make_usage_payload(used_percent=32)],
                }
            )
        )

        service.run_once()

        rows = UsageDatabase(self.settings.db_path).fetch_accounts_index()
        self.assertIn(build_dimension_key("acct-1", "free", "user-acct-1"), rows)
        self.assertNotIn(build_dimension_key("acct-2", "free", "user-acct-2"), rows)

        runtime_row = UsageDatabase(self.settings.db_path).fetch_runtime()
        assert runtime_row is not None
        self.assertEqual(runtime_row["phase"], COLLECTOR_PHASE_IDLE)
        self.assertFalse(runtime_row["manual_stop_requested_at"])

    def test_run_once_stops_during_account_interval(self) -> None:
        self.settings = Settings(
            project_root=self.settings.project_root,
            subproject_root=self.settings.subproject_root,
            tokens_dir=self.settings.tokens_dir,
            db_path=self.settings.db_path,
            auth_invalid_dir=self.settings.auth_invalid_dir,
            url_prefix=self.settings.url_prefix,
            per_account_interval_seconds=0.4,
            round_interval_seconds=self.settings.round_interval_seconds,
            request_timeout_seconds=self.settings.request_timeout_seconds,
            web_host=self.settings.web_host,
            web_port=self.settings.web_port,
            log_level=self.settings.log_level,
            user_agent=self.settings.user_agent,
            manual_trigger_poll_seconds=self.settings.manual_trigger_poll_seconds,
        )
        self._write_token(file_name="token0002_demo.json", account_id="acct-2", email="two@example.com")
        self._write_token(file_name="token0001_demo.json", account_id="acct-1", email="one@example.com")

        sleep_calls: list[float] = []

        def sleeper(seconds: float) -> None:
            sleep_calls.append(seconds)
            if len(sleep_calls) == 1:
                UsageDatabase(self.settings.db_path).request_manual_stop()

        service = CollectorService(
            settings=self.settings,
            api_client=ScriptedClient(
                fetch_actions={
                    "acct-1": [make_usage_payload(used_percent=12)],
                    "acct-2": [make_usage_payload(used_percent=32)],
                }
            ),
            sleeper=sleeper,
        )

        service.run_once()

        rows = UsageDatabase(self.settings.db_path).fetch_accounts_index()
        self.assertIn(build_dimension_key("acct-1", "free", "user-acct-1"), rows)
        self.assertNotIn(build_dimension_key("acct-2", "free", "user-acct-2"), rows)

    def test_select_candidates_prioritizes_new_and_updated_files(self) -> None:
        self._write_token(file_name="token0001_old.json", account_id="acct-old", email="old@example.com")
        updated_path = self._write_token(
            file_name="token0002_updated.json",
            account_id="acct-updated",
            email="updated@example.com",
        )
        newest_path = self._write_token(
            file_name="token0003_new.json",
            account_id="acct-new",
            email="new@example.com",
        )

        database = UsageDatabase(self.settings.db_path)
        database.initialize()
        database.upsert_account(
            {
                "account_id": "acct-old",
                "email": "old@example.com",
                "source_file": str(self.tokens_dir / "token0001_old.json"),
                "source_mtime_ns": (self.tokens_dir / "token0001_old.json").stat().st_mtime_ns,
                "lifecycle_status": LIFECYCLE_ACTIVE,
                "quota_status": QUOTA_AVAILABLE,
                "plan_type": "free",
                "used_percent": 1.0,
                "rate_limit_allowed": 1,
                "rate_limit_reached": 0,
                "reset_at_utc": None,
                "last_checked_at_utc": "2026-03-18T00:00:00Z",
                "last_success_at_utc": "2026-03-18T00:00:00Z",
                "last_http_status": 200,
                "consecutive_403_count": 0,
                "invalid_reason_code": None,
                "invalid_reason_detail": None,
                "last_error_detail": None,
                "updated_at_utc": "2026-03-18T00:00:00Z",
            }
        )
        database.upsert_account(
            {
                "account_id": "acct-updated",
                "email": "updated@example.com",
                "source_file": str(updated_path),
                "source_mtime_ns": max(updated_path.stat().st_mtime_ns - 1, 0),
                "lifecycle_status": LIFECYCLE_ACTIVE,
                "quota_status": QUOTA_AVAILABLE,
                "plan_type": "free",
                "used_percent": 1.0,
                "rate_limit_allowed": 1,
                "rate_limit_reached": 0,
                "reset_at_utc": None,
                "last_checked_at_utc": "2026-03-18T00:00:00Z",
                "last_success_at_utc": "2026-03-18T00:00:00Z",
                "last_http_status": 200,
                "consecutive_403_count": 0,
                "invalid_reason_code": None,
                "invalid_reason_detail": None,
                "last_error_detail": None,
                "updated_at_utc": "2026-03-18T00:00:00Z",
            }
        )

        updated_payload = json.loads(updated_path.read_text(encoding="utf-8"))
        updated_payload["email"] = "updated@example.com"
        updated_path.write_text(json.dumps(updated_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        updated_stat = updated_path.stat()
        os.utime(updated_path, ns=(updated_stat.st_atime_ns, updated_stat.st_mtime_ns + 10_000))

        newest_payload = json.loads(newest_path.read_text(encoding="utf-8"))
        newest_payload["email"] = "new@example.com"
        newest_path.write_text(json.dumps(newest_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        newest_stat = newest_path.stat()
        os.utime(newest_path, ns=(newest_stat.st_atime_ns, newest_stat.st_mtime_ns + 20_000))

        records, _warnings = scan_tokens_dir(self.tokens_dir)
        service = self._build_service(ScriptedClient(fetch_actions={}))
        sorted_candidates = service._select_candidates(records, database.fetch_accounts_index())

        self.assertEqual(
            [record.account_id for record in sorted_candidates],
            ["acct-new", "acct-updated", "acct-old"],
        )

    def test_shanghai_time_format(self) -> None:
        self.assertEqual(format_shanghai("2026-03-18T00:00:00Z"), "2026-03-18 08:00:00")

    def test_index_page_uses_configured_url_prefix(self) -> None:
        prefixed_settings = Settings(
            project_root=self.settings.project_root,
            subproject_root=self.settings.subproject_root,
            tokens_dir=self.settings.tokens_dir,
            db_path=self.settings.db_path,
            auth_invalid_dir=self.settings.auth_invalid_dir,
            url_prefix="/usage-monitor",
            per_account_interval_seconds=self.settings.per_account_interval_seconds,
            round_interval_seconds=self.settings.round_interval_seconds,
            request_timeout_seconds=self.settings.request_timeout_seconds,
            web_host=self.settings.web_host,
            web_port=self.settings.web_port,
            log_level=self.settings.log_level,
            user_agent=self.settings.user_agent,
        )
        app = create_app(prefixed_settings)

        captured: dict[str, object] = {}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = headers

        body = b"".join(
            app(
                {
                    "REQUEST_METHOD": "GET",
                    "PATH_INFO": "/",
                    "QUERY_STRING": "",
                },
                start_response,
            )
        ).decode("utf-8")

        self.assertEqual(captured["status"], "200 OK")
        self.assertIn('const urlPrefix = "/usage-monitor";', body)
        self.assertIn("const initialDashboardPayload =", body)
        self.assertIn("const initialProgressPayload =", body)
        self.assertIn('params.set("filter", "all");', body)
        self.assertIn('params.set("known_accounts_revision", String(state.dashboardRevision));', body)
        self.assertIn('params.set("skip_initial_dashboard", "1");', body)
        self.assertIn("return withPrefix(`/api/events?${params.toString()}`);", body)
        self.assertIn("const source = new EventSource(getEventStreamUrl());", body)
        self.assertIn('source.addEventListener("progress"', body)
        self.assertIn('source.addEventListener("dashboard"', body)
        self.assertIn('source.addEventListener("dashboard_patch"', body)
        self.assertIn('"/api/scan"', body)
        self.assertIn('"/api/scan/stop"', body)
        self.assertIn('id="scan-stop-button"', body)
        self.assertIn('id="confirm-modal"', body)
        self.assertIn('class="page-header"', body)
        self.assertIn('class="skip-link"', body)
        self.assertIn('href="#accounts-panel"', body)
        self.assertIn('id="current-filter">active</span>', body)
        self.assertIn('id="progress-total-scanned"', body)
        self.assertIn('id="table-row-count"', body)
        self.assertIn('aria-sort="none"', body)
        self.assertIn('aria-busy="false"', body)
        self.assertIn('id="mobile-list-wrap"', body)
        self.assertIn('id="mobile-list"', body)
        self.assertIn('id="sticky-table-shell"', body)
        self.assertIn('id="sticky-table-head"', body)
        self.assertIn('id="sticky-filter-list"', body)
        self.assertIn('dashboardRevision: Number(initialDashboardPayload.accounts_revision || 0),', body)
        self.assertIn("connectEventStream(false);", body)
        self.assertIn("def _current_chart_cache_minute() -> int:", Path("usage_monitor/web.py").read_text(encoding="utf-8"))
        self.assertIn("current_chart_cache_minute != last_chart_cache_minute", Path("usage_monitor/web.py").read_text(encoding="utf-8"))
        self.assertIn("_dashboard_patch_has_live_changes(patch_payload, last_dashboard_payload)", Path("usage_monitor/web.py").read_text(encoding="utf-8"))
        self.assertIn("const EVENT_STREAM_RECONNECT_DELAY_MS = 1500;", body)
        self.assertIn("const RESUME_SYNC_COOLDOWN_MS = 1200;", body)
        self.assertIn("function scheduleEventStreamReconnect(delayMs = EVENT_STREAM_RECONNECT_DELAY_MS) {", body)
        self.assertIn("async function runResumeSync(options = {}) {", body)
        self.assertIn('fetchJsonPayload("/api/dashboard?filter=all")', body)
        self.assertIn('window.addEventListener("pageshow", handleRealtimeResume);', body)
        self.assertIn('window.addEventListener("online", () => {', body)
        self.assertIn("function renderMobileRows(items) {", body)
        self.assertIn("function formatUtcToShanghai(value, empty = \"-\") {", body)
        self.assertIn("function applyDashboardPatch(payload) {", body)
        self.assertIn("function renderDashboardPlaceholder(message = \"正在加载账号数据...\") {", body)
        self.assertIn("function matchesFilter(item, filterName = state.filter) {", body)
        self.assertIn("function filterItemsByActiveView(items, filterName = state.filter) {", body)
        self.assertIn("function getPlanTagClass(planType) {", body)
        self.assertIn("function renderPlanTag(planType) {", body)
        self.assertIn("function getPlanTypePriority(planType) {", body)
        self.assertIn("enterprise: 10,", body)
        self.assertIn("business: 20,", body)
        self.assertIn("pro: 30,", body)
        self.assertIn("plus: 40,", body)
        self.assertIn("team: 50,", body)
        self.assertIn("unknown: 900,", body)
        self.assertIn("free: 1000,", body)
        self.assertIn("const typeOrder = compareNullable(", body)
        self.assertIn("getPlanTypePriority(left.plan_type)", body)
        self.assertIn("getPlanTypePriority(right.plan_type)", body)
        self.assertIn("function formatCompactDateTimeText(value) {", body)
        self.assertIn("function buildLinearTrendPath(points) {", body)
        self.assertIn("const historyPath = buildLinearTrendPath(historyChartPoints);", body)
        self.assertIn("const recoveryPath = buildLinearTrendPath(recoveryChartPoints);", body)
        self.assertNotIn("function buildSmoothTrendPath(points) {", body)
        self.assertIn("function syncStickyHeaderLayout()", body)
        self.assertIn("function updateStickyHeaderVisibility()", body)
        self.assertIn("function renderStickyQuickFilters(summary = state.summary || {})", body)
        self.assertIn("function syncFilterSelectionState() {", body)
        self.assertIn('document.querySelectorAll("#summary [data-filter]")', body)
        self.assertIn('document.querySelectorAll("#sticky-filter-list [data-sticky-filter]")', body)
        self.assertIn("syncFilterSelectionState();", body)
        self.assertIn("applyDashboardSnapshot(initialDashboardPayload);", body)
        self.assertIn('document.getElementById("table-wrap").addEventListener("scroll"', body)
        self.assertIn('document.getElementById("sticky-filter-list").addEventListener("click"', body)
        self.assertIn('class="plan-tag ${getPlanTagClass(normalized)}"', body)
        self.assertNotIn('data-sort-key="quota_status"', body)
        self.assertNotIn('aria-label="按主额度排序"', body)
        self.assertNotIn('data-label="主额度"', body)
        self.assertNotIn('class="cell-secondary mono">${{escapeHtml(item.account_id || "-")}} ·', body)
        self.assertNotIn('class="cell-secondary is-truncate">${{escapeHtml(item.source_file || "-")}}</div>', body)
        self.assertNotIn("loadDashboard()", body)
        self.assertNotIn("loadProgress()", body)
        self.assertNotIn("window.setInterval(loadDashboard", body)
        self.assertNotIn("prepareDashboardForFilterChange();", body)
        self.assertNotIn("SSE 实时推送", body)
        self.assertNotIn("页面改为 SSE", body)


if __name__ == "__main__":
    unittest.main()
