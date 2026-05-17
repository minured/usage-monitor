"""核心数据模型与维度键辅助函数。"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


LIFECYCLE_ACTIVE = "active"
LIFECYCLE_INVALID = "invalid"
LIFECYCLE_SOURCE_MISSING = "source_missing"

QUOTA_AVAILABLE = "available"
QUOTA_EXHAUSTED = "exhausted"
QUOTA_UNKNOWN = "unknown"

COLLECTOR_PHASE_IDLE = "idle"
COLLECTOR_PHASE_SCANNING = "scanning"
COLLECTOR_PHASE_QUERYING = "querying"
COLLECTOR_PHASE_RECONCILING = "reconciling"
COLLECTOR_PHASE_SLEEPING = "sleeping"
COLLECTOR_PHASE_ERROR = "error"
PLAN_TYPE_UNKNOWN = "unknown"
CHATGPT_USER_ID_UNKNOWN = "unknown"


def normalize_plan_type(value: Any) -> str:
    cleaned = str(value or "").strip().lower()
    return cleaned or PLAN_TYPE_UNKNOWN


def normalize_chatgpt_user_id(value: Any) -> str:
    cleaned = str(value or "").strip()
    return cleaned or CHATGPT_USER_ID_UNKNOWN


def build_dimension_key(account_id: str, plan_type: Any, chatgpt_user_id: Any) -> str:
    normalized_account_id = str(account_id or "").strip()
    normalized_plan_type = normalize_plan_type(plan_type)
    normalized_chatgpt_user_id = normalize_chatgpt_user_id(chatgpt_user_id)
    return f"{normalized_account_id}::{normalized_plan_type}::{normalized_chatgpt_user_id}"


@dataclass
class TokenRecord:
    account_id: str
    plan_type: str
    chatgpt_user_id: str
    email: str
    access_token: str
    refresh_token: str
    source_file: Path
    source_mtime_ns: int
    source_created_at_utc: str = ""
    raw_data: dict[str, Any] = field(default_factory=dict)
    expired: str = ""
    id_token: str = ""

    @property
    def source_file_name(self) -> str:
        return self.source_file.name

    @property
    def dimension_key(self) -> str:
        return build_dimension_key(self.account_id, self.plan_type, self.chatgpt_user_id)


@dataclass(frozen=True)
class RefreshedTokens:
    access_token: str
    refresh_token: str
    last_refresh: str
    expired: str
    id_token: str = ""
