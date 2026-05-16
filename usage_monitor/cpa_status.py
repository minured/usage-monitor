"""CPA 只读状态采集客户端。

本模块只调用 CPA 的 GET /v0/management/auth-files 接口，用于把 CPA 运行时
已经知道的账号可用/耗尽状态同步到 usage-monitor 自己的数据库。这里绝不调用
会消费队列或修改 CPA 数据的接口。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, request

from .models import QUOTA_AVAILABLE, QUOTA_EXHAUSTED, QUOTA_UNKNOWN
from .timeutil import format_utc


@dataclass(frozen=True)
class CPAAuthStatus:
    """CPA 单个 auth 文件的只读运行时状态。"""

    source_file_name: str
    email: str
    quota_status: str
    reset_at_utc: str | None
    cpa_status: str
    cpa_status_message: str

    def as_db_payload(self) -> dict[str, Any]:
        return {
            "source_file_name": self.source_file_name,
            "email": self.email,
            "quota_status": self.quota_status,
            "reset_at_utc": self.reset_at_utc,
            "cpa_status": self.cpa_status,
            "cpa_status_message": self.cpa_status_message,
        }


class CPAStatusError(RuntimeError):
    """CPA 只读状态接口请求或响应异常。"""


class CPAStatusClient:
    """只读 CPA auth-files 客户端。"""

    def __init__(self, url: str, management_key: str, timeout: int = 5):
        self.url = str(url or "").strip()
        self.management_key = str(management_key or "").strip()
        self.timeout = max(int(timeout or 5), 1)

    def fetch_auth_statuses(self) -> list[CPAAuthStatus]:
        if not self.url:
            raise CPAStatusError("缺少 CPA auth-files URL")
        if not self.management_key:
            raise CPAStatusError("缺少 CPA management key")

        req = request.Request(
            self.url,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.management_key}",
            },
            method="GET",
        )
        try:
            with request.urlopen(req, timeout=self.timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8", errors="replace"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:300]
            raise CPAStatusError(f"CPA auth-files HTTP {exc.code}: {detail}") from exc
        except Exception as exc:  # noqa: BLE001
            raise CPAStatusError(f"CPA auth-files 请求失败: {exc}") from exc

        files = payload.get("files") if isinstance(payload, dict) else None
        if not isinstance(files, list):
            raise CPAStatusError("CPA auth-files 响应缺少 files 数组")
        return [parse_auth_file_status(item) for item in files if isinstance(item, dict)]


def parse_auth_file_status(item: dict[str, Any]) -> CPAAuthStatus:
    """把 CPA auth-files 条目归一化成 Monitor 可使用的状态。"""

    source_file_name = _source_file_name(item)
    email = _first_text(item, "email", "account", "label")
    cpa_status = _first_text(item, "status") or "unknown"
    raw_message = _first_text(item, "status_message")
    message_type, message_text, message_reset_at_utc = _extract_message(raw_message)
    quota_status = _classify_quota_status(item, cpa_status, message_type, message_text)
    reset_at_utc = _parse_cpa_time(_first_text(item, "next_retry_after")) or message_reset_at_utc
    return CPAAuthStatus(
        source_file_name=source_file_name,
        email=email,
        quota_status=quota_status,
        reset_at_utc=reset_at_utc,
        cpa_status=cpa_status,
        cpa_status_message=_compact_message(message_text or raw_message),
    )


def _source_file_name(item: dict[str, Any]) -> str:
    for key in ("name", "id", "path"):
        value = _first_text(item, key)
        if value:
            return Path(value).name
    return ""


def _first_text(item: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = item.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _extract_message(raw: str) -> tuple[str, str, str | None]:
    if not raw:
        return "", "", None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return "", raw, None

    found_type = ""
    found_message = ""
    found_reset_at_utc: str | None = None

    def walk(value: Any) -> None:
        nonlocal found_type, found_message, found_reset_at_utc
        if isinstance(value, dict):
            for key, child in value.items():
                lowered = str(key).lower()
                if lowered == "type" and not found_type:
                    found_type = str(child or "").strip()
                elif lowered in {"message", "error", "detail"} and not found_message:
                    if not isinstance(child, (dict, list)):
                        found_message = str(child or "").strip()
                elif lowered in {"resets_at", "reset_at"} and found_reset_at_utc is None:
                    found_reset_at_utc = _parse_cpa_epoch_time(child) or _parse_cpa_time(str(child or ""))
                walk(child)
        elif isinstance(value, list):
            for child in value[:5]:
                walk(child)

    walk(payload)
    return found_type, found_message, found_reset_at_utc


def _classify_quota_status(
    item: dict[str, Any],
    cpa_status: str,
    message_type: str,
    message_text: str,
) -> str:
    haystack = " ".join(
        str(part or "")
        for part in (
            cpa_status,
            message_type,
            message_text,
            item.get("status_message"),
        )
    ).lower()
    if "usage_limit_reached" in haystack or "usage limit" in haystack:
        return QUOTA_EXHAUSTED
    if cpa_status.lower() == "active" and item.get("disabled") is not True:
        return QUOTA_AVAILABLE
    return QUOTA_UNKNOWN


def _parse_cpa_epoch_time(raw: Any) -> str | None:
    if isinstance(raw, bool):
        return None
    try:
        timestamp = float(raw)
    except (TypeError, ValueError):
        return None
    if timestamp <= 0:
        return None
    try:
        return format_utc(datetime.fromtimestamp(timestamp, tz=timezone.utc))
    except (OverflowError, OSError, ValueError):
        return None


def _parse_cpa_time(raw: str) -> str | None:
    if not raw:
        return None
    text = raw.strip()
    # Go 的 RFC3339Nano 可能包含 9 位纳秒；Python 只需要微秒，截断即可。
    if "." in text:
        head, tail = text.split(".", 1)
        tz_pos = min(
            [pos for pos in (tail.find("+"), tail.find("-"), tail.find("Z")) if pos >= 0]
            or [len(tail)]
        )
        fraction = tail[:tz_pos][:6]
        suffix = tail[tz_pos:]
        text = f"{head}.{fraction}{suffix}"
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return format_utc(parsed)


def _compact_message(raw: str, limit: int = 240) -> str:
    text = " ".join(str(raw or "").split())
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."
