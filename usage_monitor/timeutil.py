from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


UTC = timezone.utc
if ZoneInfo is not None:
    try:
        SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
    except Exception:  # pragma: no cover
        SHANGHAI_TZ = timezone(timedelta(hours=8))
else:  # pragma: no cover
    SHANGHAI_TZ = timezone(timedelta(hours=8))


def utc_now() -> datetime:
    return datetime.now(UTC)


def format_utc(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_now_iso() -> str:
    return format_utc(utc_now())


def parse_utc(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso_from_unix(value: Any) -> str | None:
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return None
    return format_utc(datetime.fromtimestamp(timestamp, tz=UTC))


def iso_after_seconds(seconds: Any) -> str:
    try:
        delta_seconds = int(seconds)
    except (TypeError, ValueError):
        delta_seconds = 0
    return format_utc(utc_now() + timedelta(seconds=max(delta_seconds, 0)))


@lru_cache(maxsize=4096)
def _format_shanghai_cached(raw: str) -> str:
    parsed = parse_utc(raw)
    if parsed is None:
        return "-"
    return parsed.astimezone(SHANGHAI_TZ).strftime("%Y-%m-%d %H:%M:%S")


def format_shanghai(raw: str | None, empty: str = "-") -> str:
    if not raw:
        return empty
    text = str(raw)
    if empty == "-":
        return _format_shanghai_cached(text)
    parsed = parse_utc(text)
    if parsed is None:
        return empty
    return parsed.astimezone(SHANGHAI_TZ).strftime("%Y-%m-%d %H:%M:%S")
