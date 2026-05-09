"""token 文件扫描、刷新回写与失效文件搬运。"""

from __future__ import annotations

import base64
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from .models import (
    RefreshedTokens,
    TokenRecord,
    normalize_chatgpt_user_id,
    normalize_plan_type,
)


JWT_AUTH_CLAIM_KEY = "https://api.openai.com/auth"


def _require_dict(raw: Any, path: Path) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"JSON 顶层必须是对象: {path}")
    return raw


def _read_json(path: Path) -> dict[str, Any]:
    try:
        content = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"读取失败: {path}: {exc}") from exc

    try:
        payload = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"无效 JSON: {path}") from exc

    return _require_dict(payload, path)


def _optional_string(data: dict[str, Any], field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str):
        return ""
    return value.strip()


def _required_string(data: dict[str, Any], field: str, path: Path) -> str:
    value = _optional_string(data, field)
    if not value:
        raise ValueError(f"缺少有效字段 {field}: {path}")
    return value


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    compact = token.strip()
    if not compact:
        return {}

    parts = compact.split(".")
    if len(parts) < 2:
        return {}

    payload_segment = parts[1]
    padded_segment = payload_segment + ("=" * (-len(payload_segment) % 4))
    try:
        payload_raw = base64.urlsafe_b64decode(padded_segment.encode("utf-8"))
        payload = json.loads(payload_raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
        return {}

    if not isinstance(payload, dict):
        return {}
    return payload


def _extract_token_auth_payload(raw: dict[str, Any]) -> dict[str, Any]:
    for token_field in ("id_token", "access_token"):
        payload = _decode_jwt_payload(_optional_string(raw, token_field))
        auth_payload = payload.get(JWT_AUTH_CLAIM_KEY)
        if not isinstance(auth_payload, dict):
            continue
        return auth_payload
    return {}


def extract_token_plan_type(raw: dict[str, Any]) -> str:
    auth_payload = _extract_token_auth_payload(raw)
    raw_plan_type = auth_payload.get("chatgpt_plan_type")
    if raw_plan_type not in (None, ""):
        return normalize_plan_type(raw_plan_type)
    return normalize_plan_type("")


def extract_token_user_id(raw: dict[str, Any]) -> str:
    auth_payload = _extract_token_auth_payload(raw)
    raw_user_id = auth_payload.get("chatgpt_user_id")
    if raw_user_id not in (None, ""):
        return normalize_chatgpt_user_id(raw_user_id)
    return normalize_chatgpt_user_id("")


def scan_tokens_dir(tokens_dir: Path) -> tuple[list[TokenRecord], list[str]]:
    records_by_dimension: dict[str, TokenRecord] = {}
    warnings: list[str] = []

    if not tokens_dir.exists():
        warnings.append(f"tokens 目录不存在: {tokens_dir}")
        return [], warnings

    for path in sorted(tokens_dir.glob("*.json")):
        if not path.is_file():
            continue

        try:
            raw = _read_json(path)
            account_id = _required_string(raw, "account_id", path)
            access_token = _required_string(raw, "access_token", path)
            stat = path.stat()
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"跳过 token 文件 {path.name}: {exc}")
            continue

        record = TokenRecord(
            account_id=account_id,
            plan_type=extract_token_plan_type(raw),
            chatgpt_user_id=extract_token_user_id(raw),
            email=_optional_string(raw, "email"),
            access_token=access_token,
            refresh_token=_optional_string(raw, "refresh_token"),
            source_file=path.resolve(),
            source_mtime_ns=stat.st_mtime_ns,
            raw_data=raw,
            expired=_optional_string(raw, "expired"),
            id_token=_optional_string(raw, "id_token"),
        )

        existing = records_by_dimension.get(record.dimension_key)
        if existing is None:
            records_by_dimension[record.dimension_key] = record
            continue

        current_key = (record.source_mtime_ns, record.source_file_name)
        existing_key = (existing.source_mtime_ns, existing.source_file_name)
        if current_key > existing_key:
            warnings.append(
                "账号 "
                f"{record.account_id} / {record.plan_type} / {record.chatgpt_user_id} "
                f"存在重复 token 文件，使用较新的 {record.source_file_name}，忽略 {existing.source_file_name}"
            )
            records_by_dimension[record.dimension_key] = record
        else:
            warnings.append(
                "账号 "
                f"{record.account_id} / {record.plan_type} / {record.chatgpt_user_id} "
                f"存在重复 token 文件，使用较新的 {existing.source_file_name}，忽略 {record.source_file_name}"
            )

    records = sorted(records_by_dimension.values(), key=lambda item: item.source_file_name)
    return records, warnings


def write_refreshed_tokens(record: TokenRecord, refreshed: RefreshedTokens) -> TokenRecord:
    try:
        latest_raw = _read_json(record.source_file)
    except Exception:  # noqa: BLE001
        latest_raw = dict(record.raw_data)

    latest_raw["access_token"] = refreshed.access_token
    latest_raw["refresh_token"] = refreshed.refresh_token
    latest_raw["last_refresh"] = refreshed.last_refresh
    latest_raw["expired"] = refreshed.expired
    if refreshed.id_token:
        latest_raw["id_token"] = refreshed.id_token

    record.source_file.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=record.source_file.parent,
        prefix=f"{record.source_file.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        json.dump(latest_raw, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        temp_path = Path(handle.name)

    temp_path.replace(record.source_file)
    stat = record.source_file.stat()
    return TokenRecord(
        account_id=record.account_id,
        plan_type=extract_token_plan_type(latest_raw),
        chatgpt_user_id=extract_token_user_id(latest_raw),
        email=_optional_string(latest_raw, "email") or record.email,
        access_token=refreshed.access_token,
        refresh_token=refreshed.refresh_token,
        source_file=record.source_file,
        source_mtime_ns=stat.st_mtime_ns,
        raw_data=latest_raw,
        expired=refreshed.expired,
        id_token=refreshed.id_token or _optional_string(latest_raw, "id_token"),
    )


def _safe_file_fragment(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value)
    cleaned = cleaned.strip("._")
    return cleaned or "account"


def _pick_auth_invalid_destination(source_file: Path, auth_invalid_dir: Path, account_id: str) -> Path:
    candidate = auth_invalid_dir / source_file.name
    if candidate.resolve() == source_file.resolve() or not candidate.exists():
        return candidate

    stem = source_file.stem
    suffix = source_file.suffix or ".json"
    account_fragment = _safe_file_fragment(account_id)
    candidate = auth_invalid_dir / f"{stem}__{account_fragment}{suffix}"
    if candidate.resolve() == source_file.resolve() or not candidate.exists():
        return candidate

    index = 2
    while True:
        candidate = auth_invalid_dir / f"{stem}__{account_fragment}__{index}{suffix}"
        if candidate.resolve() == source_file.resolve() or not candidate.exists():
            return candidate
        index += 1


def move_token_to_auth_invalid(record: TokenRecord, auth_invalid_dir: Path) -> TokenRecord:
    target_dir = auth_invalid_dir.expanduser().resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    destination = _pick_auth_invalid_destination(record.source_file, target_dir, record.account_id)

    if destination.resolve() != record.source_file.resolve():
        moved_path = Path(shutil.move(str(record.source_file), str(destination)))
    else:
        moved_path = destination

    moved_path = moved_path.resolve()
    stat = moved_path.stat()
    return TokenRecord(
        account_id=record.account_id,
        plan_type=record.plan_type,
        chatgpt_user_id=record.chatgpt_user_id,
        email=record.email,
        access_token=record.access_token,
        refresh_token=record.refresh_token,
        source_file=moved_path,
        source_mtime_ns=stat.st_mtime_ns,
        raw_data=dict(record.raw_data),
        expired=record.expired,
        id_token=record.id_token,
    )
