from __future__ import annotations

import json
import urllib.parse
from typing import Any
from urllib import error, request

from .config import DEFAULT_USER_AGENT
from .models import RefreshedTokens, TokenRecord
from .timeutil import iso_after_seconds, utc_now_iso


USAGE_URL = "https://chatgpt.com/backend-api/wham/usage"
TOKEN_URL = "https://auth.openai.com/oauth/token"
CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"


def _parse_body(raw: bytes) -> Any:
    text = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _render_body(body: Any) -> str:
    if body in (None, ""):
        return ""
    if isinstance(body, str):
        return body
    return json.dumps(body, ensure_ascii=False)


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


class HTTPStatusError(RuntimeError):
    def __init__(self, status: int, reason: str, body: Any = None):
        self.status = status
        self.reason = reason
        self.body = body
        detail = _render_body(body)
        message = f"HTTP {status} {reason}"
        if detail:
            message = f"{message}: {detail}"
        super().__init__(message)


class TransportError(RuntimeError):
    pass


class InvalidResponseError(RuntimeError):
    def __init__(self, message: str, status: int | None = None, body: Any = None):
        self.status = status
        self.body = body
        detail = _render_body(body)
        full_message = message
        if status is not None:
            full_message = f"{full_message} (HTTP {status})"
        if detail:
            full_message = f"{full_message}: {detail}"
        super().__init__(full_message)


class OpenAIUsageClient:
    def __init__(self, timeout: int, user_agent: str = DEFAULT_USER_AGENT):
        self.timeout = timeout
        self.user_agent = user_agent

    def fetch_usage(self, record: TokenRecord) -> dict[str, Any]:
        req = request.Request(
            USAGE_URL,
            headers={
                "Accept": "application/json, text/plain, */*",
                "Authorization": f"Bearer {record.access_token}",
                "Content-Type": "application/json",
                "Chatgpt-Account-Id": record.account_id,
                "User-Agent": self.user_agent,
            },
            method="GET",
        )

        try:
            with request.urlopen(req, timeout=self.timeout) as resp:
                body = _parse_body(resp.read())
                if not isinstance(body, dict):
                    raise InvalidResponseError("usage 响应不是 JSON 对象", resp.status, body)
                return body
        except error.HTTPError as exc:
            body = _parse_body(exc.read())
            raise HTTPStatusError(exc.code, str(exc.reason), body) from exc
        except error.URLError as exc:
            raise TransportError(f"请求失败: {exc.reason}") from exc

    def refresh_tokens(self, record: TokenRecord) -> RefreshedTokens:
        refresh_token = record.refresh_token.strip()
        if not refresh_token:
            raise InvalidResponseError("缺少 refresh_token，无法刷新")

        body = urllib.parse.urlencode(
            {
                "grant_type": "refresh_token",
                "client_id": CLIENT_ID,
                "refresh_token": refresh_token,
            }
        ).encode("utf-8")
        req = request.Request(
            TOKEN_URL,
            data=body,
            method="POST",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": self.user_agent,
            },
        )

        try:
            with request.urlopen(req, timeout=self.timeout) as resp:
                payload = _parse_body(resp.read())
                if not isinstance(payload, dict):
                    raise InvalidResponseError("refresh 响应不是 JSON 对象", resp.status, payload)
        except error.HTTPError as exc:
            payload = _parse_body(exc.read())
            raise HTTPStatusError(exc.code, str(exc.reason), payload) from exc
        except error.URLError as exc:
            raise TransportError(f"refresh 请求失败: {exc.reason}") from exc

        access_token = str(payload.get("access_token") or "").strip()
        if not access_token:
            raise InvalidResponseError("refresh 响应缺少 access_token", body=payload)

        new_refresh_token = str(payload.get("refresh_token") or "").strip() or refresh_token
        id_token = str(payload.get("id_token") or "").strip()
        expires_in = _to_int(payload.get("expires_in"))
        return RefreshedTokens(
            access_token=access_token,
            refresh_token=new_refresh_token,
            last_refresh=utc_now_iso(),
            expired=iso_after_seconds(expires_in),
            id_token=id_token,
        )
