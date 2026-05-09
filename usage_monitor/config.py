from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_USER_AGENT = "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal"


def _load_dotenv_values(dotenv_path: Path) -> dict[str, str]:
    if not dotenv_path.is_file():
        return {}

    values: dict[str, str] = {}
    for line in dotenv_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[7:].lstrip()
        if "=" not in stripped:
            continue
        name, raw_value = stripped.split("=", 1)
        name = name.strip()
        if not name:
            continue
        value = raw_value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[name] = value
    return values


def _env_raw(name: str, dotenv_values: dict[str, str]) -> str:
    if name in os.environ:
        return os.environ.get(name, "").strip()
    return dotenv_values.get(name, "").strip()


def _env_text(name: str, default: str, dotenv_values: dict[str, str]) -> str:
    value = _env_raw(name, dotenv_values)
    return value or default


def _env_int(name: str, default: int, dotenv_values: dict[str, str], minimum: int = 0) -> int:
    raw = _env_raw(name, dotenv_values)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(value, minimum)


def _env_float(name: str, default: float, dotenv_values: dict[str, str], minimum: float = 0.0) -> float:
    raw = _env_raw(name, dotenv_values)
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return max(value, minimum)


def _env_path(name: str, default: Path, dotenv_values: dict[str, str], base_dir: Path) -> Path:
    value = _env_raw(name, dotenv_values)
    if not value:
        return default
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _normalize_url_prefix(value: str) -> str:
    prefix = value.strip()
    if not prefix or prefix == "/":
        return ""
    prefix = "/" + prefix.strip("/")
    return prefix


@dataclass(frozen=True)
class Settings:
    project_root: Path
    subproject_root: Path
    tokens_dir: Path
    db_path: Path
    auth_invalid_dir: Path
    url_prefix: str
    per_account_interval_seconds: float
    round_interval_seconds: float
    request_timeout_seconds: int
    web_host: str
    web_port: int
    log_level: str
    user_agent: str = DEFAULT_USER_AGENT
    manual_trigger_poll_seconds: float = 2.0


def load_settings() -> Settings:
    project_root = Path(__file__).resolve().parents[2]
    subproject_root = project_root / "usage-monitor"
    dotenv_values = _load_dotenv_values(subproject_root / ".env.example")
    dotenv_values.update(_load_dotenv_values(subproject_root / ".env"))
    tokens_dir = _env_path(
        "USAGE_MONITOR_TOKENS_DIR",
        project_root / "tokens",
        dotenv_values,
        subproject_root,
    )
    db_path = _env_path(
        "USAGE_MONITOR_DB_PATH",
        subproject_root / "data" / "usage-monitor.sqlite3",
        dotenv_values,
        subproject_root,
    )
    return Settings(
        project_root=project_root,
        subproject_root=subproject_root,
        tokens_dir=tokens_dir,
        db_path=db_path,
        auth_invalid_dir=_env_path(
            "USAGE_MONITOR_AUTH_INVALID_DIR",
            subproject_root / "data" / "authInvalid",
            dotenv_values,
            subproject_root,
        ),
        url_prefix=_normalize_url_prefix(
            _env_text("USAGE_MONITOR_URL_PREFIX", "", dotenv_values)
        ),
        per_account_interval_seconds=_env_float(
            "USAGE_MONITOR_PER_ACCOUNT_INTERVAL_SECONDS",
            default=2.0,
            dotenv_values=dotenv_values,
            minimum=0.0,
        ),
        round_interval_seconds=_env_float(
            "USAGE_MONITOR_ROUND_INTERVAL_SECONDS",
            default=300.0,
            dotenv_values=dotenv_values,
            minimum=1.0,
        ),
        request_timeout_seconds=_env_int(
            "USAGE_MONITOR_REQUEST_TIMEOUT_SECONDS",
            default=30,
            dotenv_values=dotenv_values,
            minimum=1,
        ),
        web_host=_env_text("USAGE_MONITOR_WEB_HOST", "127.0.0.1", dotenv_values),
        web_port=_env_int("USAGE_MONITOR_WEB_PORT", default=8765, dotenv_values=dotenv_values, minimum=1),
        log_level=_env_text("USAGE_MONITOR_LOG_LEVEL", "INFO", dotenv_values).upper(),
        manual_trigger_poll_seconds=_env_float(
            "USAGE_MONITOR_MANUAL_TRIGGER_POLL_SECONDS",
            default=2.0,
            dotenv_values=dotenv_values,
            minimum=0.2,
        ),
    )
