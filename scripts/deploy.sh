#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

SYNC_ENV=0
SYNC_HTPASSWD=0
SKIP_CHECKS=0

log() {
  printf '[usage-monitor deploy] %s\n' "$*"
}

fail() {
  printf '[usage-monitor deploy] %s\n' "$*" >&2
  exit 1
}

load_dotenv_file() {
  local dotenv_path="$1"
  [[ -f "${dotenv_path}" ]] || return 0
  set -a
  # shellcheck disable=SC1090
  source "${dotenv_path}"
  set +a
}

usage() {
  cat <<'EOF'
用法：
  ./scripts/deploy.sh [选项]

默认行为：
  1. 本地运行 usage-monitor 单元测试
  2. rsync 同步 usage-monitor/ 到 ${USAGE_MONITOR_DEPLOY_SSH_TARGET}:${USAGE_MONITOR_DEPLOY_REMOTE_DIR}
  3. 远端执行 docker compose up -d --build
  4. 远端验证 compose 状态，并轮询等待 /healthz 成功

选项：
  --sync-env         额外同步本地 usage-monitor/.env 到远端 .env
  --sync-htpasswd    额外同步本地 usage-monitor/.htpasswd 到远端，并安装到 /etc/nginx/usage-monitor.htpasswd
  --skip-checks      跳过本地单元测试
  --help             显示帮助

环境变量来源：
  1. 当前 shell 环境变量
  2. usage-monitor/.env.example
  3. usage-monitor/.env

关键变量：
  USAGE_MONITOR_DEPLOY_SSH_TARGET   远端 SSH 别名，例如 your-server
  USAGE_MONITOR_DEPLOY_REMOTE_DIR   远端部署目录，例如 /srv/usage-monitor
  USAGE_MONITOR_DEPLOY_NGINX_HTPASSWD_PATH
                                 远端 Nginx Basic Auth 文件路径
  USAGE_MONITOR_DEPLOY_HEALTHCHECK_RETRIES
                                 远端健康检查重试次数，默认 15
  USAGE_MONITOR_DEPLOY_HEALTHCHECK_INTERVAL_SECONDS
                                 两次健康检查之间的等待秒数，默认 2
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --sync-env)
      SYNC_ENV=1
      shift
      ;;
    --sync-htpasswd)
      SYNC_HTPASSWD=1
      shift
      ;;
    --skip-checks)
      SKIP_CHECKS=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      fail "未知参数: $1"
      ;;
  esac
done

load_dotenv_file "${PROJECT_DIR}/.env.example"
load_dotenv_file "${PROJECT_DIR}/.env"

REMOTE_HOST="${USAGE_MONITOR_DEPLOY_SSH_TARGET:-}"
REMOTE_DIR="${USAGE_MONITOR_DEPLOY_REMOTE_DIR:-}"
NGINX_HTPASSWD_PATH="${USAGE_MONITOR_DEPLOY_NGINX_HTPASSWD_PATH:-/etc/nginx/usage-monitor.htpasswd}"
HEALTHCHECK_RETRIES="${USAGE_MONITOR_DEPLOY_HEALTHCHECK_RETRIES:-15}"
HEALTHCHECK_INTERVAL_SECONDS="${USAGE_MONITOR_DEPLOY_HEALTHCHECK_INTERVAL_SECONDS:-2}"

require_command() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || fail "缺少命令: $cmd"
}

find_python() {
  if [[ -x "${PROJECT_DIR}/../.venv/bin/python" ]]; then
    printf '%s\n' "${PROJECT_DIR}/../.venv/bin/python"
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return 0
  fi
  fail "未找到可用 Python，请先准备 .venv 或安装 python3"
}

sync_optional_file() {
  local local_path="$1"
  local remote_path="$2"
  [[ -f "${local_path}" ]] || fail "本地文件不存在: ${local_path}"
  rsync -az "${local_path}" "${REMOTE_HOST}:${remote_path}"
}

require_command ssh
require_command rsync

[[ -n "${REMOTE_HOST}" ]] || fail "缺少 USAGE_MONITOR_DEPLOY_SSH_TARGET，请写到 .env 或当前 shell 环境变量"
[[ -n "${REMOTE_DIR}" ]] || fail "缺少 USAGE_MONITOR_DEPLOY_REMOTE_DIR，请写到 .env 或当前 shell 环境变量"

PYTHON_BIN="$(find_python)"

if [[ "${SKIP_CHECKS}" -eq 0 ]]; then
  log "运行本地单元测试"
  (
    cd "${PROJECT_DIR}"
    "${PYTHON_BIN}" -m unittest discover -s tests -p 'spec_*.py'
  )
else
  log "跳过本地单元测试"
fi

log "准备远端目录 ${REMOTE_HOST}:${REMOTE_DIR}"
# 避免前置 ssh 吃掉 deploy 脚本 stdin，导致后续 if/heredoc 被截断。
ssh -n "${REMOTE_HOST}" "mkdir -p '${REMOTE_DIR}' '${REMOTE_DIR}/data'"

if [[ "${SYNC_ENV}" -eq 1 ]]; then
  log "同步本地 .env 到远端"
  sync_optional_file "${PROJECT_DIR}/.env" "${REMOTE_DIR}/.env"
else
  log "保留远端现有 .env"
fi

if [[ "${SYNC_HTPASSWD}" -eq 1 ]]; then
  log "同步本地 .htpasswd 到远端"
  sync_optional_file "${PROJECT_DIR}/.htpasswd" "${REMOTE_DIR}/.htpasswd"
  ssh "${REMOTE_HOST}" \
    "REMOTE_DIR='${REMOTE_DIR}' NGINX_HTPASSWD_PATH='${NGINX_HTPASSWD_PATH}' bash -s" <<'REMOTE'
set -euo pipefail
install -m 640 -o root -g www-data "${REMOTE_DIR}/.htpasswd" "${NGINX_HTPASSWD_PATH}"
REMOTE
else
  log "保留远端现有 .htpasswd"
fi

log "同步 usage-monitor 项目文件"
rsync \
  -az \
  --delete \
  --exclude '.env' \
  --exclude '.htpasswd' \
  --exclude 'data/' \
  --exclude '__pycache__/' \
  --exclude '.pytest_cache/' \
  --exclude '.DS_Store' \
  "${PROJECT_DIR}/" \
  "${REMOTE_HOST}:${REMOTE_DIR}/"

log "远端执行 docker compose 发布"
ssh "${REMOTE_HOST}" \
  "REMOTE_DIR='${REMOTE_DIR}' HEALTHCHECK_RETRIES='${HEALTHCHECK_RETRIES}' HEALTHCHECK_INTERVAL_SECONDS='${HEALTHCHECK_INTERVAL_SECONDS}' bash -s" <<'REMOTE'
set -euo pipefail
cd "${REMOTE_DIR}"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source ./.env
  set +a
fi

port="${USAGE_MONITOR_WEB_PORT:-8765}"

docker compose up -d --build
docker compose ps
for ((attempt = 1; attempt <= HEALTHCHECK_RETRIES; attempt += 1)); do
  if curl -fsS "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1; then
    echo "healthz ok"
    exit 0
  fi
  if [[ "${attempt}" -lt "${HEALTHCHECK_RETRIES}" ]]; then
    sleep "${HEALTHCHECK_INTERVAL_SECONDS}"
  fi
done

echo "healthz 检查失败：已重试 ${HEALTHCHECK_RETRIES} 次" >&2
exit 1
REMOTE

log "发布完成"
if [[ "${SYNC_ENV}" -eq 0 ]]; then
  log "提示：本次未同步远端 .env"
fi
if [[ "${SYNC_HTPASSWD}" -eq 0 ]]; then
  log "提示：本次未同步远端 .htpasswd"
fi
