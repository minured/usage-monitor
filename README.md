# usage-monitor

`usage-monitor` 是一个轻量的账号用量巡检项目，负责：

- 定时扫描指定 `tokens` 目录下的账号 JSON
- 查询 `https://chatgpt.com/backend-api/wham/usage`
- 将每个 `account_id + chatgpt_plan_type + chatgpt_user_id` 维度的最新状态写入 SQLite
- 提供一个极简运维风格页面查看总览和列表
- 页面顶部支持当前轮次进度查看、手动开始下一轮、手动安全停止本轮

当前版本只保留“最新状态”，不保存历史快照。

## 目录说明

```text
usage-monitor/
├── .env.example
├── Dockerfile
├── README.md
├── docker-compose.yml
├── scripts/
│   └── deploy.sh
├── tests/
│   └── spec_usage_monitor.py
└── usage_monitor/
    ├── collector.py
    ├── config.py
    ├── db.py
    ├── models.py
    ├── openai_api.py
    ├── timeutil.py
    ├── tokens.py
    └── web.py
```

## 状态模型

- 生命周期状态：
  - `active`
  - `invalid`
  - `source_missing`
- 主额度状态：
  - `available`
  - `exhausted`
  - `unknown`

判定规则：

- `200`：按 `rate_limit.allowed` / `rate_limit.limit_reached` 判定 `available` 或 `exhausted`
- `401`：先用 `refresh_token` 刷新并原地回写 token 文件；如果本轮最终仍是 `401`，标记 `invalid`；如果连续两轮最终都是 `401`，会把对应 JSON 剪切到 `authInvalid` 目录
- `403`：第一次记为 `active + unknown`；连续两轮都是 `403` 才标记 `invalid`
- `429`、`5xx`、超时、网络错误、解析失败：记为 `active + unknown`
- 源文件真实消失：记为 `source_missing`
- 同一路径 token 的 `chatgpt_plan_type` / `chatgpt_user_id` 变化导致旧维度失效：自动清理旧维度记录，不再计入 `source_missing`

维度补充：

- 数据库与页面统计按 `account_id + chatgpt_plan_type + chatgpt_user_id` 聚合
- 同一 `account_id` 下，不同 `chatgpt_plan_type` 会分别统计
- 同一 `account_id + chatgpt_plan_type` 下，不同 `chatgpt_user_id` 也会分别统计
- 同一三元组维度下如果存在多个 token 文件，只保留较新的那个文件参与本轮采集
- token JSON 顶层 `type` 字段当前不参与维度计算

扫描顺序补充：

- 每轮开始前会先基于数据库快照重新排序待查询账号
- 新账号或 token 文件有更新的账号会优先扫描
- 优先组按 `source_mtime_ns` 从新到旧，再按文件名排序
- 其余账号保持按文件名稳定排序

## 配置

建议先复制模板：

```bash
cp .env.example .env
```

程序、`docker compose`、`deploy.sh` 都会读取 `.env`。

### 运行配置

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `USAGE_MONITOR_TOKENS_DIR` | `../tokens` | token JSON 目录 |
| `USAGE_MONITOR_DB_PATH` | `./data/usage-monitor.sqlite3` | SQLite 路径 |
| `USAGE_MONITOR_AUTH_INVALID_DIR` | `./data/authInvalid` | 连续两轮最终 `401` 后，JSON 在当前运行环境内的剪切目录 |
| `USAGE_MONITOR_URL_PREFIX` | `""` | 页面挂载前缀，留空表示根路径 |
| `USAGE_MONITOR_TOKENS_HOST_PATH` | `../tokens` | Docker Compose 宿主机 tokens 挂载路径 |
| `USAGE_MONITOR_DATA_HOST_PATH` | `./data` | Docker Compose 宿主机数据目录 |
| `USAGE_MONITOR_AUTH_INVALID_HOST_PATH` | `./authInvalid` | Docker Compose 宿主机 authInvalid 目录 |
| `USAGE_MONITOR_PER_ACCOUNT_INTERVAL_SECONDS` | `3` | 同一轮内账号之间的间隔 |
| `USAGE_MONITOR_ROUND_INTERVAL_SECONDS` | `21600` | 整轮结束后的等待时间 |
| `USAGE_MONITOR_MANUAL_TRIGGER_POLL_SECONDS` | `2` | `sleeping` 阶段检查手动触发请求的轮询间隔 |
| `USAGE_MONITOR_SSE_POLL_SECONDS` | `0.5` | Web 端检查数据库修订号并触发 SSE 推送的间隔 |
| `USAGE_MONITOR_SSE_PING_SECONDS` | `15` | SSE 空闲保活间隔 |
| `USAGE_MONITOR_WEB_GZIP_MIN_BYTES` | `1024` | HTML / JSON 响应启用 gzip 的最小字节数 |
| `USAGE_MONITOR_REQUEST_TIMEOUT_SECONDS` | `30` | 单次 HTTP 请求超时 |
| `USAGE_MONITOR_WEB_HOST` | `127.0.0.1` | Web 监听地址 |
| `USAGE_MONITOR_WEB_PORT` | `8765` | Web 监听端口 |
| `USAGE_MONITOR_LOG_LEVEL` | `INFO` | 日志级别 |
| `HTTP_PROXY` | `""` | 可选 HTTP 代理 |
| `HTTPS_PROXY` | `""` | 可选 HTTPS 代理 |
| `NO_PROXY` | `127.0.0.1,localhost,::1` | 可选 NO_PROXY |

### 发布脚本配置

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `USAGE_MONITOR_DEPLOY_SSH_TARGET` | `""` | 发布目标 SSH 别名或主机 |
| `USAGE_MONITOR_DEPLOY_REMOTE_DIR` | `""` | 远端部署目录 |
| `USAGE_MONITOR_DEPLOY_NGINX_HTPASSWD_PATH` | `/etc/nginx/usage-monitor.htpasswd` | 远端 Nginx Basic Auth 文件路径 |
| `USAGE_MONITOR_DEPLOY_HEALTHCHECK_RETRIES` | `15` | 远端健康检查重试次数 |
| `USAGE_MONITOR_DEPLOY_HEALTHCHECK_INTERVAL_SECONDS` | `2` | 两次健康检查之间的等待秒数 |

## 本地运行

```bash
# 跑一轮采集
python -m usage_monitor.collector --once

# 持续采集
python -m usage_monitor.collector

# 启动页面
python -m usage_monitor.web
```

默认页面地址：

```text
http://127.0.0.1:8765
```

页面行为补充：

- 首页会直接内嵌全量账号快照与进度快照，默认列表首屏即可直接渲染
- 页面后续改为通过 **SSE** 接收实时推送，不再靠前端定时轮询
- 切换筛选改为前端本地过滤，不再为切换筛选重建 SSE 连接或重新拉整表
- 账号状态变化时优先推送增量补丁，减少整表重复传输与整页重绘
- HTML / JSON 默认支持 gzip，显著降低首页和列表传输体积
- 支持“手动开始扫描”和“停止本轮”，两个动作都会先二次确认
- 停止本轮采用安全停止：当前账号会先完成，本轮剩余账号不再继续

## Docker Compose

```bash
docker compose up -d --build
```

默认包含两个服务：

- `collector`
- `web`

Compose 行为：

- `collector` 读写挂载 `${USAGE_MONITOR_TOKENS_HOST_PATH}`
- `web` 和 `collector` 共享 `${USAGE_MONITOR_DATA_HOST_PATH}`
- `collector` 会把连续两轮最终 `401` 的文件移动到 `${USAGE_MONITOR_AUTH_INVALID_HOST_PATH}`
- `web` 默认仅绑定到 `127.0.0.1:${USAGE_MONITOR_WEB_PORT}`
- 如果需要挂到子路径，请把 `USAGE_MONITOR_URL_PREFIX` 设成类似 `/usage-monitor`

## 多实例部署

如果你需要多套独立实例，可以让每套实例各自维护：

- 独立的 `.env`
- 独立的 `data/`
- 独立的 `authInvalid/`
- 独立的端口 / URL 前缀

常见做法是：

- 保留一份源码仓库
- 在源码仓库外创建多个实例目录
- 每个实例目录用自己的 `docker-compose.yml` 指回这份源码作为构建上下文

这样可以做到：

- 代码只维护一份
- 部署可以有多份
- 数据彼此隔离

## 一键发布

仓库内提供发布脚本：[`./scripts/deploy.sh`](./scripts/deploy.sh)

默认流程：

1. 本地运行单元测试
2. rsync 同步项目文件到 `${USAGE_MONITOR_DEPLOY_SSH_TARGET}:${USAGE_MONITOR_DEPLOY_REMOTE_DIR}`
3. 远端执行 `docker compose up -d --build`
4. 远端轮询等待 `/healthz` 成功

常用命令：

```bash
# 默认发布，不覆盖远端 .env 和 .htpasswd
./scripts/deploy.sh

# 连同本地 .env 一起同步到远端
./scripts/deploy.sh --sync-env

# 连同本地 .env 和 .htpasswd 一起同步到远端
./scripts/deploy.sh --sync-env --sync-htpasswd
```

补充说明：

- `--sync-htpasswd` 会同步远端部署目录下的 `.htpasswd`
- 同时会安装到 `${USAGE_MONITOR_DEPLOY_NGINX_HTPASSWD_PATH}`
- 如果只改了应用代码或页面样式，通常直接执行 `./scripts/deploy.sh` 即可

## HTTP 接口

- `GET /`
  - 返回运维页面
- `GET /api/dashboard?filter=all|active|available|exhausted|unknown|invalid|source_missing`
  - 返回总览、列表 JSON，以及 `exhausted_history` 趋势点
  - `exhausted_history` 从服务上线后的账号状态变化开始记录，用于前端绘制 exhausted 数量折线图
  - Web 进程内会按 `accounts_revision` 复用已编码响应，账号数据变化后立即失效
- `GET /api/progress`
  - 返回当前采集轮次进度 JSON，供调试或外部调用使用
  - Web 进程内会按 `runtime_revision` 复用已编码响应，运行态变化后立即失效
- `GET /api/events?filter=...`
  - 返回 `text/event-stream`
  - 首次连接会立即推送 `progress` 与当前筛选下的 `dashboard`
  - 后续由服务端按数据库修订号变化实时推送
  - 列表更新优先发送 `dashboard_patch` 增量事件，只在首次连接时发送完整快照
  - 当前端已持有同一 `accounts_revision` 的全量快照时，可附带
    `skip_initial_dashboard=1&known_accounts_revision=...`，让事件流跳过首个整表快照
- `POST /api/scan`
  - 请求手动开始下一轮扫描；只会单实例串行执行，不会并发、也不会排队
- `POST /api/scan/stop`
  - 请求安全停止当前轮；当前账号会先完成，本轮剩余账号不再继续
- `GET /healthz`
  - 健康检查

## 测试

```bash
python -m unittest discover -s tests -p 'spec_*.py'
```

## License

本项目采用 MIT License。
