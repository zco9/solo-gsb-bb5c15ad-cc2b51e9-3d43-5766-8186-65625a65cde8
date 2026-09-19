# Local Webhook Relay（本地 Webhook 投递中继）

基于 **FastAPI + SQLite** 的本地 Webhook 投递中继服务：创建带签名密钥的订阅，提交事件后为每个订阅生成独立的持久化投递任务，由后台并发工作器可靠投递，失败按指数退避自动重试。

## 功能特性

- **订阅管理**：创建/查询/删除订阅（目标 URL + secret）
- **订阅暂停/恢复**：`POST /subscriptions/{id}/pause` 暂停后不再领取该订阅的新任务（已 `in_progress` 的尝试正常完成），暂停期间提交的事件照常入库并保持 `pending`；`/resume` 恢复后按原 `next_attempt_at` 与 `task_id` 顺序继续投递，不批量改写调度时间；暂停状态持久化在 SQLite 中，重启后保持
- **签名密钥平滑轮换**：`POST /subscriptions/{id}/rotate-secret` 原子切换订阅密钥；宽限期（0～86400 秒）内每次投递同时携带按新密钥生成的 `X-Webhook-Signature` 与按旧密钥生成的 `X-Webhook-Signature-Previous`，期满后只发新签名；宽限期内重复轮换返回 `409`，轮换状态持久化、重启后保持，重试按每次实际发送时的状态签名
- **按订阅投递熔断器**：同一订阅连续最终失败达到 `BREAKER_FAILURE_THRESHOLD` 次即原子熔断（`open`），熔断期间事件照常入库但工作器不再领取；冷却 `BREAKER_COOLDOWN_SECONDS` 秒后仅放行最早待投递任务作为唯一 half-open 探针——成功则关闭并清零计数，失败则重新熔断并刷新冷却时间；状态持久化在 SQLite 中，重启后保持
- **按订阅令牌桶限流**：`PUT /subscriptions/{id}/rate-policy` 为订阅设置持续速率（`rate_per_second`，令牌/秒，支持小数）与突发容量（`burst`，桶容量）；工作器每次认领在同一数据库临界区内惰性补充并原子扣取一个令牌，多工作器（含多进程）不会重复扣用同一令牌。无整令牌时队首任务保持 `pending`，其 `next_attempt_at` 被推到由持久化余额**确定性计算**出的下一令牌可用时刻（`now + (1 − 余额)/rate`），不依赖进程内定时器；被限流订阅会在同一次认领中跳过、转投其它订阅。桶容量、补充速率、余额及余额时间戳全部持久化在 SQLite 中，服务重启与多工作器并发下限流状态一致；令牌仅在队首任务（严格有序）处扣取，故绝不破坏同订阅有序投递。`GET` 查询实时余额与下一令牌时间，`DELETE` 关闭限流（桶在安装/替换策略时重置为满）
- **按订阅投递时段（delivery window）**：`PUT /subscriptions/{id}/delivery-window` 配置 IANA 时区（如 `Asia/Shanghai` / `America/New_York`）、允许投递的星期（ISO 编号 1=周一 … 7=周日，可多选）与本地起止时间（`HH:MM` 或 `HH:MM:SS`，半开区间 `[start, end)`；结束时间不晚于开始时间即**跨午夜**窗口，如 `22:00-06:00`）。工作器只在开放时段认领该订阅的队首任务；窗口关闭时队首任务保持 `pending`，其 `next_attempt_at` 在原子认领临界区内被**确定性推进到下次开放时刻**（绝对 UTC epoch；每次开放绑定到明确的 UTC 起止瞬间，按 IANA 时区库从原理上正确处理跨午夜、春令跳空与秋令重复小时——结束时刻落入重复小时时在第一次到达即关闭，第二次经过不重开），随后继续认领其它订阅。调度时间与配置均持久化在 SQLite 中，无进程内定时器，**重启不会提前投递**；窗口门控先于令牌桶判定（关闭期间不消耗令牌），并与严格有序、限流、熔断 half-open 探针、退避重试、暂停/恢复链路叠加生效。`GET` 实时返回 `is_open_now` / `next_open_at`（只读现算、不写库），`DELETE` 移除时段（不提前改写既有任务调度）
- **HMAC 受保护入站接口**：`POST /ingest` 使用 `INGEST_SECRET` 校验 `X-Webhook-Signature`（覆盖时间戳、ID 与原始 JSON），并强制 ±300 秒时间窗
- **入站幂等与防重放**：已接收的 `X-Webhook-Id` 持久化在 SQLite 中；重复 ID + 相同内容返回首次接收结果且不重复投递，相同 ID 不同内容返回 `409`
- **事件定时投递**：`POST /events` 支持可选 `deliver_at`（带时区的未来 ISO-8601 时间，如 `2026-09-20T09:00:00Z`；省略或为 `null` 表示立即投递）。计划时间持久化到任务，工作器在到点前绝不认领（无内存定时器，重启同样生效）；同订阅严格按 `task_id` 有序，因此较早的定时任务会阻塞其后的所有任务（包括立即投递的任务），直到该定时任务投递成功、最终失败或被撤销。`GET /events/{id}` 返回每个任务的 `deliver_at`；缺少时区、过去时间、非法格式均返回 `422`
- **持久化主题发布（pub/sub）**：`PUT /subscriptions/{id}/topics` 以替换语义维护订阅的去重主题集合（`GET` 查询、空列表清空）；`POST /topic-events` 提交 `event_id`、`topic`、`payload`，在**单个 SQLite 事务**中快照该主题当前全部订阅并为每个订阅生成一条普通投递任务（任务随后完全复用有序、限流、熔断、重试链路）。幂等键为 `(event_id, topic, payload)`：相同三元组重放返回**首次**收件人与任务结果（即使主题成员已变化也不新增、不漏投）；同一 `event_id` 主题不同或内容不同返回 `409`；主题无订阅者返回 `422` 且不落库；主题事件与 `POST /events`、`POST /ingest` 共享事件 ID 命名空间，跨通道复用同一 ID 互返 `409`
- **事件投递**：一次提交可指定多个订阅，每个订阅生成独立任务
- **事件级撤销**：`POST /events/{id}/cancel` 在单个 SQLite 原子事务中仅撤销尚未被工作器认领（`pending`）的投递任务；已在投递中（`in_progress`）或已结束（`delivered`/`failed`）的任务保持原结果。被撤销任务进入终态 `cancelled`：不再重试、不参与熔断计数、不阻塞同订阅后续任务；响应返回各类数量，重复撤销幂等，不存在的事件返回 `404`，撤销状态在 `GET /events/{id}` 中明确展示
- **按订阅严格有序**：同一订阅的任务按 `task_id` 升序（即提交顺序）依次投递——前一条任务处于 `pending`/`in_progress` 时后续任务不会被认领，前一条 `delivered` 或 `failed` 后才放行下一条；不同订阅之间互不影响，仍可由多个工作器并行投递
- **持久化与断点续投**：任务存储在 SQLite 中，服务重启后自动恢复未完成任务（包括崩溃时处于 `in_progress` 状态的任务），有序约束在重启后同样生效
- **并发安全**：多个工作器通过数据库级原子认领（SELECT + UPDATE 在同一临界区内）保证同一轮尝试不会被重复领取
- **可靠重试**：非 2xx、连接错误、超时均视为失败，按 **1s → 2s → 4s** 退避重试，最多 **4 次**尝试后标记为失败
- **幂等去重**：`(event_id, subscription_id)` 唯一约束，重复提交不会产生重复任务
- **HMAC-SHA256 签名**：每个投递请求携带签名头，接收方可校验来源与完整性
- **状态查询**：按事件查询各目标的尝试次数、下次执行时间、最终结果及完整尝试历史
- **投递审计查询**：`GET /audit/deliveries` 按订阅、事件、状态及 UTC 时间范围筛选投递任务，稳定游标分页（按更新时间倒序，每页最多 100 条）；`GET /audit/deliveries/{task_id}` 返回单个任务的完整尝试记录

## 快速开始

### Docker（推荐，一键启动）

```bash
docker compose up --build
```

一条命令完成镜像构建与启动：服务监听容器内 **8000** 端口，`docker-compose.yml` 通过 `"8000:8000"` 映射到宿主机同一端口，启动后访问 **http://localhost:8000**（交互式 API 文档：http://localhost:8000/docs，健康检查：http://localhost:8000/healthz）。
SQLite 数据库保存在名为 `webhook-data` 的卷（容器内 `/data/webhook_relay.db`）中，容器重启后任务不丢失。老版本数据库在启动时按 `PRAGMA user_version` 版本链**自动迁移**：每个版本的结构变更与版本号推进在同一事务内提交，多实例同时启动时由数据库写锁串行（后到实例复查版本后跳过已完成步骤）；任一步骤失败整体回滚，修复数据库文件后再次启动即从断点继续升级；迁移结束执行外键完整性检查（`PRAGMA foreign_key_check`），不通过则拒绝启动、不启动任何工作器。
熔断阈值与冷却时间等配置均可在 `docker-compose.yml` 的 `environment` 中调整（见文末配置表）。

`/ingest` 的密钥默认使用 compose 内置的开发值 `dev-ingest-secret-change-me`，生产环境请通过环境变量覆盖：

```bash
INGEST_SECRET="$(openssl rand -hex 32)" docker compose up --build
```

### 本地运行

```bash
pip install -r requirements.txt
export INGEST_SECRET="dev-ingest-secret-change-me"   # 启用 POST /ingest
uvicorn app.main:app --host 0.0.0.0 --port 8000     # 监听 http://localhost:8000
```

### 运行测试

```bash
python3 -m unittest discover -s tests -v   # 仅标准库，无需安装额外依赖；亦可用 pytest 运行
```

回归测试覆盖投递时段的 DST 边界：秋令回拨重复小时的两个 fold（跨午夜窗口在第一次到达结束时刻即关闭、第二次经过不重开）、春令跳空、星期归属，以及工作器原子认领与令牌桶限流的先后顺序。

### 一分钟冒烟（端口 8000）

```bash
# 1) 建订阅  2) 提交事件  3) 撤销尚未投递的任务  4) 查询撤销后的状态
SID=$(curl -s -X POST http://localhost:8000/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"target_url": "http://localhost:9001/hook", "secret": "my-secret"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')

curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d "{\"event_id\": \"evt-demo\", \"subscription_ids\": [\"$SID\"], \"payload\": {\"msg\": \"hi\"}}"

curl -s -X POST http://localhost:8000/events/evt-demo/cancel
curl -s http://localhost:8000/events/evt-demo

# 定时投递：60 秒后再投（Z = UTC；必须带时区且为未来时间）
LATER=$(date -u -d '+60 seconds' +%Y-%m-%dT%H:%M:%SZ)
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d "{\"event_id\": \"evt-later\", \"subscription_ids\": [\"$SID\"], \"payload\": {\"msg\": \"later\"}, \"deliver_at\": \"$LATER\"}"
curl -s http://localhost:8000/events/evt-later   # status=pending，deliver_at 为计划时间

# 按订阅限流：持续 1 次/秒、突发 1；查看实时令牌余额；关闭限流
curl -s -X PUT http://localhost:8000/subscriptions/$SID/rate-policy \
  -H 'Content-Type: application/json' -d '{"rate_per_second": 1, "burst": 1}'
curl -s http://localhost:8000/subscriptions/$SID/rate-policy
curl -s -X DELETE http://localhost:8000/subscriptions/$SID/rate-policy -o /dev/null -w '%{http_code}\n'

# 投递时段：仅工作日（周一至周五）纽约时间 09:00-18:00 允许投递
curl -s -X PUT http://localhost:8000/subscriptions/$SID/delivery-window \
  -H 'Content-Type: application/json' \
  -d '{"timezone": "America/New_York", "weekdays": [1,2,3,4,5], "local_start": "09:00", "local_end": "18:00"}'
curl -s http://localhost:8000/subscriptions/$SID/delivery-window
curl -s -X DELETE http://localhost:8000/subscriptions/$SID/delivery-window -o /dev/null -w '%{http_code}\n'

# 主题发布：订阅加入主题 → 向主题发布 → 重放返回首次快照（即使成员变化）
curl -s -X PUT http://localhost:8000/subscriptions/$SID/topics \
  -H 'Content-Type: application/json' -d '{"topics": ["orders", "billing"]}'
curl -s http://localhost:8000/subscriptions/$SID/topics
curl -s -X POST http://localhost:8000/topic-events \
  -H 'Content-Type: application/json' \
  -d '{"event_id": "evt-topic-1", "topic": "orders", "payload": {"order_id": 42}}'
```

## API 使用示例

### 1. 创建订阅

```bash
curl -s -X POST http://localhost:8000/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"target_url": "http://localhost:9001/hook", "secret": "my-secret"}'
```

```json
{"id": "44a79d7077ce", "target_url": "http://localhost:9001/hook", "created_at": "2026-09-17T11:11:24+00:00", "paused": false, "paused_at": null, "rotating": false, "previous_secret_expires_at": null, "state": "closed", "consecutive_failures": 0, "next_probe_at": null, "rate_per_second": null, "burst": null}
```

### 2. 提交入站事件（HMAC 保护，POST /ingest）

`/ingest` 面向外部事件源，使用共享密钥 `INGEST_SECRET` 做 HMAC-SHA256 认证。
每个请求必须携带三个头部：

| 头部 | 说明 |
|------|------|
| `X-Webhook-Timestamp` | 发送时刻 Unix 时间戳（秒），与服务端时差不得超过 **±300 秒** |
| `X-Webhook-Id` | 事件唯一 ID（最长 128 字符），用于幂等去重 |
| `X-Webhook-Signature` | `sha256=<HMAC-SHA256(secret, timestamp + "." + id + "." + 原始请求体)>` |

请求体为原始 JSON，**必须**包含 `subscription_ids`（非空数组，1–100 个），其余字段原样转发给投递目标：

```bash
SECRET="dev-ingest-secret-change-me"
TS=$(date +%s)
ID="evt-ingest-001"
BODY='{"subscription_ids": ["44a79d7077ce"], "type": "order.paid", "order_id": 42}'
SIG="sha256=$(printf '%s' "${TS}.${ID}.${BODY}" | openssl dgst -sha256 -hmac "$SECRET" | sed 's/^.*= //')"

curl -s -X POST http://localhost:8000/ingest \
  -H 'Content-Type: application/json' \
  -H "X-Webhook-Timestamp: $TS" \
  -H "X-Webhook-Id: $ID" \
  -H "X-Webhook-Signature: $SIG" \
  -d "$BODY"
```

Python 生成签名示例：

```python
import hashlib, hmac, time

secret = "dev-ingest-secret-change-me"
ts = str(int(time.time()))
webhook_id = "evt-ingest-001"
body = b'{"subscription_ids": ["44a79d7077ce"], "type": "order.paid"}'
signed = ts.encode() + b"." + webhook_id.encode() + b"." + body
sig = "sha256=" + hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
```

验签与幂等行为：

- **首次验证成功**：`201 Created`，按 `subscription_ids` 创建投递任务，响应与 `POST /events` 相同（`created` / `duplicates`）。
- **重复 ID + 相同原始 body**：`200 OK`，返回与首次完全一致的接收结果，**不会重复创建投递任务**（含并发重试：多个相同请求同时到达也只有一个 `201`）。
- **相同 ID 但 body 不同**：`409 Conflict`，不产生任何任务。
- **签名错误**（含签名未覆盖时间戳/ID/原始 body）：`401 Unauthorized`。
- **时间戳超出 ±300 秒窗口**或无法解析：`401 Unauthorized`。
- **缺少必需头部**、空 body、body 非 JSON：`400 Bad Request`；body 缺少 `subscription_ids`：`422`。
- **包含不存在的订阅 ID**：`400 Bad Request`，且该事件不会持久化。
- 服务端未配置 `INGEST_SECRET` 时端点失败关闭，返回 `503`。

入站事件的 `X-Webhook-Id` 即事件 ID，可用 `GET /events/{id}` 查询投递状态；投递给订阅目标的出站请求体就是 `/ingest` 收到的**原始 JSON**（出站签名使用该订阅自己的 secret，见下文）。

### 3. 提交事件（内部接口 POST /events）

```bash
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{
        "event_id": "evt-001",
        "subscription_ids": ["44a79d7077ce", "94d3c624dad2"],
        "payload": {"msg": "hello", "n": 1}
      }'
```

```json
{
  "event_id": "evt-001",
  "created": [
    {"task_id": 1, "subscription_id": "44a79d7077ce", "status": "pending", "deliver_at": null},
    {"task_id": 2, "subscription_id": "94d3c624dad2", "status": "pending", "deliver_at": null}
  ],
  "duplicates": []
}
```

#### 定时投递（deliver_at）

请求体可选 `deliver_at` 字段指定投递时间：

```bash
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{
        "event_id": "evt-scheduled",
        "subscription_ids": ["44a79d7077ce"],
        "payload": {"msg": "happy new year"},
        "deliver_at": "2027-01-01T00:00:00Z"
      }'
```

```json
{
  "event_id": "evt-scheduled",
  "created": [
    {"task_id": 1, "subscription_id": "44a79d7077ce", "status": "pending",
     "deliver_at": "2027-01-01T00:00:00+00:00"}
  ],
  "duplicates": []
}
```

行为约定：

- **字段格式**：`deliver_at` 必须是**带显式时区**的未来 ISO-8601 时间——`Z` 结尾的 UTC 时间（`2027-01-01T00:00:00Z`）或带偏移量的时间（`2027-01-01T08:00:00+08:00`）均可；省略或显式传 `null` 表示立即投递。
- **校验错误返回 `422`**：过去时间（含恰好等于当前时间）、无时区的朴素时间（`2027-01-01T00:00:00`）、只有日期（`2027-01-01`）、无法解析的字符串或非字符串类型，均返回 `422 Unprocessable Entity`，事件与任务均不会入库。
- **到点前不认领**：计划时间以 epoch 秒持久化到任务并作为任务的首次到期时间；工作器的认领查询只选择到期任务，因此定时任务在 `deliver_at` 之前始终保持 `pending`，任何工作器（含服务重启后的工作器）都不会提前投递。调度不依赖进程内定时器，服务在到点前重启也不会提前或丢失投递，到点后按原计划执行。
- **定时任务阻塞后续任务**：定时任务等待期间仍是 `pending`，按订阅严格有序规则会**阻塞同订阅的所有后续任务**——即使后续事件是立即投递（无 `deliver_at`）的，也要等这条定时任务到达终态（`delivered` / `failed` / `cancelled`）后才会被认领；不同订阅互不影响，仍并行投递。
- **失败重试与撤销**：到点后的投递失败按普通退避规则重试（1s → 2s → 4s）；定时任务在到点前可以随事件被 `POST /events/{id}/cancel` 撤销（与普通 `pending` 任务一样），撤销后立即解除对后续任务的阻塞。
- **暂停与熔断**：订阅暂停或熔断期间到点的定时任务继续等待，恢复/熔断放行后按原 `deliver_at`（已过则立即）与 `task_id` 顺序投递。
- **幂等**：重复提交相同 `event_id` + 订阅返回 `200`，已存在的任务出现在 `duplicates` 中并保留其**首次提交时的计划时间**，重复请求中的 `deliver_at` 不会改写调度；相同 `event_id` 但 payload 不同返回 `409`；包含不存在的订阅 ID 返回 `400`。
- **状态可见**：`POST /events` 的 `created` / `duplicates` 条目与 `GET /events/{id}` 的每个 target 都带 `deliver_at` 字段（ISO 时间，立即投递的任务为 `null`）。

### 4. 查询事件状态

```bash
curl -s http://localhost:8000/events/evt-001
```

```json
{
  "event_id": "evt-001",
  "created_at": "2026-09-17T11:11:24+00:00",
  "targets": [
    {
      "task_id": 2,
      "subscription_id": "94d3c624dad2",
      "target_url": "http://localhost:9002/hook",
      "status": "delivered",
      "attempts": 3,
      "max_attempts": 4,
      "deliver_at": null,
      "next_attempt_at": null,
      "last_status_code": 200,
      "last_error": null,
      "result": "delivered",
      "history": [
        {"attempt_no": 1, "status_code": 500, "error": "non-2xx response: 500", ...},
        {"attempt_no": 2, "status_code": 500, "error": "non-2xx response: 500", ...},
        {"attempt_no": 3, "status_code": 200, "error": null, ...}
      ]
    }
  ]
}
```

`status` 取值：`pending`（等待执行，含定时未到点）/ `in_progress`（投递中）/ `delivered`（成功）/ `failed`（最终失败）/ `cancelled`（随事件撤销，从未投递）。
`deliver_at` 是提交时请求的计划投递时间（ISO 时间）；立即投递的任务为 `null`，定时任务即使已投递/失败/撤销也保留该值。
`next_attempt_at` 仅在 `pending` 时有值，即下次执行时间——定时任务在到点前它等于 `deliver_at`，失败退避期间为重试到期时间。

事件是否被撤销过在顶层明确展示：`cancelled`（布尔，只要有一个任务为 `cancelled` 即为 `true`）与 `cancelled_at`（最近一次实际撤销动作的 ISO 时间，从未撤销为 `null`）；`counts` 给出该事件各状态任务数量（`pending` / `in_progress` / `delivered` / `failed` / `cancelled`），被撤销的任务还带有各自的 `cancelled_at`：

```json
{
  "event_id": "evt-001",
  "created_at": "2026-09-17T11:11:24+00:00",
  "cancelled": true,
  "cancelled_at": "2026-09-18T09:00:00+00:00",
  "counts": {"pending": 0, "in_progress": 0, "delivered": 1, "failed": 0, "cancelled": 1},
  "targets": [
    {"task_id": 1, "subscription_id": "44a79d7077ce", "status": "delivered", "cancelled_at": null, ...},
    {"task_id": 2, "subscription_id": "94d3c624dad2", "status": "cancelled", "cancelled_at": "2026-09-18T09:00:00+00:00", ...}
  ]
}
```

注意：若同一订阅存在更早的未完成任务，后续任务会因有序调度而保持 `pending`（被前序任务阻塞），状态查询会如实展示这些被阻塞的任务。定时投递（`deliver_at`）是最常见的阻塞来源：一条尚未到点的定时任务会阻塞同订阅的全部后续任务（包括立即投递的任务）。

### 5. 撤销事件投递（POST /events/{id}/cancel）

撤销一个事件**尚未被工作器认领**的投递任务：

```bash
curl -s -X POST http://localhost:8000/events/evt-001/cancel
```

```json
{
  "event_id": "evt-001",
  "cancelled": 1,
  "cancelled_at": "2026-09-18T09:00:00+00:00",
  "counts": {"pending": 0, "in_progress": 1, "delivered": 0, "failed": 0, "cancelled": 1}
}
```

行为约定：

- **原子只撤未认领任务**：撤销在单个 SQLite 事务中完成，与工作器的认领操作互斥；只有撤销瞬间仍为 `pending` 的任务会被置为终态 `cancelled`（包括退避等待重试中的任务）。已被工作器认领（`in_progress`）的 HTTP 投递不会被中断，正常完成并记录真实结果；已结束的 `delivered` / `failed` 任务结果保持不变。
- **返回各类数量**：响应的 `counts` 始终包含全部五种状态的任务数量；`cancelled`（顶层字段）是**本次调用新撤销**的任务数（首次为实际数量，重复调用为 `0`，可用于判断是否幂等命中）。
- **不重试、不计熔断**：`cancelled` 是终态，工作器认领查询只选择 `pending`，故撤销后的任务永远不会再被投递或重试；撤销不经过终态记账逻辑，既不增加也不清零任何订阅熔断器计数。
- **不阻塞同订阅后续任务**：按订阅有序投递只把 `pending` / `in_progress` 的较早任务视为阻塞项，`cancelled` 立即成为终态，同订阅的下一条任务即刻可被认领。
- **幂等**：对同一事件重复调用返回 `200`；若期间没有新的待撤销任务，`cancelled` 为 `0`、`counts` 保持稳定（撤销期间在投递的任务可能恰好结束，其结果如实体现在 `counts` 中，已撤销任务不会变化）。
- **404**：撤销或查询不存在的事件均返回 `404 Not Found`（仅存在于 `ingested_events` 但未建事件的情况不会发生：验签通过的入站事件必同时写入 `events`）。
- **状态可见**：撤销后 `GET /events/{id}` 顶层 `cancelled=true` 并给出事件级 `cancelled_at` 与 `counts`，每个被撤销任务的 `status` 为 `cancelled` 且带有任务级 `cancelled_at`。
- **与暂停的区别**：暂停（`/subscriptions/{id}/pause`）作用于订阅、保留所有任务待恢复后继续投递；撤销作用于单个事件、把该事件的待投递任务永久终止。二者可以组合：暂停期间撤销事件，恢复后该事件的被撤销任务不会补投。
- **重启恢复边界**：服务崩溃时残留的 `in_progress` 任务会在启动时恢复为 `pending`（视作尚未认领），因此恢复后对其事件再次撤销可以将其撤销；未撤销则照常重新投递。

### 6. 暂停与恢复订阅投递

```bash
curl -s -X POST http://localhost:8000/subscriptions/44a79d7077ce/pause
```

```json
{"id": "44a79d7077ce", "target_url": "http://localhost:9001/hook", "created_at": "2026-09-17T11:11:24+00:00", "paused": true, "paused_at": "2026-09-18T08:30:00+00:00", "rotating": false, "previous_secret_expires_at": null, "state": "closed", "consecutive_failures": 0, "next_probe_at": null, "rate_per_second": null, "burst": null}
```

```bash
curl -s -X POST http://localhost:8000/subscriptions/44a79d7077ce/resume
```

行为约定：

- **暂停生效边界**：暂停在任务认领时生效——已提交暂停后，工作器不再领取该订阅的任何新任务；暂停前已被认领（`in_progress`）的投递会正常执行完成，暂停不会中断进行中的 HTTP 请求。
- **暂停期间继续入库**：通过 `POST /events` 或 `POST /ingest` 提交的事件照常持久化，任务保持 `pending`（含因退避等待重试的任务，其 `next_attempt_at` 不变），可在 `GET /events/{id}` 中查询到。
- **恢复语义**：恢复只清除订阅上的暂停标记，**不批量改写**任何任务的 `next_attempt_at`；工作器按原到期时间、并在同一订阅内按 `task_id` 严格顺序继续领取——到期时间已过的任务立即被领取，未到期的仍在原时间执行。
- **幂等**：重复暂停/恢复是幂等操作，均返回 `200` 与当前订阅状态（重复暂停保留首次的 `paused_at`）；订阅不存在时返回 `404`。
- **状态可见**：`GET /subscriptions` 与 `GET /subscriptions/{id}` 返回 `paused`（布尔）与 `paused_at`（ISO 时间或 `null`）。
- **持久化**：暂停状态存储在 SQLite 的 `subscriptions.paused_at` 列中，服务（或容器）重启后保持。

### 7. 轮换订阅签名密钥

```bash
curl -s -X POST http://localhost:8000/subscriptions/44a79d7077ce/rotate-secret \
  -H 'Content-Type: application/json' \
  -d '{"new_secret": "my-new-secret", "grace_seconds": 3600}'
```

```json
{"id": "44a79d7077ce", "target_url": "http://localhost:9001/hook", "created_at": "2026-09-17T11:11:24+00:00", "paused": false, "paused_at": null, "rotating": true, "previous_secret_expires_at": "2026-09-18T10:30:00+00:00", "state": "closed", "consecutive_failures": 0, "next_probe_at": null, "rate_per_second": null, "burst": null}
```

行为约定：

- **原子切换**：新密钥与旧密钥失效时间在同一个 SQLite 事务中原子落库；自轮换成功起，新投递的 `X-Webhook-Signature` 立即按新密钥生成。
- **宽限期双签名**：`grace_seconds`（0～86400 秒）内，每次出站投递同时携带 `X-Webhook-Signature`（新密钥）与 `X-Webhook-Signature-Previous`（旧密钥）两个签名头；宽限期满后只发送新签名，不再携带 `-Previous` 头。`grace_seconds=0` 表示立即切换，从不双签。
- **宽限期内禁止再次轮换**：上一次轮换的宽限期未结束时再次调用返回 `409 Conflict`（响应中包含当前宽限期截止时间）；期满后可再次轮换，已被淘汰的旧密钥随之丢弃。
- **按发送时状态签名**：密钥状态在每次任务认领（即每次实际发送，含每次重试）时重新读取，宽限期是否结束也在构造请求头时判定——重试中的任务在轮换后自动改用新密钥（宽限期内附带旧签名），宽限期内到期的任务后续尝试只发新签名。
- **持久化**：轮换状态（当前密钥、旧密钥及其失效时间）存储在 SQLite 的 `subscriptions` 表中，服务（或容器）重启后宽限期继续按原截止时间生效。
- **错误码**：订阅不存在返回 `404`；`new_secret` 为空或 `grace_seconds` 超出 0～86400 返回 `422`。
- **密钥保密**：任何查询响应（含本端点响应、`GET /subscriptions*`）都不会包含密钥明文，日志也不记录密钥；响应仅暴露轮换状态 `rotating`（布尔）与 `previous_secret_expires_at`（宽限期截止时间，非轮换期则为 `null`）。

**接收方切换流程**（零中断换密钥）：

1. **接收方先支持双头验签**：验签逻辑改为——`X-Webhook-Signature-Previous` 头存在时，接受任一签名通过（新密钥验 `X-Webhook-Signature`，旧密钥验 `X-Webhook-Signature-Previous`）。
2. **发起轮换**：调用 `POST /subscriptions/{id}/rotate-secret`，`grace_seconds` 取足以完成接收方部署的窗口（如 3600 秒）。
3. **宽限期内更新接收方配置**：将接收方配置的密钥换成新密钥。期间投递始终双签，接收方无论仍用旧密钥还是已切到新密钥都能验签通过，投递不中断。
4. **宽限期满**：中继自动停止发送 `X-Webhook-Signature-Previous`，只发新密钥签名；接收方可移除旧密钥与双头兼容逻辑。可用 `GET /subscriptions/{id}` 观察 `rotating` 变回 `false` 确认窗口已结束。

### 8. 投递熔断器（按订阅）

每个订阅自带一个持久化熔断器，避免对持续故障的目标无休止重试。熔断器状态随订阅查询接口返回：

```bash
curl -s http://localhost:8000/subscriptions/44a79d7077ce
```

```json
{"id": "44a79d7077ce", "target_url": "http://localhost:9001/hook", "created_at": "2026-09-17T11:11:24+00:00", "paused": false, "paused_at": null, "rotating": false, "previous_secret_expires_at": null, "state": "open", "consecutive_failures": 5, "next_probe_at": "2026-09-18T09:31:00+00:00", "rate_per_second": 2.0, "burst": 2.0}
```

三个熔断器字段（`GET /subscriptions`、`GET /subscriptions/{id}` 及各订阅写操作响应均包含）：

| 字段 | 说明 |
|------|------|
| `state` | `closed`（正常投递）/ `open`（熔断中，暂停领取）/ `half_open`（探针投递中） |
| `consecutive_failures` | 当前连续最终失败任务数（成功投递即清零） |
| `next_probe_at` | 仅 `open` 时有值：冷却到期、允许下一次探针的最早时间（ISO 时间） |

状态机行为约定：

- **累计失败（closed）**：某订阅的任务重试耗尽、最终失败（`failed`）时，该订阅的 `consecutive_failures` 加一；任意一次投递成功都会将其清零（只统计**连续**失败）。
- **原子熔断（closed → open）**：连续失败数达到 `BREAKER_FAILURE_THRESHOLD`（默认 5）时，计数更新与状态切换在同一个 SQLite 事务中原子落库，不会出现计数与状态不一致的中间态。
- **熔断期间（open）**：`POST /events` / `POST /ingest` 照常接收该订阅的事件并持久化为 `pending` 任务，但工作器不会领取该订阅的任何任务；`next_probe_at` 显示冷却到期时间。
- **唯一探针（open → half_open）**：冷却到期（`now >= next_probe_at`）后，该订阅**最早的待投递任务**成为唯一 half-open 探针——状态翻转与任务认领在同一原子临界区内完成，多个工作器并发也只会有一个探针；探针的重试按正常退避进行，期间该订阅的其它任务继续等待。
- **探针成功（half_open → closed）**：探针任务投递成功，熔断器关闭、`consecutive_failures` 清零，积压任务按原顺序恢复投递。
- **探针失败（half_open → open）**：探针任务最终失败，熔断器立即重新打开，并以失败时刻为起点刷新冷却时间（`next_probe_at = 失败时刻 + BREAKER_COOLDOWN_SECONDS`）。
- **持久化**：`state`、`consecutive_failures`、`next_probe_at` 及探针归属全部存储在 SQLite 的 `subscriptions` 表中，服务（或容器）重启后熔断状态、冷却截止时间与探针唯一性继续生效。

### 9. 按订阅令牌桶限流

为单个订阅设置投递速率上限，避免压垮目标。限流状态随订阅查询接口一并返回，也可用专用端点查看实时桶状态：

```bash
# 持续 2 次/秒，突发容量 2（省略 burst 时默认等于“一秒的令牌量”，至少为 1）
curl -s -X PUT http://localhost:8000/subscriptions/44a79d7077ce/rate-policy \
  -H 'Content-Type: application/json' \
  -d '{"rate_per_second": 2, "burst": 2}'
```

```json
{
  "enabled": true,
  "rate_per_second": 2.0,
  "burst": 2.0,
  "tokens_available": 2.0,
  "next_token_at": null
}
```

```bash
# 查询实时桶状态（余额按查询时刻惰性补充计算，只读不改库）
curl -s http://localhost:8000/subscriptions/44a79d7077ce/rate-policy

# 关闭限流
curl -s -X DELETE http://localhost:8000/subscriptions/44a79d7077ce/rate-policy -o /dev/null -w '%{http_code}\n'
# 204
```

请求体与响应字段：

| 字段 | 说明 |
|------|------|
| `rate_per_second` | 请求字段：持续速率，每秒补充的令牌数；**支持小数**（如 `0.5` 表示每 2 秒 1 次），范围 `(0, 1000000]` |
| `burst` | 请求字段：令牌桶容量，即允许背靠背突发的最大投递数，范围 `[1, 1000000]`；省略时取 `max(1, rate_per_second)` |
| `enabled` | 响应字段：是否已配置限流；未配置时为 `false`，其余数值字段均为 `null` |
| `tokens_available` | 响应字段：按查询时刻惰性补充后的实时余额（只读展示，不写库），上限为 `burst` |
| `next_token_at` | 响应字段：下一个整令牌可用的最早时间（ISO 时间）；已有整令牌或未配置限流时为 `null` |

状态机行为约定：

- **安装/替换即重置满桶**：`PUT` 成功后桶立即拥有 `burst` 个令牌（余额时间戳记为当前时刻），因此刚配置完的订阅可以立刻背靠背投递最多 `burst` 次。替换已有策略同样重置为满桶。
- **认领时原子扣取**：工作器每实际认领一个任务，就在认领的同一个数据库临界区（`BEGIN IMMEDIATE` 写事务 + 进程锁）内完成「惰性按墙钟补充 → 判断 → 扣 1 令牌 → 持久化余额 → 任务置 `in_progress`」。多个工作器即使分属不同进程、并发打到同一个数据库文件，也会被 SQLite 写锁串行化，绝不会重复扣用同一个令牌或领取同一任务。
- **无令牌时确定性再调度**：桶中不足一个整令牌时，队首任务**不被认领**、保持 `pending`，其 `next_attempt_at` 被原子推到下一个令牌的可用时刻 `now + (1 − 当前余额)/rate_per_second`；该时刻是持久化余额的纯函数，任何工作器重算结果都相同。不使用任何进程内定时器，因此该调度在服务重启后同样生效。被限流的订阅在同一次认领调用内只重调度一次并被跳过，工作器随后转而认领**其它订阅**的到期任务——单个被限流订阅不会空占工作器。
- **不破坏同订阅有序投递**：令牌只在订阅的队首任务（按严格有序约束本就只有队首可被认领）处检查与扣取；队首因无令牌保持 `pending` 时，后续任务本就被有序 `NOT EXISTS` 条件阻塞，队首到点放行后顺序自然延续，令牌限流不会重排或跳过任何任务。
- **与重试/定时/暂停/熔断叠加**：每次实际发送尝试（含每次退避重试）都消耗一个令牌；定时任务到点后同样受令牌约束；暂停期间不补充也不扣取（按墙钟在下次认领时统一惰性补充）；熔断 open 期间不进入令牌判断，冷却后的唯一 half-open 探针也消耗一个令牌。
- **重启与多工作器一致**：容量、速率、余额与余额时间戳持久化在 `subscriptions` 表（`rate_capacity` / `rate_refill_per_second` / `rate_tokens` / `rate_updated_at`），启动无需重建内存状态；重启后余额按持久化时间戳继续补充，多工作器看到的始终是同一份余额。老数据库由启动迁移自动 `ALTER TABLE` 补齐四列，存量订阅默认不限流。
- **关闭限流**：`DELETE` 清空桶配置（`204`）；此前因等令牌而延后的任务按其已排定的时间醒来，之后不再做令牌检查。重复 `DELETE` 或对未配置策略的订阅调用返回 `404`；订阅不存在时 `GET`/`PUT`/`DELETE` 均返回 `404`。
- **校验错误**：`rate_per_second ≤ 0`、`burst < 1`、超出上限均返回 `422`。
- **订阅视图**：`GET /subscriptions` 与 `GET /subscriptions/{id}` 的每个订阅都带 `rate_per_second` 与 `burst`（未配置时为 `null`）；实时余额请通过专用 `GET …/rate-policy` 查询。

### 10. 按订阅投递时段（delivery window）

为单个订阅设置“只在这些时间投递”的开放时段，窗口外事件照常入库但工作器不认领，并把队首任务确定性地排到下次开放时刻。

```bash
# 工作日（周一=1 … 周五=5）纽约时间 09:00-18:00 开放
curl -s -X PUT http://localhost:8000/subscriptions/44a79d7077ce/delivery-window \
  -H 'Content-Type: application/json' \
  -d '{"timezone": "America/New_York", "weekdays": [1,2,3,4,5], "local_start": "09:00", "local_end": "18:00"}'
```

```json
{
  "enabled": true,
  "timezone": "America/New_York",
  "weekdays": [1, 2, 3, 4, 5],
  "local_start": "09:00:00",
  "local_end": "18:00:00",
  "wraps_past_midnight": false,
  "is_open_now": false,
  "next_open_at": "2026-09-21T13:00:00+00:00",
  "updated_at": "2026-09-19T05:02:32.259284+00:00"
}
```

```bash
# 查询（is_open_now / next_open_at 按查询时刻在 IANA 时区中现算，只读不改库）
curl -s http://localhost:8000/subscriptions/44a79d7077ce/delivery-window

# 跨午夜窗口：每天 22:00 到次日 06:00 开放（结束不晚于开始即自动按跨午夜处理）
curl -s -X PUT http://localhost:8000/subscriptions/44a79d7077ce/delivery-window \
  -H 'Content-Type: application/json' \
  -d '{"timezone": "Asia/Shanghai", "weekdays": [1,2,3,4,5,6,7], "local_start": "22:00", "local_end": "06:00"}'

# 仅周末（周六=6、周日=7）
curl -s -X PUT http://localhost:8000/subscriptions/44a79d7077ce/delivery-window \
  -H 'Content-Type: application/json' \
  -d '{"timezone": "UTC", "weekdays": [6,7], "local_start": "00:00", "local_end": "23:59:59"}'

# 移除时段（恢复全天可投递）
curl -s -X DELETE http://localhost:8000/subscriptions/44a79d7077ce/delivery-window -o /dev/null -w '%{http_code}\n'
# 204
```

未配置时段时 `GET` 返回 `enabled: false`（其余配置字段为 `null`，`is_open_now` 为 `true` 表示“没有任何限制”）：

```json
{"enabled": false, "timezone": null, "weekdays": null, "local_start": null,
 "local_end": null, "wraps_past_midnight": null, "is_open_now": true,
 "next_open_at": null, "updated_at": null}
```

请求体与响应字段：

| 字段 | 说明 |
|------|------|
| `timezone` | 请求字段：IANA 时区名（如 `Europe/Berlin`、`America/New_York`、`Asia/Shanghai`、`Pacific/Chatham`），星期与起止时间都按此时区的本地挂钟解释；无法识别的时区名返回 `422` |
| `weekdays` | 请求字段：允许开放的 ISO 星期编号数组，`1`=周一 … `7`=周日；非空、1～7、**不可重复**，顺序任意（响应按升序回显） |
| `local_start` / `local_end` | 请求字段：本地起止时间，接受 `HH:MM` 或 `HH:MM:SS`（响应统一规范化为 `HH:MM:SS`）；区间为半开 `[start, end)`，秒与分必须在合法范围内 |
| `wraps_past_midnight` | 响应字段：`local_end <= local_start` 时为 `true`，表示窗口从开始时间跨过午夜直到次日结束时间；如 `22:00-06:00`、`00:00-00:00`（该星期全天开放） |
| `is_open_now` | 响应字段：按查询时刻在配置时区中现算的开放状态（只读，不写库）；未配置时段时为 `true` |
| `next_open_at` | 响应字段：当前关闭时，下次开放的最早时刻（ISO 时间，UTC 偏移）；当前开放或未配置时段时为 `null` |
| `updated_at` | 响应字段：时段安装/替换的时间（ISO 时间）；未配置时为 `null` |

调度行为约定：

- **只在开放时段认领队首**：窗口判定与任务认领、令牌扣减、熔断 half-open 翻转处在**同一个 `BEGIN IMMEDIATE` 数据库临界区**内。开放即正常认领（随后照常扣令牌、发 HTTP）；关闭则队首任务**不被认领**、保持 `pending`，其 `next_attempt_at` 被原子更新为该窗口的**下次开放时刻**，同一次认领调用随即转投其它订阅的到期任务——单个处于关闭时段的订阅不会空占工作器（实现上与限流共用 `deferred_sids` 机制，每次认领对同一订阅至多重调度一次）。
- **下次开放时刻是确定性的**：它是“当前 epoch 时刻 + 持久化窗口配置”的纯函数，任何工作器（含独立进程）重算结果一致；结果以**绝对 UTC epoch 秒**持久化在任务行，不使用任何进程内定时器。
- **重启不得提前投递**：窗口外任务的到期时间已经是未来的绝对 UTC 时刻，重启（含崩溃恢复把 `in_progress` 重置为 `pending`）后认领查询的 `next_attempt_at <= now` 条件天然阻止提前投递；只有墙上时钟真正走到开放时刻才会被认领。窗口配置（时区、星期、起止、安装时间）同样持久化，重启后无需任何内存重建。
- **跨午夜窗口**：`local_end <= local_start`（含 `00:00` 结束）表示从前一个允许星期的 `local_start` 开放、持续到次日 `local_end` 关闭；例如每天 `22:00-06:00`，周五晚的窗口覆盖周六凌晨。`00:00-00:00` 表示该星期从午夜到午夜全天开放。
- **夏令时正确处理**（基于系统/IANA 时区数据库，容器镜像通过 `tzdata` 包内置完整库）：每次开放被绑定为一条明确的 UTC 半开区间——开放瞬间是允许星期当日 `local_start` 的真实时刻，关闭瞬间是同一（跨午夜时次日）`local_end` 的真实时刻，开放判定与调度都只比较绝对瞬间。春令跳空（如纽约 02:00→03:00 不存在的小时）天然不在窗口内；配置的起止时刻恰好落在跳空小时内时，解析为跳空后的**第一个真实时刻**（挂钟跳过了配置时刻），整段窗口都落入跳空小时时当天不开放；落在秋令重复小时内的起止时刻取**较早的那次**（`fold=0`）：窗口随第一次经过即开放，跨午夜窗口（如纽约周六 23:00—周日 01:30）的结束时刻落入重复小时时，在**第一次**（真实的）结束时刻即关闭，第二次经过同一挂钟时段（第二次 01:00—01:30）不会重新开放或延长。星期/周末跨 DST 的日期推进（如纽约周五晚 → 周一 10:00 EDT）也按真实偏移计算。
- **与严格有序叠加**：窗口只在订阅的**队首任务**处判定（严格有序约束保证候选恒为队首）；队首被推到下次开放期间，同订阅后续任务（包括立即到期的）与定时任务一样被有序 `NOT EXISTS` 条件阻塞，队首到点放行后顺序自然延续；撤销队首后，下一条任务成为新队首并按同一窗口判定。不同订阅互不影响、继续并行投递。
- **与限流叠加**：窗口门控**先于**令牌桶——窗口关闭时不扣令牌、也不写令牌余额；开放时认领才消耗令牌。因此“窗口开放但令牌不足”会把队首排到“下一令牌时刻”，“窗口关闭”排到“下次开放时刻”，两者都在同一临界区内确定性推进。
- **与熔断/暂停/重试叠加**：熔断 open 冷却未到、订阅暂停时任务在进入窗口判定之前即被跳过（调度时间不被窗口逻辑改动）；冷却后的唯一 half-open 探针同样受窗口约束，只在开放时刻被认领。失败退避到期的重试若落在窗口外，会被改排到下次开放时刻（而不是按退避立即重试）；落在开放内则照常重试。
- **安装/替换/移除不批量改写任务**：与限流策略一致，`PUT` 只持久化新配置（并记录 `updated_at`），`DELETE` 只清空配置列，均**不批量改写**既有任务的 `next_attempt_at`。已被旧窗口推到未来的任务保持其已持久化的到期时间（绝不提前）；到期醒来或工作器再次轮询时按**新窗口**判定，若新窗口已开放则认领，否则改排到新窗口的下次开放。
- **错误码**：时区名无法识别、星期为空/越界/重复、时间格式非法或超出 `00:00:00-23:59:59`、起止时间相同，均返回 `422` 且不写库；订阅不存在时 `PUT`/`GET`/`DELETE` 均返回 `404`；对未配置时段的订阅调用 `DELETE` 返回 `404`（与限流端点一致）。

### 11. 持久化主题发布（pub/sub）

主题是持久化的订阅分组：先让订阅加入主题，再向主题发布事件，中继会在发布瞬间快照主题成员并为每个成员生成一条普通投递任务。

#### 维护订阅的主题集合（PUT / GET /subscriptions/{id}/topics）

```bash
# 替换语义：整体替换该订阅的主题集合（自动 trim、去重；空列表 = 退出全部主题）
curl -s -X PUT http://localhost:8000/subscriptions/44a79d7077ce/topics \
  -H 'Content-Type: application/json' \
  -d '{"topics": ["orders", "billing", "orders"]}'
```

```json
{"subscription_id": "44a79d7077ce", "topics": ["billing", "orders"]}
```

```bash
# 查询当前集合（始终排序、去重）
curl -s http://localhost:8000/subscriptions/44a79d7077ce/topics
# {"subscription_id": "44a79d7077ce", "topics": ["billing", "orders"]}

# 退出全部主题
curl -s -X PUT http://localhost:8000/subscriptions/44a79d7077ce/topics \
  -H 'Content-Type: application/json' -d '{"topics": []}'
```

行为约定：

- **替换而非合并**：`PUT` 在单个 SQLite 事务中删除该订阅的全部旧成员关系后重新写入；响应返回规范化后的**排序去重**集合。
- **仅影响未来发布**：改主题集合不会影响已经发布的事件——每个事件的收件人在发布时已快照固定。
- **删除订阅级联清退**：`DELETE /subscriptions/{id}` 会在同一事务中把该订阅从所有主题集合移除（历史投递任务仍保留）。
- **错误码**：订阅不存在返回 `404`；主题名为空、trim 后超过 128 字符、去重后超过 100 个主题返回 `422`。
- **持久化**：成员关系存储在 SQLite 的 `subscription_topics` 表中，服务（或容器）重启后保持。

#### 发布主题事件（POST /topic-events）

```bash
curl -s -X POST http://localhost:8000/topic-events \
  -H 'Content-Type: application/json' \
  -d '{"event_id": "evt-topic-001", "topic": "orders", "payload": {"order_id": 42, "state": "paid"}}'
```

```json
{
  "event_id": "evt-topic-001",
  "topic": "orders",
  "payload": {"order_id": 42, "state": "paid"},
  "replayed": false,
  "recipients": [
    {"task_id": 11, "subscription_id": "44a79d7077ce", "status": "pending", "deliver_at": null},
    {"task_id": 12, "subscription_id": "94d3c624dad2", "status": "pending", "deliver_at": null}
  ]
}
```

`recipients` 即发布瞬间该主题成员的快照，每个收件人对应一条**普通投递任务**（与 `POST /events` 生成的任务完全相同）。行为约定：

- **单事务快照发布**：幂等检查 → 读取主题成员 → 写事件 → 为每个成员建任务 → 写发布记录，全部在同一个 SQLite 事务（`BEGIN IMMEDIATE` + 连接锁）内完成；并发发布/并发成员修改在数据库层被串行化，快照不可能取到“半个集合”。
- **生成的是普通任务**：主题任务复用现有全部投递语义——按订阅 `task_id` 严格有序、令牌桶限流（每次认领扣令牌）、熔断器（连续失败熔断、half-open 探针）、指数退避重试、暂停/恢复、`POST /events/{id}/cancel` 撤销、崩溃恢复与审计查询；worker 代码对任务来源无感知。
- **精确重放（200）**：以 `(event_id, topic, payload)` 为幂等键，三元组完全相同的重放返回**首次发布时存储的回执**——相同的收件人集合、相同的 `task_id`（回执为首次状态的存档；需要实时状态请用 `GET /events/{event_id}`），响应 `replayed=true`，**即使期间主题成员已新增或移除，也不会新增或漏掉任何任务**；并发重放同样只有一个请求真正建任务。
- **内容/主题冲突（409）**：同一 `event_id` 已发布过但 `topic` 不同（409，提示已发布到其它主题）或 `payload` 不同（409，提示内容不同），不产生任何任务。
- **跨通道冲突（409）**：主题事件与 `POST /events`、`POST /ingest` 共享事件 ID 命名空间：`event_id` 已被直接事件/入站事件使用时发布返回 `409`；反之，已发布的主题事件 ID 再用于 `POST /events` 或 `POST /ingest` 同样返回 `409`。
- **空收件人（422）**：发布时该主题没有任何订阅者，返回 `422 Unprocessable Entity`，且**事件、发布记录、任务一律不落库**；之后有订阅者加入主题再用同一 `event_id` 发布仍属首次发布。
- **校验错误（422）**：`event_id` / `topic` 为空或超长（上限 128 字符）、`payload` 不是 JSON 对象，均返回 `422`。
- **状态与审计**：主题事件的 `event_id` 可直接用于 `GET /events/{id}`（各收件任务的实时状态、尝试历史、`counts`）、`POST /events/{id}/cancel` 以及 `GET /audit/deliveries`；出站投递的请求体即规范化后的 `payload` JSON，并按收件订阅自己的密钥签名。
- **首次/重放状态码**：首次发布 `201 Created`（`replayed=false`）；精确重放 `200 OK`（`replayed=true`）。

### 12. 投递审计查询

跨事件、跨订阅的投递任务审计视图，只读、不影响投递行为。

#### 任务列表（GET /audit/deliveries）

```bash
curl -s 'http://localhost:8000/audit/deliveries?status=failed&limit=2'
```

```json
{
  "items": [
    {
      "task_id": 12,
      "event_id": "evt-009",
      "subscription_id": "94d3c624dad2",
      "target_url": "http://localhost:9002/hook",
      "status": "failed",
      "attempts": 4,
      "last_error": "non-2xx response: 500",
      "deliver_at": null,
      "updated_at": "2026-09-18T09:12:04.123456+00:00"
    },
    {
      "task_id": 7,
      "event_id": "evt-004",
      "subscription_id": "44a79d7077ce",
      "target_url": "http://localhost:9001/hook",
      "status": "failed",
      "attempts": 4,
      "last_error": "connection error: ...",
      "deliver_at": "2026-09-18T09:00:00+00:00",
      "updated_at": "2026-09-18T09:04:07.654321+00:00"
    }
  ],
  "next_cursor": "WzE3NTgyMTk4NDcuNjU0MzIxLDdd"
}
```

查询参数（全部可选，组合时按 AND 过滤）：

| 参数 | 说明 |
|------|------|
| `subscription_id` | 按订阅 ID 过滤 |
| `event_id` | 按事件 ID 过滤 |
| `status` | `pending` / `in_progress` / `delivered` / `failed` / `cancelled`，其它值返回 `422` |
| `updated_from` / `updated_to` | 按任务最后更新时间（`updated_at`）过滤的**闭区间**边界；ISO-8601 且必须带显式时区（`Z` 或 `+08:00` 等偏移均可，统一换算为 UTC 比较） |
| `limit` | 每页条数，1～100，默认 50；超出范围返回 `422` |
| `cursor` | 上一页响应的 `next_cursor`，原样传回即可 |

行为约定：

- **排序与分页**：固定按 `updated_at` 倒序（相同则按任务 ID 倒序）返回任务摘要（任务 ID、事件 ID、订阅 ID、目标 URL、状态、尝试次数、最后错误、计划时间 `deliver_at`、更新时间）；采用键集游标分页——游标编码上一页最后一行的 `(updated_at, id)`，不使用偏移量，翻页稳定（同事务批量更新的任务时间戳相同也不会漏重）。响应 `next_cursor` 为 `null` 表示已到最后一页。
- **错误码**：游标无法解析或形状非法返回 `400`；时间格式非法、缺少时区、`updated_from` 晚于 `updated_to` 均返回 `400`；`status` 非法、`limit` 超出 1～100 返回 `422`。
- **已删除订阅**：订阅被删除后其历史任务仍可查询，`target_url` 为 `null`。

#### 任务详情（GET /audit/deliveries/{task_id}）

```bash
curl -s http://localhost:8000/audit/deliveries/12
```

```json
{
  "task_id": 12,
  "event_id": "evt-009",
  "subscription_id": "94d3c624dad2",
  "target_url": "http://localhost:9002/hook",
  "status": "failed",
  "attempts": 4,
  "last_error": "non-2xx response: 500",
  "deliver_at": null,
  "updated_at": "2026-09-18T09:12:04.123456+00:00",
  "max_attempts": 4,
  "next_attempt_at": null,
  "last_status_code": 500,
  "result": "failed",
  "cancelled_at": null,
  "created_at": "2026-09-18T09:11:58.000000+00:00",
  "history": [
    {"attempt_no": 1, "status_code": 500, "error": "non-2xx response: 500",
     "started_at": "2026-09-18T09:11:58+00:00", "finished_at": "2026-09-18T09:11:58+00:00", "duration_ms": 12},
    {"attempt_no": 2, "status_code": 500, "error": "non-2xx response: 500",
     "started_at": "2026-09-18T09:11:59+00:00", "finished_at": "2026-09-18T09:11:59+00:00", "duration_ms": 9}
  ]
}
```

返回该任务的全部字段与**完整尝试记录**（每次尝试的状态码、错误、起止时间与耗时，按 `attempt_no` 升序）。任务不存在返回 `404`；`task_id` 非整数返回 `422`。

### 其他端点

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/ingest` | HMAC 保护的入站事件接口（时间窗 + 签名 + ID 幂等） |
| `POST` | `/events` | 内部提交事件（event_id + payload，无需签名；可选 `deliver_at` 定时投递） |
| `GET` | `/events/{id}` | 查询事件投递状态（含 `cancelled` / `cancelled_at` / 各状态 `counts`） |
| `POST` | `/events/{id}/cancel` | 撤销事件未认领的投递任务（原子、幂等，不存在返回 404） |
| `GET` | `/subscriptions` | 列出全部订阅（含 `paused` / `paused_at`、熔断器字段及 `rate_per_second` / `burst`） |
| `GET` | `/subscriptions/{id}` | 查询单个订阅（含 `paused` / `paused_at`、熔断器字段及 `rate_per_second` / `burst`） |
| `POST` | `/subscriptions/{id}/pause` | 暂停订阅投递（幂等，不存在返回 404） |
| `POST` | `/subscriptions/{id}/resume` | 恢复订阅投递（幂等，不存在返回 404） |
| `POST` | `/subscriptions/{id}/rotate-secret` | 轮换签名密钥（宽限期双签；宽限期内重复轮换返回 409，不存在返回 404） |
| `PUT` | `/subscriptions/{id}/rate-policy` | 设置/替换按订阅令牌桶限流策略（`rate_per_second` + `burst`，桶重置为满，不存在返回 404） |
| `GET` | `/subscriptions/{id}/rate-policy` | 查询限流策略与实时桶余额、下一令牌时间（未配置时 `enabled=false`） |
| `DELETE` | `/subscriptions/{id}/rate-policy` | 关闭按订阅限流（清空桶配置，重复删除或未配置返回 404） |
| `PUT` | `/subscriptions/{id}/delivery-window` | 设置/替换按订阅投递时段（IANA 时区 + 星期 + 本地起止时间，支持跨午夜，不存在返回 404） |
| `GET` | `/subscriptions/{id}/delivery-window` | 查询投递时段与实时 `is_open_now` / `next_open_at`（未配置时 `enabled=false`，不存在返回 404） |
| `DELETE` | `/subscriptions/{id}/delivery-window` | 移除投递时段（不提前改写任务调度；未配置或订阅不存在返回 404） |
| `PUT` | `/subscriptions/{id}/topics` | 替换订阅的去重主题集合（空列表清空，不存在返回 404） |
| `GET` | `/subscriptions/{id}/topics` | 查询订阅的当前主题集合（排序去重，不存在返回 404） |
| `POST` | `/topic-events` | 向主题当前全部订阅者发布事件（单事务快照；精确重放返回首次结果，冲突 409，空主题 422） |
| `DELETE` | `/subscriptions/{id}` | 删除订阅（级联退出全部主题；已有任务保留历史） |
| `GET` | `/audit/deliveries` | 投递任务审计列表（按订阅/事件/状态/UTC 时间范围过滤，游标分页，按更新时间倒序） |
| `GET` | `/audit/deliveries/{task_id}` | 单个投递任务的完整审计视图（含全部尝试记录，不存在返回 404） |
| `GET` | `/healthz` | 健康检查 |

## 按订阅严格有序投递

同一 `subscription_id` 下的投递任务按 `tasks.id` 升序（即任务创建顺序）逐条投递，语义如下：

- 较早任务处于 `pending`（含退避等待重试中、定时任务等待 `deliver_at` 到点，以及投递时段关闭时被排到下次开放的队首任务）或 `in_progress`（投递中）时，同订阅的后续任务**不会被任何工作器认领**；定时任务与窗口外任务因此会阻塞其后的立即投递任务；
- 较早任务到达终态——`delivered`（成功）、`failed`（重试耗尽后最终失败）或 `cancelled`（随事件撤销）——才放行同订阅的下一条任务；
- 不同订阅之间没有顺序约束，`WORKER_COUNT` 个工作器可并行投递不同订阅的任务；
- 约束在认领时基于 SQLite 中的持久化状态判定，因此失败重试、服务重启（含崩溃恢复）后依然有效；
- 被阻塞的任务状态保持 `pending`，通过 `GET /events/{event_id}` 可正常查询到。

## 接收端验签

每个投递请求携带以下头部：

| 头部 | 说明 |
|------|------|
| `X-Webhook-Signature` | `sha256=<HMAC-SHA256(当前 secret, 请求体)>` 十六进制摘要 |
| `X-Webhook-Signature-Previous` | 仅在密钥轮换宽限期内出现：`sha256=<HMAC-SHA256(旧 secret, 请求体)>`，用于接收方平滑切换 |
| `X-Webhook-Event-Id` | 事件 ID |
| `X-Webhook-Task-Id` | 投递任务 ID |
| `X-Webhook-Attempt` | 当前第几次尝试 |
| `X-Webhook-Timestamp` | 发送时间戳（秒） |

Python 验签示例：

```python
import hashlib, hmac

def is_valid(secret: str, body: bytes, signature_header: str) -> bool:
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature_header)

# 轮换宽限期内：任一签名通过即可（headers 为请求头字典）
def is_valid_during_rotation(new_secret: str, old_secret: str, body: bytes, headers) -> bool:
    if is_valid(new_secret, body, headers.get("X-Webhook-Signature", "")):
        return True
    prev = headers.get("X-Webhook-Signature-Previous")
    return prev is not None and is_valid(old_secret, body, prev)
```

## 设计说明

- **启动迁移（PRAGMA user_version 顺序版本链）**：所有结构变更按有序版本链登记（`app/db.py` 中追加式的 `_MIGRATIONS`，版本号只增、永不重排）。每个版本的 DDL 与 `PRAGMA user_version` 推进在**同一个 `BEGIN IMMEDIATE` 事务**内提交（SQLite 的 DDL 与该 pragma 均可回滚），因此外部只能观察到完整版本；**新建库**在单事务内直接创建最新结构并加盖最新版本，不遍历历史链。迁移开始前先取数据库写锁并在锁内复查 `user_version`：多个实例（或多进程）同时启动时写锁把迁移串行化，后到实例等待 `busy_timeout` 后读到已提交的版本，跳过全部已完成步骤——每个步骤全局只执行一次。任意步骤异常时该步骤的全部 DDL/数据搬运与版本号推进整体回滚，文件停留在上一个完整版本；修复后再次启动会从失败版本继续升级（各步骤对版本 0 的旧库均按列/表是否存在做幂等守卫，可安全重放）。数据库文件的版本新于当前代码支持的版本时拒绝启动（防止降级破坏）。全部步骤完成后执行 `PRAGMA foreign_key_check`：发现违例行即抛出迁移错误，FastAPI 生命周期在创建任何工作器之前终止（容器 `restart: unless-stopped` 会重启但始终不投递），保证不会在结构不一致的库上运行。需要重建表的版本（`tasks` 的 `cancelled` CHECK 扩宽）按 SQLite 官方流程在事务外临时 `foreign_keys=OFF`，事务提交后立即恢复 `ON`，并由最终的外键检查兜底。

- **任务模型**：事件存于 `events` 表（payload 只存一份），每个 `(event_id, subscription_id)` 对应 `tasks` 表中一行（唯一约束保证幂等）；每次尝试写入 `attempts` 历史表。
- **入站幂等**：每个验签通过的 `X-Webhook-Id` 在 `ingested_events` 表中记录一行（原始 body 字节 + 首次回执）。查重、冲突判定、建事件、建任务在同一个 SQLite 事务中完成；重放请求直接返回存储的首次回执，即使并发到达也只会有一个请求建任务。
- **原子认领**：工作器在单个连接锁保护的临界区内完成「选中最旧到期任务 → 置为 `in_progress`」，且该事务以 `BEGIN IMMEDIATE` 开启（配合 `busy_timeout=5000`），在 SELECT 之前即取得 SQLite 写锁，因此即使多个工作器分属不同进程、共享同一个数据库文件，同一轮尝试也会在数据库层被串行化、不会被重复领取；纯空闲轮询不产生任何写入并回滚（ROLLBACK），不触发 fsync。
- **按订阅有序**：认领查询附带 `NOT EXISTS` 条件——同订阅存在 id 更小且状态为 `pending`/`in_progress` 的任务时，当前任务不可认领；只有前序任务进入终态（`delivered`/`failed`/`cancelled`）才放行下一条。条件针对持久化状态在认领时求值，故重试退避、崩溃恢复（`in_progress` 重置为 `pending` 后依然阻塞后续任务）与多工作器并发下均成立；不同订阅互不影响，可并行投递。
- **订阅暂停**：暂停只是翻转 `subscriptions.paused_at` 标记（首次暂停的时间戳，重复暂停不变），从不修改任务行。认领查询增加 `s.paused_at IS NULL` 条件，与有序约束在同一原子临界区内求值，因此暂停对已 `in_progress` 的尝试（至多 `WORKER_COUNT` 个并发）无影响、它们照常完成，而新的认领立即停摆；恢复时清除标记即可，所有 `pending` 任务保留原 `next_attempt_at` 与 `task_id` 顺序，无需任何批量更新。
- **密钥轮换**：`subscriptions` 表新增 `previous_secret` 与 `previous_secret_expires_at` 两列（老库由启动迁移自动 `ALTER TABLE` 补齐），二者始终同时设置或同时清空。轮换在连接锁保护的单次提交中原子完成——当前密钥降级为 previous 并写入绝对失效时间，`secret` 列更新为新密钥；`grace_seconds=0` 时直接清空 previous 列。工作器每次认领任务时随连接读出当前密钥状态，构造请求头时按发送时刻判定宽限期是否结束，因此重试、重启后均按当时状态签名；宽限期未结束的再次轮换在判定阶段即被拒绝（409），不产生任何写操作。查询接口只读取失效时间戳推导 `rotating` 标志，密钥明文从不离开持久层进入响应或日志。
- **投递熔断器**：`subscriptions` 表新增 `breaker_state`、`breaker_consecutive_failures`、`breaker_next_probe_at`、`breaker_probe_task_id` 四列（老库由启动迁移自动补齐，存量订阅初始为 `closed`/0）。任务终态（`delivered`/`failed`）与熔断器记账在 `finish_attempt` 的同一个 SQLite 事务中提交：最终失败使连续失败数加一，达到 `BREAKER_FAILURE_THRESHOLD` 即原子切为 `open` 并写入 `next_probe_at = 失败时刻 + BREAKER_COOLDOWN_SECONDS`；任意成功投递清零计数。认领查询按熔断状态过滤——`open` 且冷却未到的订阅不可认领；冷却到期后首个认领把订阅原子翻为 `half_open` 并把该任务（按订阅有序约束保证即最早待投递任务）登记为唯一探针，`half_open` 期间认领查询只允许探针任务本身通过，故多工作器并发下探针唯一；探针成功则关闭并清零，最终失败则重新 `open` 并以该失败时刻刷新冷却。全部状态持久化，重启（含崩溃恢复）后冷却计时与探针唯一性继续生效。
- **崩溃恢复**：启动时将残留的 `in_progress` 任务重置为 `pending` 并立即到期；`pending` 任务按原 `next_attempt_at` 继续调度。崩溃恢复把未完成任务重新视为“尚未认领”，因此若其事件在恢复后被撤销，这些任务会随撤销终止；未撤销则照常重新投递。
- **按订阅令牌桶限流**：`subscriptions` 表新增 `rate_capacity`、`rate_refill_per_second`、`rate_tokens`、`rate_updated_at` 四列（老库由启动迁移自动 `ALTER TABLE` 补齐，存量订阅四列为 `NULL` 即不限流）。令牌补充采用惰性墙钟模型——读取桶时按 `min(capacity, 余额 + (now − updated_at) * rate)` 现算，不需要后台补充协程。令牌的补充、判断、扣减与余额持久化全部内联在认领的同一个 `BEGIN IMMEDIATE` 临界区内，与任务状态翻转、熔断器 half_open 翻转一并提交，因此多工作器（含多进程共享一个 DB 文件）不会重复扣用同一令牌，重启后从持久化的 `rate_tokens`/`rate_updated_at` 继续补充。桶中不足一个整令牌时不领取任务：队首任务保持 `pending`，`next_attempt_at` 被原子改为下一令牌可用时刻 `now + (1 − 余额)/rate`（持久化余额的纯函数，任意工作器重算一致），补充后的余额同事务落库；同一次认领调用会记住已被门控（限流或窗口关闭）的订阅集合（`deferred_sids`），对每个订阅至多重调度一次后跳过并继续寻找其它订阅的任务，既避免无意义地反复推后时间，也保证循环必然终止。由于严格有序约束使候选任务恒为订阅队首，令牌检查只发生在队首，队首等令牌时后续任务本就被 `NOT EXISTS` 阻塞，故限流绝不破坏同订阅有序投递；不同订阅互不影响，被限流期间仍可并行投递。`PUT` 安装/替换策略把余额重置为满（`rate_tokens=capacity, rate_updated_at=now`）但不批量改写任务调度；`DELETE` 清空四列关闭限流；`GET` 的实时余额仅现算展示、不写库。
- **按订阅投递时段**：新增 `app/delivery_window.py` 纯函数调度模块（`DeliveryWindow` 不可变值对象 + IANA 时区解析），`subscriptions` 表新增 `window_timezone`、`window_weekdays`（排序去重后的 JSON 数组）、`window_local_start`、`window_local_end`（本地午夜后秒数）、`window_updated_at` 五列（新建库直接具备；老库由启动迁移自动 `ALTER TABLE` 补齐，存量订阅五列为 `NULL` 即全天可投递）。时区按 IANA 名经 `zoneinfo.ZoneInfo` 加载并按名缓存；Pydantic 模型在写入前校验时区可解析、星期非空且 1..7 不重复、起止时间可解析且在 `00:00:00-23:59:59`、起止不相等（非法一律 `422` 不写库），接受 `HH:MM`/`HH:MM:SS`、响应规范化为 `HH:MM:SS`；`local_end <= local_start` 即跨午夜窗口（如 `22:00-06:00`、`00:00-00:00` 全天）。开放判定与下次开放共用同一套区间解析：每次开放被绑定为一条明确的 UTC 半开区间 `[open, close)`，`open` 是允许星期当日 `local_start` 的真实时刻，`close` 是同一（跨午夜时次日）`local_end` 的真实时刻；`is_open(ts)` 只需检查当日锚定的区间与（跨午夜时）前一日锚定的区间是否覆盖 `ts`——跨午夜尾部因此仍归属于开窗的那个星期——判定只比较绝对瞬间，不依赖“当前是第几次经过重复小时”的任何状态。起止锚点统一经 `_open_instant` 解析：普通时刻直接映射，重复小时取 `fold=0`（较早一次，故结束时刻落入秋令重复小时的窗口在第一次到达结束时刻即关闭，第二次经过同一挂钟时段不会重开或延长），不存在的跳空时刻先经往返比对（`fromtimestamp` 后能否还原同一 naive 本地时间）识别，再对单调不减的本地时间函数二分定位并取整到 tzdb 的整秒跳变点（搜索跨度 48h，远超地球上任何 DST 跳变），即跳空后的第一个真实时刻；春令跳空小时因此天然不在任何区间内，整段落入跳空的窗口当天不开放。`next_open(ts)` 从当天起枚举至多 7 个本地日期中属于允许星期的日子，用同一套区间解析取不早于 `ts` 且非空的最早区间起点，结果必为窗口真正开放的时刻。认领临界区在熔断器/暂停过滤之后、令牌桶之前对候选队首做窗口门控：关闭时一条 `UPDATE tasks` 把队首 `next_attempt_at` 确定性推到 `next_open(now)`（仅对已到期任务前推，未来的计划时间永不被提前），订阅记入 `deferred_sids` 后继续认领其它订阅，全部重调度随事务一次提交；开放则继续进入令牌扣减与认领。调度时间是绝对 UTC epoch 且全程持久化，无进程内定时器，故重启/崩溃恢复后只能在真实开放时刻投递；窗口配置本身同样落库，重启无需重建。`PUT`/`DELETE` 与限流策略一样不批量改写任务行：已排到未来开放时刻的任务保留其持久化时间（绝不因配置变更而提前），醒来时按新配置判定；`GET` 的 `is_open_now`/`next_open_at` 仅现算展示、不写库。容器侧在 `requirements.txt` 增加 `tzdata`，确保 `python:3.12-slim`（无系统 tzdb）内 `zoneinfo` 可解析全部 IANA 时区。
- **定时投递**：`POST /events` 的可选 `deliver_at` 在 Pydantic 模型层校验——必须是可解析的 ISO-8601 字符串、带显式时区偏移（拒绝无时区的朴素时间与纯日期）、且严格晚于当前时刻，任何不满足均返回 `422` 且不写库；通过后转换为 UTC epoch 秒。`tasks` 表新增 `deliver_at REAL` 列保存该计划时间（立即投递为 `NULL`），并在建任务时用它初始化 `next_attempt_at`，因此“到点前不认领”无需任何进程内定时器，完全由认领查询既有的 `next_attempt_at <= now` 条件保证，服务重启、崩溃恢复后同样成立。等待中的定时任务状态仍是 `pending`，认领查询既有的按订阅有序 `NOT EXISTS` 条件使其自动阻塞同订阅后续所有任务（包括立即到期的任务），直到定时任务本身进入终态；不同订阅照常并行投递。定时任务到点后的投递、失败退避、撤销、暂停恢复、熔断探针等路径与普通任务完全复用，无需特判。老库迁移：经 `cancelled` 表重建升级的最老库在重建时直接带上 `deliver_at` 列；已有 `cancelled_at` 但缺少该列的较新库通过 `ALTER TABLE tasks ADD COLUMN deliver_at REAL` 补齐，存量任务取值 `NULL`（即立即投递语义）。
- **事件级撤销**：`tasks.status` 增加终态 `cancelled` 并新增 `cancelled_at` 列（新建库直接具备；老库启动迁移时按 SQLite 官方表重建流程——`foreign_keys=OFF`、单个显式事务内建新表、拷贝数据、DROP、RENAME、重建 `tasks` 上的两个索引——自动升级，既有任务数据原样保留）。`POST /events/{id}/cancel` 调用的 `cancel_event` 在连接锁保护的单个事务中执行：事件不存在直接返回 `None`（API 返回 `404`）；否则一条 `UPDATE ... WHERE event_id=? AND status='pending'` 原子地只撤销尚未认领的任务（写入 `cancelled_at`、清空 `next_attempt_at`/`claimed_by`），`in_progress` 与已结束任务不在 WHERE 命中范围内、结果不变；随后按状态 `GROUP BY` 计数返回。由于认领查询只选 `pending` 且有序约束只盯 `pending`/`in_progress`，撤销行既不会被重试、也不会阻塞同订阅后续任务；撤销从不经过 `finish_attempt`，故完全不触碰熔断器记账。唯一需要修复的边角是 half-open 探针在两次尝试之间回到 `pending` 时被撤销：同一事务内把该订阅熔断器从 `half_open` 转回 `open`、探针立即到期并清空探针归属，使下一个待投递任务成为替代探针，避免订阅永久卡在 half-open。重复撤销时 UPDATE 命中 0 行，响应 `cancelled=0` 且计数稳定，天然幂等；撤销与工作器认领共用同一把连接锁，并发下每个任务只可能“被认领（投递并记账）”或“被撤销”，不会重复领取也不会状态错乱。
- **退避策略**：第 n 次失败后延迟 `2^(n-1)` 秒（1、2、4），第 4 次失败后任务终止，结果为 `failed`。
- **投递审计查询**：审计接口是 `tasks` / `attempts` 表的只读视图，不新增任何写入路径，投递行为完全不受影响。列表查询固定按 `(updated_at DESC, id DESC)` 排序并采用键集（keyset）分页：游标是上一页最后一行 `(updated_at, id)` 的 base64url 编码（不透明字符串，客户端只需原样回传），下一页以 `WHERE (updated_at, id) < cursor` 续查，因此无需 OFFSET、翻页成本与页深无关，且同一事务批量更新产生的相同 `updated_at`（如事件级撤销）由 `id` 决胜、不会漏行或重复；每页多取一行判断是否存在下一页，`next_cursor` 为 `null` 即末页。`updated_from` / `updated_to` 在 API 层解析为 UTC epoch 秒（要求显式时区，与 `deliver_at` 的校验风格一致）后作为 `updated_at` 的闭区间下推同一查询；游标解码失败、时间范围非法在进入数据库前即返回 `400`。详情查询按任务 ID 联查 `subscriptions`（LEFT JOIN，订阅已删时 `target_url` 为 `null`）并取出该任务的全部 `attempts` 行。
- **持久化主题发布**：新增两张表（新建库直接具备；老库由启动时 `CREATE TABLE IF NOT EXISTS` 自动创建，不改动任何旧表）：`subscription_topics(subscription_id, topic, created_at)` 以联合主键保存去重成员关系、按 `topic` 建索引；`topic_events(id, topic, payload, receipt, created_at)` 保存发布记录与首次回执。`PUT /subscriptions/{id}/topics` 在单事务内“全删 + 重写”整个集合（替换语义），`DELETE` 订阅在删除订阅的同一事务中级联清退成员关系。`POST /topic-events` 的 `publish_topic_event` 在连接锁保护的一个 `BEGIN IMMEDIATE` 事务中依次执行：①按 `(event_id, topic, payload)` 查 `topic_events`——三元组全等返回存储回执（`duplicate`），topic 不符返回 `('conflict','topic')`，payload 不符返回 `('conflict','payload')`；②查 `events` 表拦截跨通道 ID 复用（`('conflict','channel')`，`POST /events` 侧用 `topic_event_exists`、`POST /ingest` 侧在 `ingest_event` 内做对称检查，因此跨通道冲突在两个方向上都封死）；③快照 `subscription_topics` 中该主题的成员（按 `created_at, subscription_id` 排序，保证收件顺序确定），为空则直接回滚返回 `empty`——API 映射为 422，且事务未写入任何行；④写 `events`（payload 规范化 JSON 存一份，worker 与 `GET /events` 无需区分来源）、逐成员 `INSERT tasks`（普通 pending 任务，`max_attempts`、退避、有序、令牌桶、熔断全部由既有认领/记账逻辑承载）、写 `topic_events`（含首次回执 JSON）。由于任务是普通任务，有序投递天然成立：同订阅的主题任务与其直接事件任务共用一条 `task_id` 队列。重放只读取首次回执、不碰任务表，所以成员集合事后如何变化都不会增删任务；回执中 `recipients` 的任务状态是首次发布时刻（均为 pending）的存档，实时状态由 `GET /events/{id}` 提供。
- **成功判定**：仅 HTTP 2xx 视为成功；其余状态码、连接错误、超时均触发重试。

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `INGEST_SECRET` | *无（compose 内为 `dev-ingest-secret-change-me`）* | `POST /ingest` 的 HMAC 共享密钥；未配置时该端点返回 `503` |
| `INGEST_TIMESTAMP_TOLERANCE_SECONDS` | `300` | 入站时间戳允许的最大偏差（秒，正负两个方向） |
| `DB_PATH` | `./webhook_relay.db`（容器内 `/data/webhook_relay.db`） | SQLite 数据库路径 |
| `WORKER_COUNT` | `4` | 并发投递工作器数量 |
| `HTTP_TIMEOUT_SECONDS` | `5` | 单次投递 HTTP 超时 |
| `MAX_ATTEMPTS` | `4` | 单任务最大尝试次数 |
| `BACKOFF_BASE_SECONDS` | `1` | 首次重试延迟（之后每次翻倍） |
| `BREAKER_FAILURE_THRESHOLD` | `5` | 触发订阅熔断的连续最终失败任务数 |
| `BREAKER_COOLDOWN_SECONDS` | `30` | 熔断冷却时间（到期后放行唯一 half-open 探针） |
| *按订阅限流* | 未启用（默认不限流） | 限流**不使用环境变量**，逐订阅通过 `PUT /subscriptions/{id}/rate-policy` 配置（`rate_per_second` / `burst`），策略与令牌余额持久化在数据库中 |
| *按订阅投递时段* | 未启用（默认全天可投递） | 投递时段**不使用环境变量**，逐订阅通过 `PUT /subscriptions/{id}/delivery-window` 配置（IANA 时区 + 星期 + 本地起止时间），配置与调度状态持久化在数据库中 |
| `POLL_INTERVAL_SECONDS` | `0.2` | 空闲工作器轮询间隔 |

## 项目结构

```
app/
├── main.py            # FastAPI 应用：API 端点 + 生命周期（启动/停止工作器）
├── worker.py          # 投递工作器：认领任务、HTTP 投递、记录结果
├── db.py              # SQLite 持久层：user_version 版本链迁移、建表、原子认领、状态更新
├── delivery_window.py # 投递时段：IANA 时区、星期窗口、DST 安全的确定性调度
├── schemas.py         # Pydantic 请求/响应模型
├── signing.py         # HMAC-SHA256 签名与验签
└── config.py          # 环境变量配置
tests/
├── test_db_migrations.py        # 启动迁移版本链：新库直达、旧库升级保数据、步骤事务原子性、并发串行、外键检查拒绝启动
└── test_delivery_window_dst.py # 投递时段 DST 回归（秋令回拨两个 fold、春令跳空、星期归属、认领/限流顺序）
Dockerfile
docker-compose.yml
requirements.txt
```
