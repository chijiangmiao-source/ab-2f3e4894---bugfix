# 高危遥控值班系统（选择 Selection — 执行 Execution）

地面站向载荷下发**高危遥控**前，值班系统要求先创建一条带失效时刻的
**选择**，执行请求只有在“同一稳定操作标识、命令摘要一致且仍在有效期”时
才能**消费**该选择。系统提供并发唯一消费、重传裁决回放、断电恢复与持久
记录完整性保护。仅依赖 **Python 3.11 标准库**（`http.server` / `sqlite3`
/ `hmac` / `threading`），无需第三方包。

## 一、业务规则

### 选择（Selection）
- 以四要素创建：`device_id`（设备标识）、`op_id`（稳定操作标识）、
  `summary`（命令摘要）、`expires_at_ms`（失效时刻，毫秒时间戳）。
- 同一设备**同时只保留一条未失效（ACTIVE）选择**。
- 选择成功执行后（CONSUMED）**不得再次选择**（即使字段变化也 409）。
- 旧选择过期后可建立新选择，旧选择保持 `EXPIRED`，**不会复活**。

### 执行（Execution）
仅当满足以下全部条件才允许消费选择：
1. 设备存在选择；
2. `op_id` 与选择完全一致（不一致 → `403 OP_ID_MISMATCH`）；
3. `summary` 与选择完全一致（不一致 → `409 SUMMARY_MISMATCH`）；
4. 当前时间未超过 `expires_at_ms`（过期 → `410 SELECTION_EXPIRED`）；
5. 选择状态为 `ACTIVE`（已消费 → 回放原最终结果）。

### 重传与冲突（裁决回放）
- **相同内容**或**相同 `request_id`** 的选择/执行重传：回放原裁决
  （同一记录 id 与同一结果），响应带 `"replayed": true`。
- 相同 `request_id` 但任一字段变化：明确返回
  `409 RETRANSMIT_FIELD_CONFLICT`，绝不静默当作新请求。
- 并发竞争：同一选择多个执行请求，**只有一个首次消费成功**；其余请求
  阻塞到终态落盘后，收到**同一最终结果（EXECUTED）**，而不是“处理中”。

### 断电恢复
执行落盘分两段事务（WAL + `synchronous=FULL`）：
- **T1**：写 `execution(EXECUTING)` 并把选择置为 `CONSUMED`（同一事务）。
- **T2**：写 `execution(EXECUTED, result)`。

| 注入点 | 磁盘状态 | 重启恢复 |
|--------|----------|----------|
| 结果落盘**前**（设备已驱动、T2 前） | 悬空 EXECUTING + CONSUMED | 回滚为 **ACTIVE 未执行**；设备按 op_id 幂等，可安全重试并得到同一确定结果 |
| 结果落盘**后**（T2 提交后） | EXECUTED + CONSUMED | 恢复为 **CONSUMED 已执行**，重传回放同一结果 |

- 启动恢复会：校验全部持久记录 → 回滚所有悬空 EXECUTING → 标记到点选择过期。
- 每条记录带 **HMAC-SHA256**（绑定表名与全部字段，`DUTY_MAC_KEY`）。
  记录被篡改/缺失 MAC/不可读时：
  - `GET /health` 返回 **503 `degraded`** 并列出 `integrity_errors`；
  - 所有变更操作 **fail-closed**（`503 PERSISTENCE_INTEGRITY_FAILED`）。

## 二、HTTP API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/health` | 健康检查，异常返回 503 |
| POST | `/api/v1/selections` | 创建/重传选择（成功 201） |
| POST | `/api/v1/executions` | 消费选择执行/回放（200） |
| GET  | `/api/v1/selections?device_id=...` | 查询设备最新选择 |

```bash
# 创建选择
curl -s localhost:8080/api/v1/selections -H 'Content-Type: application/json' -d '{
  "device_id":"SAT-1","op_id":"OP-100","summary":"开机 红外相机",
  "expires_at_ms": 1767225600000, "request_id":"sel-1"}'

# 执行（消费）
curl -s localhost:8080/api/v1/executions -H 'Content-Type: application/json' -d '{
  "device_id":"SAT-1","op_id":"OP-100","summary":"开机 红外相机",
  "request_id":"exe-1"}'

# 健康
curl -i localhost:8080/health
```

## 三、本地运行（无 Docker）

```bash
# 运行服务（默认 /data/duty.db，可改 DUTY_DB_PATH 到可写目录）
DUTY_DB_PATH=./data/duty.db DUTY_HTTP_PORT=8080 python3 -m app.main

# 只跑单元测试
python3 -m unittest discover -s tests -v

# 一键验收（构建检查 + 单元测试 + HTTP 冒烟），退出码即结果
python3 scripts/verify.py
```

## 四、Docker Compose 验收

`docker-compose.yml` 提供两个服务：

- **duty**：值班 HTTP 服务，SQLite 持久化在命名卷 `duty-data`，
  自带 `/health` 健康探针（完整性异常时探针失败）。
- **verify**：**一次性**验收服务。启动后等待 `duty` 健康，然后执行
  代码测试、构建检查、针对健康与选择—执行流程的 API/HTTP 冒烟
  （实际覆盖并发竞争、过期拒绝、两处断电恢复），**执行结束即退出并以
  退出码报告结果**（0 通过 / 1 失败）。

```bash
docker compose build
docker compose up verify          # duty 作为依赖被拉起
docker compose ps -a              # 查看 verify 退出码（Exited 0）
docker compose logs verify        # 查看逐条验收结果
```

断电恢复的中断注入通过环境变量开启（verify 在容器内部以真实子进程模拟，
不影响常驻 duty）：
`CRASH_BEFORE_PERSIST=1`（退出码 121）、`CRASH_AFTER_PERSIST=1`（122）。

## 五、目录结构

```
app/
  config.py    运行配置与崩溃注入开关
  security.py  记录级 HMAC 完整性保护
  store.py     SQLite 持久化、两段式落盘与启动恢复
  device.py    载荷设备模拟器（按稳定操作标识幂等，结果确定可重放）
  service.py   业务规则与并发裁决（每选择一把消费锁）
  httpapi.py   HTTP/JSON 接口
  main.py      进程入口（打开存储→恢复→服务）
scripts/
  verify.py    一次性验收（构建检查/单测/HTTP 冒烟）
  healthcheck.py 容器健康探针
tests/
  test_service.py 单元测试（并发/过期/两处断电恢复/篡改）
```
