# sys-analyzer

sys-analyzer 的控制面由一个 Controller、多个 Agent 和一个管理 CLI 组成。Agent 通过明文 gRPC（h2c）主动注册，并按 Controller 下发的间隔发送心跳续租。CLI 通过明文 HTTP/1.1 查询和管理 Agent。

## 本地运行

在仓库根目录编译。Cargo 构建时使用内置的 `protoc`。

```sh
cargo build --locked --workspace
```

在两个终端分别启动 Controller 和 Agent。

```sh
./target/debug/sys-analyzer-controller
```

```sh
./target/debug/sys-analyzer-agent --agent-id local-agent --label dev --metadata zone=local
```

省略 `--agent-id` 时，每个 Agent 进程生成自己的 UUID。可以重复使用 `--label` 和 `--metadata KEY=VALUE`。Controller 会拒绝在线 Agent 使用的 ID。

```sh
curl http://127.0.0.1:8080/health
./target/debug/sys-analyzer-cli list
./target/debug/sys-analyzer-cli list --output json
./target/debug/sys-analyzer-cli list --status offline
./target/debug/sys-analyzer-cli show local-agent --output json
```

`GET /health` 返回 `{"status":"ok"}`。`GET /agents` 返回 Agent 列表，支持用 `status` 查询参数按 `Online`、`Offline` 或 `Disabled` 过滤。`GET /agents/{agent_id}` 返回单个节点详情，包含租约到期时间。节点不存在时返回 HTTP 404，`show` 输出错误并返回非零退出码。

租约一到期，REST 列表、状态筛选和详情就会将该在线 Agent 显示为 `Offline`。离线保留期从巡检首次发现租约到期时开始，保留期结束后由后续巡检摘除记录。离线或被摘除的 Agent 可以沿用原 ID 注册新会话。注册记录保存在 Controller 内存中，重启 Controller 会清空列表。

CLI 的表格和 JSON 都使用整数 Unix 秒展示时间。例如，`2024-01-01T00:00:00.123Z` 在 JSON 中输出为数字 `1704067200`。第一次心跳到达前，表格显示 `-`，JSON 的 `last_heartbeat_at` 为 `null`。Controller REST API 使用 RFC 3339 UTC 时间戳。

Controller 暂时无法连接，或注册、心跳 RPC 超时时，Agent 会等待 3 秒再重试。
如果 Controller 已完成注册且租约仍然有效，重试同一次注册会取回原会话。

Agent 在 `Heartbeat` 请求中携带 `Register` 返回的会话 ID。Controller 要求携带过期或不匹配会话 ID 的 Agent 重新注册，已禁用的 Agent 会收到停止指令。收到重新注册指令后，Agent 沿用原 ID 发起注册，成功后按新会话的间隔发送心跳。为兼容省略会话 ID 的旧客户端，Controller 仍接受其按 Agent ID 为有效租约续期。

Agent 收到 `SIGINT` 或 `SIGTERM` 时会停止心跳并请求注销当前会话，Controller 确认后立即从列表移除该节点。Agent 在最多 2 秒的退出清理时间内重试暂时失败的注销请求。

## 管理 Agent

```sh
./target/debug/sys-analyzer-cli disable local-agent
./target/debug/sys-analyzer-cli enable local-agent --output json
./target/debug/sys-analyzer-cli remove local-agent --output json
```

禁用操作将 `Online` 或 `Offline` 节点转为 `Disabled`，同时撤销原注册会话。Controller 拒绝该 ID 的注册请求，并在心跳响应中通知 Agent 停止。Agent 收到停止指令后正常退出。禁用记录会跨巡检周期保留。

启用操作将 `Disabled` 节点转为 `Offline`，开始新的离线保留期，并允许该 ID 注册新会话。移除操作立即删除记录并释放该 ID。仍在运行的 Agent 在下一次心跳收到重新注册指令后可以再次上线。

| CLI 子命令 | REST 接口 | 成功响应 |
| --- | --- | --- |
| `disable AGENT_ID` | `POST /agents/{agent_id}/disable` | `200`，禁用后的节点详情 |
| `enable AGENT_ID` | `POST /agents/{agent_id}/enable` | `200`，启用后的节点详情 |
| `remove AGENT_ID` | `DELETE /agents/{agent_id}` | `204`，空响应体 |

三个子命令均支持 `--output table` 和 `--output json`。移除成功时，CLI 的 JSON 输出为 `{"agent_id":"local-agent","removed":true}`。找不到节点时返回 `404`，重复禁用或启用未禁用节点时返回 `409`。两种错误的响应体均为包含 `error` 字段的 JSON 对象。CLI 会输出 HTTP 状态和错误原因，并返回非零退出码。

## 地址配置

| 组件 | 参数 | 环境变量 | 默认值 |
| --- | --- | --- | --- |
| Controller REST | `--rest-addr` | `SYS_ANALYZER_REST_ADDR` | `127.0.0.1:8080` |
| Controller gRPC | `--grpc-addr` | `SYS_ANALYZER_GRPC_ADDR` | `127.0.0.1:50051` |
| Agent | `--controller` | `SYS_ANALYZER_GRPC_URL` | `http://127.0.0.1:50051` |
| CLI | `--controller` | `SYS_ANALYZER_REST_URL` | `http://127.0.0.1:8080` |

Controller 使用 `--heartbeat-interval-ms`（`SYS_ANALYZER_HEARTBEAT_INTERVAL_MS`）配置心跳间隔，默认为 5000 毫秒。使用 `--lease-timeout-ms`（`SYS_ANALYZER_LEASE_TIMEOUT_MS`）配置租约时长，默认为 15000 毫秒。心跳间隔须大于零，租约时长须大于心跳间隔。

`--offline-retention-ms`（`SYS_ANALYZER_OFFLINE_RETENTION_MS`）配置离线保留期，默认为 60000 毫秒。`--sweep-interval-ms`（`SYS_ANALYZER_SWEEP_INTERVAL_MS`）配置巡检周期，默认为 1000 毫秒。两者都须大于零。Controller 收到 `SIGINT` 或 `SIGTERM` 时会停止巡检并关闭 REST 和 gRPC 服务。

## 测试

在仓库根目录运行 Rust 单元测试和端到端测试。

```sh
cargo test --locked --workspace
./e2e/control_plane.py -v
```

端到端测试使用 Python 3.11 或更新版本的标准库。脚本会构建并启动 Controller、Agent 和 CLI，使用临时回环端口验证健康检查、Agent 注册和冲突处理、多 Agent 心跳保活、CLI 查询、禁用、启用、移除及错误处理。
