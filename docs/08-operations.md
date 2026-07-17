# 08 · 部署与运维

本页给出可直接执行的远程单实例流程，以及故障时需要观察的持久状态。默认本机路径仍是 `human local`；只有 gateway 与专家 TUI 不在同一台机器时才拆分。

## 远程单实例

非 loopback 的 bearer 连接必须使用 HTTPS/WSS。推荐让 `human gateway` 只监听反向代理同机的 loopback 地址，由 Caddy、nginx 或云负载均衡器终止 TLS；不要把 `127.0.0.1:8080` 直接暴露到网络。

先在 gateway 主机创建私有目录和两类凭据。输出 JSON 同时包含 `key_id` 和一次性明文 token，文件必须按 secret 管理：

```sh
install -d -m 700 /var/lib/human
umask 077
human gateway --db /var/lib/human/human.db token issue \
  --type worker --subject expert-1 > /var/lib/human/worker-credential.json
human gateway --db /var/lib/human/human.db token issue \
  --type caller --subject tenant-a > /var/lib/human/caller-credential.json
jq -r .token /var/lib/human/worker-credential.json > /var/lib/human/worker.token
```

启动 gateway，并让 TLS 代理把公开的 `https://human.example` / `wss://human.example` 转发到它：

```sh
human gateway \
  --db /var/lib/human/human.db \
  --listen 127.0.0.1:8080
```

专家机器只从环境变量或 mode `0600` token 文件读取 worker secret；CLI 不接受明文 token 参数：

```sh
human worker \
  --gateway wss://human.example/internal/v1/worker/ws \
  --token-file ~/.config/human/worker.token
```

需求方把 caller token 放入 `HUMAN_CALLER_TOKEN`，再生成指向 HTTPS endpoint 的 exact OpenCode 配置：

```sh
export HUMAN_CALLER_TOKEN="$(jq -r .token /secure/caller-credential.json)"
human init opencode \
  --workspace . \
  --base-url https://human.example/v1 \
  --output ./opencode.human.jsonc
```

`human init`、worker 与 shim 都会拒绝非 loopback 的明文 bearer endpoint。配置文件只在显式传入 `--config` 时读取；客户 workspace 中的 `human.yaml` 不会被自动执行或用于选择 token 文件。

吊销时使用签发 JSON 中的 `key_id`：

```sh
human gateway --db /var/lib/human/human.db token revoke \
  --key-id "$(jq -r .key_id /var/lib/human/worker-credential.json)"
```

## 可重复故障门

项目内部网络与服务异常矩阵：

```sh
make fault-test FAULT_COUNT=3
```

安装精确 OpenCode 1.17.18 后，再跑两个真实客户端门：

```sh
make real-opencode-tui-test REAL_COUNT=3
make real-opencode-network-test REAL_COUNT=3
```

网络门依次在 gateway 已接单但下游尚未收到 response headers、完整 stream-start 首帧后、完整 Human progress 帧后三处主动断 TCP，并核对 retry 的请求 body、`X-Session-Id`、Human 幂等键、assignment 数和最终 CLI 输出。它验证真实 OpenCode 的断线行为；内部 fault-test 继续负责 worker/gateway/SQLite 重启、三方重叠离线和 outbox 精确重放。

发布门以普通构建 `REAL_COUNT=3` 观察真实客户端时序，同时由 release/CI 的全仓 race suite 独立裁决 Go 数据竞争；三断点真实 CLI 门也额外在 `-race -count=1` 下通过。race 曾放大前一个辅助 completion 的 durable commit 延迟，并暴露“终态已经提交、但下一 Inbox 仍显示忙碌”的过期文案；成功 ACK 现在会刷新为可接单状态，且有确定性回归。它没有改变 fail-closed 背压、assignment 或 outbox 语义。

## Outbox 损坏

worker 会逐条解码 durable outbox。单条损坏时，它在同一 SQLite 事务中把原始 assignment/payload 移入 `worker_outbox_quarantine`，再从发送队列删除；健康事件继续发送。TUI 会持续显示损坏数量、有限个 event ID 和数据库路径，不把它伪装成网络断线。

处理步骤：

1. 停止对应 worker，并备份整个 outbox 数据库。
2. 只在受控主机查看 `worker_outbox_quarantine` 的 `event_id`、`reason` 与时间；assignment/payload 可能包含客户内容，不要贴到工单或公共日志。
3. 对照 gateway 的 task/request 终态和客户 Agent 工作区，裁决该副作用是否已经发生。
4. 保留隔离行作为证据；确认完成后才人工删除，并用新的 event/tool-call ID 继续。系统不会猜测损坏内容或静默重发。

## 当前部署边界

- 官方持久实现是单实例 SQLite；多进程共享同一 DB、网络文件系统和自动 failover 尚不在承诺内。
- 公共 `gateway` package 要求 embedder 显式给出 `DatabasePath`，避免两个宿主意外共享用户级数据库。
- Unix 文件型 SQLite 拒绝 symlink、特殊文件和多 hardlink；默认用户数据目录是 mode `0700`。Windows 依赖 `%LOCALAPPDATA%` 继承 ACL，使用 `HUMAN_DATA_HOME` 覆盖到共享目录前必须由部署方核验 ACL。
- `human doctor` 是本地 onboarding 与运行状态检查，不替代 SQLite 备份、TLS 证书监控或外部进程监督。
