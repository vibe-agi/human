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
# token administration now shares the gateway database owner lock; stop the
# gateway process before this direct SQLite administration command.
human gateway --db /var/lib/human/human.db token revoke \
  --key-id "$(jq -r .key_id /var/lib/human/worker-credential.json)"
```

worker 凭据轮换应给新 token 保持相同的稳定 worker subject，并在确认新凭据可认证后再吊销旧 key。durable outbox 的命名空间来自认证 hello 中的 worker subject 与规范化 gateway endpoint，不来自 token 文本；因此同一 gateway/subject 下，旧 token 时已落盘但未 ACK 的事件会由新 token 连接继续补发。改变 subject 或 endpoint 会得到隔离命名空间，不会跨租户猜测重放。

## 健康检查

gateway 提供三个免认证、无敏感路径或身份信息的 JSON 端点：

- `GET /livez` 只证明 HTTP handler 活着，始终返回 200；为避免把依赖检查混进 liveness，`database.status` 为 `unchecked`。
- `GET /readyz` 只有在启动恢复已经完成且 SQLite 当前可执行查询时返回 200，否则返回 503。
- `GET /healthz` 是 `/readyz` 的兼容别名，状态码与 JSON 语义一致。

典型 ready 响应：

```json
{
  "status": "ok",
  "database": {"status": "ok"},
  "recovery": {"complete": true},
  "workers": {"online": 0, "has_online": false}
}
```

worker 离线不会让 gateway 自身变成 not-ready；它会保留 HTTP 200 并明确报告 `online: 0`，此时新的模型请求会得到 `worker_unavailable`。监控应分别告警 `/readyz` 非 200 与 `workers.has_online=false`。`human doctor` 默认检查 `http://127.0.0.1:19080/readyz`：前者是 FAIL，后者只是 WARN。不要把 `/livez` 用作流量就绪探针。

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

## 本机离线备份与恢复

`human local` 的可恢复单元不是一个裸 `gateway.db`，而是同一 workspace scope 的五组状态：gateway SQLite、mode `0600` credential rotation journal、worker outbox、可选 worker TUI state，以及本地 caller subject 对应的 mirror worktree 与 `.human-state` baseline/blob/delivery intent。必须在 local 完全停止后把它们作为一套处理：

```sh
human local --workspace . backup \
  --output ~/Backups/human-local-$(date +%Y%m%d).tar.gz

human local verify-backup \
  --input ~/Backups/human-local-20260718.tar.gz

human local --workspace . restore \
  --input ~/Backups/human-local-20260718.tar.gz
```

`backup` 与 `restore` 按 canonical path 排序、非阻塞抢占 gateway、outbox 和启用时 state DB 的全部 owner lock；运行中的 local/gateway、独立 worker 或 direct token administration 会明确失败，绝不边写边复制，也不会因不同路径顺序形成 AB/BA 死锁。对 gateway、outbox 和启用时的 state DB，backup 先在源库执行 `PRAGMA quick_check`，再用 SQLite `VACUUM INTO` 生成自包含快照，随后对快照再次 quick-check。它因此吸收已经提交在 WAL/rollback sidecar 中的状态，而不是错误地只复制主文件。`verify-backup` 对归档里的每个 SQLite 再执行 quick-check，并把 manifest 的 gateway identity/worker subject 与 outbox、state 中全部 correctness row 交叉验证；任何异域 namespace 都拒绝，而不是把别人的 pending/state 一起恢复。restore 在 staging 和整套安装后各执行一次 quick-check，并把旧 `-journal/-wal/-shm` 一并纳入可回滚的“应当不存在”组件，绝不让旧 sidecar 套到新主库上。

archive 本身固定 mode `0600`，因为 manifest 旁边包含明文 caller/worker secret。v1 manifest 固定 core layout，并为每个目录/文件记录规范 path、type、mode、size 和 SHA-256；reader 拒绝未知顶层、额外/缺失 tar 项、重复与 Unicode/case 可移植碰撞、路径穿越、损坏 gzip checksum、第二 gzip member/尾随数据、过量 entry 或超过 64 GiB 的声明解压。credential journal 不只做 JSON 校验：active caller/worker secret 的 SHA-256、key ID、principal type、subject 和未吊销状态必须与同一 gateway snapshot 的 `api_tokens` 精确匹配。mirror 的 symlink、socket/FIFO 等特殊节点不跟随、不写回，而是逐项记录到 manifest `skipped`；regular worktree 和 `.human-state` 正确性数据仍完整归档。

这里的 SHA-256 与 SQLite quick-check 是结构/内容完整性检查，不是签名、MAC 或来源认证。`verify-backup` 只能证明一份 archive 内部自洽；若攻击者能同时重写 payload 和 manifest，它不会把该 archive 变成可信来源。除 `0600` 外仍应使用受控备份目录、加密介质或另行签名，并在 restore 前确认来源。

restore 默认拒绝 gateway/credentials/outbox/state 或该 caller mirror scope 中任一非空目标。只有人工核对后才能用 `--force` 整套替换：

```sh
human local --workspace . restore \
  --input ~/Backups/human-local-20260718.tar.gz \
  --force
```

实现先把每个 component 写到其目标目录内的私有 staging，全部校验后 fsync 一个固定格式的 restore journal，再逐 component 做 `old/new` rename；旧集一直保留到所有新 SQLite、credential binding 和 mirror tree digest 都通过。进程在任一 rename 边界退出时，journal 仍在，普通 `human local` 会 fail-closed 拒绝启动混合状态。不要手工删 journal/staging，保持 local 停止并运行：

```sh
human local --workspace . restore --resume
```

恢复到另一个 canonical gateway DB 路径时，restore 会在私有 staging 内把 outbox 与 worker-state 的 transport gateway identity 从 archive 值事务重绑到目标值；整套 gateway/outbox/state 同时提交，所以 pending/state 不会因路径 hash 改变而静默隔离。恢复到不同 real Git 路径时 workspace SHA-256 会不同，必须先确认 archive，再显式增加 `--accept-workspace-mismatch`；caller/worker subject 不能用该参数绕过，必须与归档身份一致。这个开关只表示“已人工确认是同一 workspace 的搬迁/取证”，不会机械重写 gateway task 或 pending assignment 里的旧 `workspace_key`、root、tool arguments。恢复后先审阅旧 in-flight scope，不要让旧 edit 在未经确认时落到新根。自定义 `--db`、`--credentials`、`--outbox`、`--state-db`、`--mirror-root` 和 subject 参数是 `local` persistent flags，backup/restore 必须传入与运行时完全相同的值。state-disabled archive 会把目标 state 视为应当不存在；state-enabled archive 要求非空 `--state-db` 目的地。

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
- `human doctor` 是本地 onboarding 与运行状态检查，不替代上述经校验的离线 archive、TLS 证书监控或外部进程监督。
