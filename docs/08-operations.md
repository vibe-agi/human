# 08 · 部署与运维

本页给出本机 RC 的安装/备份/升级/回滚流程、可直接执行的远程单实例接入，以及拆分部署的人工灾备边界。默认本机路径仍是 `human local`；只有 gateway 与专家 TUI 不在同一台机器时才拆分。

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

可嵌入公共栈另有一条不依赖 legacy CLI 的团队部署门：`llm.Service + callerhttp +
llm/workerws.Client` 在 host-owned HTTPS/WSS 上连接两个 tenant 与两个 durable worker；认证
回调产生的 claims 决定 admission/routing，客户端伪造 tenant header 无效；无效身份返回
401，身份提供方暂时故障返回 503。worker socket 被强制断开后，`HeaderProvider` 会在每次
重连重新取得短期凭据。另一条 WSS 门会同时更换服务端证书与客户端信任根、切断现有 socket，
并要求 durable client 通过宿主注入的 `http.Client` 建立新 TLS session；这证明 host-managed
证书/CA rollover 的扩展点可用。Human 不内置 OIDC、证书文件管理或反向代理配置；真实 IdP、
代理滚动发布、重叠信任窗口和回滚仍应在部署环境演练。

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

安装精确 OpenCode 1.17.18、Codex 0.145.0 与已支持的 Claude Code 后，先跑当前
三客户端公共/Web 门；Playwright 浏览器与破网矩阵分别独立运行：

```sh
make real-client-test REAL_COUNT=3
make real-opencode-browser-test REAL_COUNT=3
# REAL_NETWORK_DROPS defaults to 5; release repeats the whole matrix 3 times.
make real-opencode-network-test REAL_COUNT=3
# Extra race probe for the same three real-network scenarios:
HUMAN_REAL_OPENCODE_NETWORK_E2E=1 HUMAN_REAL_OPENCODE_NETWORK_DROPS=5 \
  go test -race -count=1 -timeout=8m ./local \
  -run '^TestRealOpenCodeRecoversAcrossNetworkFaultMatrix$' -v
```

### Testcontainers 协议直连 + 三客户端 + LLM 人类门

主机已安装的真实 CLI 门之外，还有两层共用同一真实 Web 人侧的可重现产品门：

1. **协议直连层**：测试调用方直接向 `local.Open` 发送 Anthropic Messages、OpenAI Chat
   Completions 和 OpenAI Responses；三种方言分别跑 `stream:false` 聚合与 `stream:true` SSE，
   不经过任何 Agent CLI。
2. **真实 Agent CLI 层**：固定版本的 Claude Code、OpenCode、Codex 作为真正调用方；三端
   共同跑 final、关闭首进程后由第二进程 resume、工具成功、工具失败后恢复 final、Web 拒单。
   Claude/OpenCode 另跑各自 exact Workspace 的文件 create→原生 write、modify→原生 edit；
   正式 Tasks 面板让 Claude 依次执行 `TaskCreate → TaskUpdate → TaskList`，OpenCode
   `todowrite` 与 Codex `update_plan` 各自走 pending→in_progress→completed。Claude/Codex 还在完整 progress
   帧后被反向代理强制断开 SSE，必须以同一 durable turn 恢复后才能收到 final。

两层都由 Playwright 操作真实页面，并由配置的主机 LLM 扮演人。测试构建两套固定镜像：
`testdata/real-clients/package-lock.json` 固定 Claude Code 2.1.217、OpenCode 1.17.18、
Codex 0.145.0 和 Node 22.22.0；`testdata/browser-human/package-lock.json` 固定
Playwright 1.61.1，并配套使用同版本官方 Chromium 镜像的 multi-arch digest。三套客户端
运行在 caller 容器，Playwright/Chromium 在独立 browser-human 容器以 headless 模式运行。
两者经 Testcontainers 的 `host.testcontainers.internal` 端口转发访问测试进程中的
`local.Open`，不使用 host network。

browser-human 不调用 Web JSON API 来绕过页面：它等待真实 inbox DOM、点击接单、从页面
读取 preview/transcript、发送一段 progress、把模型生成的回答填进 composer，再点击完成。
三套 CLI 场景分别从正式 Command 面板发 Claude `Bash`、OpenCode `bash`、Codex
`exec_command`，等待 DOM 出现 tool result 后才 final；服务端只有在 exact harness/version
和完整 behavioral schema 匹配时才投影对应 `CommandProfile`，浏览器不按名字猜工具。另一个
共同场景让命令以非零状态失败，验证失败 result 回流后仍可 final。Tasks/Plan 场景同样不绕到高级 JSON：它从正式面板添加
任务、切换三态并逐次同步；Claude 还从 `TaskCreate` result 恢复 task ID 后执行
`TaskUpdate` 和 `TaskList`。Live Workspace 场景由浏览器容器在 mirror
里先创建、再修改同一文件，页面逐次点击交付；Claude 必须实际执行 `Write`/`Edit`，OpenCode
必须执行 `write`/`edit`，最后都从 caller bind mount 按字节验证第二版内容。
主机上的短生命周期 authenticated relay 把页面上下文交给 OpenAI-compatible Chat
Completions 模型作为“人”；外部模型密钥不会进入任一容器。

默认配置正好对应本机 `dashscope:glm-5`：

```sh
export HUMAN_TEST_LLM_API_KEY='从秘密管理器读取，不要写进仓库或 shell history'
make real-container-client-test REAL_COUNT=1

# 非默认 endpoint/model：
make real-container-client-test \
  REAL_LLM_BASE_URL=http://127.0.0.1:23333 \
  REAL_LLM_MODEL=dashscope:glm-5
```

`HUMAN_TEST_LLM_BASE_URL` 默认是 `http://127.0.0.1:23333`，测试会规范化为
`/v1/chat/completions`；也可直接给以 `/v1` 或完整 endpoint 结尾的 URL。
`HUMAN_TEST_LLM_API_KEY` 只存在于主机 Go 测试进程：不会进入 Docker build context、
容器环境、OpenCode/Codex 配置或测试日志。容器只收到每次 `local.Open` 新发的临时 caller
token。测试启动 Docker 前先调用一次外部模型 preflight，避免构建完镜像才发现 endpoint
或授权无效。

| 容器客户端 | Human wire | 三端共同证据 | 额外证据 |
|---|---|---|---|
| Claude Code 2.1.217 | Anthropic Messages | progress/LLM final；第二进程 `--resume`；`Bash` 成功与失败 result→final；拒单 | exact 双 session UUID；Tasks 三态 + List；mirror create→`Write`、modify→`Edit`；partial SSE 后同 body/session/key 恢复 |
| OpenCode 1.17.18 | Chat Completions | progress/LLM final；第二进程按 session ID 恢复；`bash` 成功与失败 result→final；拒单 | `todowrite` 三态；mirror create→`write`、modify→精确 old/new `edit`，最终字节实证 |
| Codex 0.145.0 | Responses | progress/LLM final；第二进程 `exec resume --last`；`exec_command` 成功与失败 result→final；拒单 | `update_plan` 三态；partial SSE 后按冻结的 retry profile 恢复；模型目录启用 freeform 时 mirror create/modify→native `apply_patch`，最终字节实证 |

部分 SSE 门不是简单观察“CLI 打印了 Reconnecting”：浏览器发 progress 后会停在一个短生命周期、
带随机 bearer 的 loopback 屏障，直到故障代理确认 retry 命中同一服务端幂等键与同一 Claude
session/Codex turn metadata 才允许发送 final。Claude retry body 必须完全一致。Codex 0.145.0
会重排 `client_metadata`，并可能在 retry 时省略其中与 canonical turn metadata 重复的
`session_id`；测试只放行这一个冻结差异，prompt、tools 或其它字段变化都会失败。

协议直连层另有官方 Go SDK contract 测试覆盖三方言工具/result continuation 与标准错误
envelope；该容器产品门专注证明三种真实 wire 的 aggregate/stream 都会进入同一个 Web+LLM
人侧。CLI 层三端还各有 Web 拒单 unhappy path。流式客户端是在 HTTP 200 之后收到终态错误，不同 CLI
会选择退出、重试或等待，因此测试让浏览器拒绝所有可见重试、禁止出现成功 marker，并用
25 秒进程边界保证 CI 可终止；这项证据不外推为“三个上游 CLI 都会立即自行退出”。

Codex 在普通 Docker 中不能创建 `bwrap` 所需的 unprivileged user namespace，因此该子进程
使用官方的 externally-sandboxed 模式关闭内层 sandbox。安全边界是临时容器本身；它只
bind mount 本次测试的临时 workspace，未挂载仓库、Docker socket、主机 home 或外部 LLM
密钥。Claude/OpenCode 不使用这个开关。

普通 CI 若只需验证镜像、端口转发、协议直连、Web 编排和三套 CLI tool continuation，可运行不访问
外部模型的显式自检门：

```sh
make container-client-harness-test
```

该目标设置 `HUMAN_TEST_LLM_FAKE=1`，不能作为真实 LLM 证据。受限网络可以预构建镜像后
通过 `HUMAN_TEST_CLIENT_IMAGE=registry.example/human-real-clients@sha256:...` 与
`HUMAN_TEST_BROWSER_IMAGE=registry.example/human-browser@sha256:...` 复用；测试仍会在容器
启动后逐一执行 CLI 与 Playwright `--version`，版本不符立即失败。默认构建固定基础镜像
digest，并对 npm 大包下载启用有界重试；它不要求 Testcontainers 的 Docker API builder
支持 BuildKit，构建失败会附最后 32 KiB 日志。首次拉取 Playwright/Chromium 镜像较大，
Make 目标因此给整个冷构建门 25 分钟，单次对话本身仍有 2 分钟硬超时。升级任一 CLI
时，必须同时更新 package/lock、Docker 构建期版本断言、测试运行期版本断言，以及相应
harness resolver 的 exact-version profile，不能只改镜像标签后把旧 profile 证据外推。

网络门在 gateway 已接单但下游尚未收到 response headers、完整 stream-start 首帧后、完整 Human progress 帧后三个场景并行运行。每个场景连续主动断 TCP 5 次，第 6 次必须以相同 request body 与 `X-Session-Id` 命中同一个 Human idempotency key 和唯一 assignment，再正常完成；三个并行场景单轮约 70 秒。当前 Makefile 默认 `REAL_NETWORK_DROPS=5`，release 使用 `REAL_COUNT=3` 重复整套门。它验证真实 OpenCode 的传输重试；测试本身不重启 gateway、worker 或 caller，不能写成真实进程恢复顺序已通过。内部 `fault-test` 继续负责项目内 worker/gateway/SQLite 重启、三方重叠离线和 outbox 精确重放。

发布门以普通构建 `REAL_COUNT=3` 观察真实客户端时序，同时由 release/CI 的全仓 race suite 独立裁决 Go 数据竞争；三断点真实 CLI 门也额外在 `-race -count=1` 下通过。race 曾放大前一个辅助 completion 的 durable commit 延迟，并暴露“终态已经提交、但下一 Inbox 仍显示忙碌”的过期文案；成功 ACK 现在会刷新为可接单状态，且有确定性回归。它没有改变 fail-closed 背压、assignment 或 outbox 语义。

## 本机离线备份与恢复（public v3）

当前 `human local` 的恢复单元由同一 workspace scope 的四部分组成：

1. `llm.Service` 的 `store.db`；
2. mode `0600` 的单 caller `credentials.json`（没有 worker token 或 rotation pair）；
3. 可选 `workerkit-state.db`；
4. `--workspace` 指定的 Human 基础目录（含默认 session 子目录）与启用 state 时的
   `workerkit-mirror-baseline.json`。Web 选择的基础目录之外的已有 repo 只保存路径引用，
   不复制进 local archive；repo 内容应由 Git/项目自己的灾备负责。

归档格式固定为 `human-local-backup` v3。backup 必须在 local 完全停止后运行；命令按
canonical path 排序取得 service/state SQLite owner lock，用 `VACUUM INTO` 生成自包含
快照，并用真正的 `llm/sqlite`、`workerkit/sqlite` adapter 验证 staged schema。manifest
逐项绑定 path/type/mode/size/SHA-256，reader 拒绝未声明/重复/缺失项、case/Unicode
可移植碰撞、路径穿越、第二 gzip member、尾随数据与越界解压；credential 还会按公共
单-token 格式重新解析。archive 在 Unix 必须为 `0600`。

restore 先完整 extract+verify，再在每个目标的同目录建立 new/old staging，fsync
restore journal 后逐项 rename。安装后的 SQLite、credential 和 tree digest 全部通过才
删除旧集；任一边界中断都会让普通 `human local` fail closed，保持停机并运行
`human local --workspace ~/human-workspace restore --resume` 收口。`--force` 只在人工确认后替换完整
目标集；workspace scope 不同时还需 `--accept-workspace-mismatch`，caller/worker stable
identity 仍必须相同。自定义路径只包括 `--db`、`--credentials`、`--state-db`；
subject 也必须与运行时一致。本地格式没有 `--mirror-root` 或 `--outbox`。

下面的命令适用于当前 v3：

```sh
install -d -m 0700 ~/Backups
BACKUP="$HOME/Backups/human-local-$(date +%Y%m%d-%H%M%S).tar.gz"

human local --workspace ~/human-workspace backup \
  --output "$BACKUP"

human local verify-backup \
  --input "$BACKUP"
```

上面只完成备份与验证。恢复是独立维护操作：保持 local 停止，并对一个空目标运行 `human local --workspace . restore --input /path/to/verified-human-local.tar.gz`。不要紧接着对刚完成备份的同一非空 scope 执行 restore。

### 历史 v2 gateway/outbox 格式（仅供旧 binary 运维）

以下说明只适用于迁移前的旧 binary 和它生成的 v2 archive。当前 binary 不读取该格式；
若必须恢复旧 archive，保留旧 binary，在隔离目录恢复并导出业务数据，不能把旧
gateway/outbox 文件混入 public v3 目标。

`backup` 与 `restore` 按 canonical path 排序、非阻塞抢占 gateway、outbox 和启用时 state DB 的全部 owner lock；运行中的 local/gateway、独立 worker 或 direct token administration 会明确失败，绝不边写边复制，也不会因不同路径顺序形成 AB/BA 死锁。对 gateway、outbox 和启用时的 state DB，backup 先在源库执行 `PRAGMA quick_check`，再用 SQLite `VACUUM INTO` 生成自包含快照，随后对快照再次 quick-check。它因此吸收已经提交在 WAL/rollback sidecar 中的状态，而不是错误地只复制主文件。`verify-backup` 对归档里的每个 SQLite 再执行 quick-check，并把 manifest 的 gateway identity/worker subject 与 outbox、state 中全部 correctness row 交叉验证；任何异域 namespace 都拒绝，而不是把别人的 pending/state 一起恢复。restore 在 staging 和整套安装后各执行一次 quick-check，并把旧 `-journal/-wal/-shm` 一并纳入可回滚的“应当不存在”组件，绝不让旧 sidecar 套到新主库上。

archive 在 Unix 固定为 mode `0600`，因为 manifest 旁边包含明文 caller/worker secret；Windows 依赖目标目录 ACL，运维方必须单独核验。v2 manifest 固定 core layout，并用 `mirror/workspace` 与 `mirror/state` 承载且只承载 manifest `workspace_scope` 对应的一棵 worktree 和 `.human-state` 树；旧 v1 caller-wide archive 明确拒绝，没有兼容读取。每个目录/文件都记录规范 path、type、mode、size 和 SHA-256；reader 拒绝未知顶层、额外/缺失 tar 项、重复与 Unicode/case 可移植碰撞、路径穿越、损坏 gzip checksum、第二 gzip member/尾随数据、过量 entry 或超过 64 GiB 的声明解压。credential journal 不只做 JSON 校验：active caller/worker secret 的 SHA-256、key ID、principal type、subject 和未吊销状态必须与同一 gateway snapshot 的 `api_tokens` 精确匹配。mirror 的 symlink、socket/FIFO 等特殊节点不跟随、不写回，而是逐项记录到 manifest `skipped`；所选 workspace 的 regular worktree 和 `.human-state` 正确性数据仍完整归档。

这里的 SHA-256 与 SQLite quick-check 是结构/内容完整性检查，不是签名、MAC 或来源认证。`verify-backup` 只能证明一份 archive 内部自洽；若攻击者能同时重写 payload 和 manifest，它不会把该 archive 变成可信来源。除 `0600` 外仍应使用受控备份目录、加密介质或另行签名，并在 restore 前确认来源。

restore 默认拒绝 gateway/credentials/outbox/state 或所选 workspace 的 mirror worktree/`.human-state` 中任一非空目标；同一 caller 的 sibling workspace 既不会使普通 restore 失败，也不在 `--force` 的替换范围内。只有人工核对所选目标后才能用 `--force` 整套替换：

```sh
human local --workspace . restore \
  --input /path/to/verified-human-local.tar.gz \
  --force
```

实现先把每个 component 写到其目标目录内的私有 staging，全部校验后 fsync 一个固定格式的 restore journal，再逐 component 做 `old/new` rename；旧集一直保留到所有新 SQLite、credential binding 和 mirror tree digest 都通过。进程在任一 rename 边界退出时，journal 仍在，普通 `human local` 会 fail-closed 拒绝启动混合状态。不要手工删 journal/staging，保持 local 停止并运行：

```sh
human local --workspace . restore --resume
```

恢复到另一个 canonical gateway DB 路径时，restore 会在私有 staging 内把 outbox 与 worker-state 的 transport gateway identity 从 archive 值事务重绑到目标值；整套 gateway/outbox/state 同时提交，所以 pending/state 不会因路径 hash 改变而静默隔离。恢复到不同 Human 基础目录时 workspace SHA-256 会不同，必须先确认 archive，再显式增加 `--accept-workspace-mismatch`；caller/worker subject 不能用该参数绕过，必须与归档身份一致。这个开关只表示“已人工确认是同一逻辑 workspace 的搬迁/取证”，不会机械重写已有 task 的 `workspace_key`。恢复后若某会话原先选择的外部 repo 不存在，Web 会把它标成 unavailable；Human 必须显式选择新 repo，系统不会猜路径。自定义 `--db`、`--credentials`、`--state-db` 和 subject 参数是 `local` persistent flags，backup/restore 必须传入与运行时完全相同的值。state-disabled archive 会把目标 state 视为应当不存在；state-enabled archive 要求非空 `--state-db` 目的地。

## 安装、升级、回滚与卸载

当前 v0.x 是 clean-break：gateway、outbox/state 与 caller ledger 都只接受唯一的当前 schema，**没有迁移器**。升级前必须保留旧 binary 和由旧 binary 生成并验证的 archive；不能假设新 binary 会迁移旧库。未来若改变 schema，发行必须同时提供受支持的迁移路径，或明确声明新的 break/recreate 边界。

首次安装在校验 Release `checksums.txt` 后，也应先把 binary 写成目标目录内的 `human.new`，再同目录 rename 为 `human`，最后运行 `human version --json` 和 `human doctor --workspace . --require-opencode`。一旦产生本机状态，后续替换一律视为升级并执行下面的备份步骤。

本机 RC 路径按下面顺序升级；示例假设 binary 位于用户自己的 `~/.local/bin`，自定义状态参数必须与平时启动完全相同：

1. 停止 OpenCode、`human local` 及任何独立 gateway/worker/token-admin，确认备份命令能取得全部 owner lock。
2. 在 mode `0700` 目录保存旧 binary，执行 `backup` 并立即 `verify-backup`。
3. 校验 Release 的 `checksums.txt`，把新 binary 先写到目标目录内的临时文件，再以同目录 rename 原子替换。
4. 运行 `version --json` 与 `doctor`；随后按原方式启动 local，再复查 doctor/readiness。任何检查失败都先停机，不让新旧 binary 交替写同一状态。

```sh
WORKSPACE="$(pwd -P)"
HUMAN_BIN="$HOME/.local/bin/human"
BACKUP_DIR="$HOME/Backups/human/upgrade-$(date +%Y%m%d-%H%M%S)"
NEW_BINARY=/path/to/verified-release/human

install -d -m 0700 "$BACKUP_DIR"
install -m 0755 "$HUMAN_BIN" "$BACKUP_DIR/human.previous"
"$HUMAN_BIN" local --workspace "$WORKSPACE" backup \
  --output "$BACKUP_DIR/local-before-upgrade.tar.gz"
"$HUMAN_BIN" local verify-backup \
  --input "$BACKUP_DIR/local-before-upgrade.tar.gz"

install -m 0755 "$NEW_BINARY" "$HUMAN_BIN.new"
mv -f "$HUMAN_BIN.new" "$HUMAN_BIN"
"$HUMAN_BIN" version --json
"$HUMAN_BIN" doctor --workspace "$WORKSPACE" --require-opencode
```

离线 `doctor` 只完成静态与状态检查；gateway 未启动会显示 WARN。随后在一个终端按原参数启动 `"$HUMAN_BIN" local --workspace "$WORKSPACE"`，再从第二个终端重跑 `doctor`，并确认 `curl -fsS http://127.0.0.1:19080/readyz` 成功，才算新 binary 真正通过启动验收。

失败回滚必须保持所有进程停止，用保留的旧 binary 原子换回，再由**旧 binary**恢复升级前 archive；目标已有状态时需要人工确认后使用 `--force`。若 restore 报告中断 journal，继续用旧 binary 执行 `restore --resume`。

```sh
install -m 0755 "$BACKUP_DIR/human.previous" "$HUMAN_BIN.rollback"
mv -f "$HUMAN_BIN.rollback" "$HUMAN_BIN"
"$HUMAN_BIN" version --json
"$HUMAN_BIN" local --workspace "$WORKSPACE" restore \
  --input "$BACKUP_DIR/local-before-upgrade.tar.gz" --force
"$HUMAN_BIN" doctor --workspace "$WORKSPACE" --require-opencode
```

回滚后的最终验收同样必须实际启动旧 binary 的 `human local`，再从另一终端检查 `doctor` 与 `/readyz`；离线 WARN 不能替代启动验证。

卸载同样先停机并做最后一次 `backup + verify-backup`。默认只删除 binary 和显式生成的客户端配置，保留私有数据目录与 archive；只有确认 archive 可读且不再需要回滚后，才单独人工删除状态。删除 binary 不会自动吊销远程 token，远程部署应先按 token runbook 吊销。

## 远程拆分灾备

远程 gateway、一个或多个 worker、caller 配置分布在不同主机，目前**没有单命令的全局一致备份**。`human local backup/restore` 只适用于一个 local workspace scope，不能套到拆分部署；以下是人工维护窗口流程，尚未作为自动灾备门验证：

1. 先停止 caller/harness 的新请求，再协调停止所有 workers 和 gateway；确认进程与 token-admin 都已退出，避免分散快照期间继续写入。
2. gateway 主机用 SQLite-aware backup 或停机文件系统快照保存 gateway DB，并执行 `PRAGMA quick_check`；同时保存 TLS/代理配置和外部保管的 caller/worker token 文件。token 明文不在 gateway DB 中，二者缺一不可。
3. 每台 worker 分别保存 outbox DB、可选 state DB、Human workspace base/baseline、
   worker token/config；记录每个会话选择的外部 repo 路径、规范化 gateway endpoint、
   稳定 worker subject、时间与校验和。外部 repo 内容仍由 Git/项目自己的灾备负责。
4. 恢复时先恢复并启动 gateway，确认 `/readyz` 可查询（此时零 worker 可以只是状态提示）；再按原 endpoint/subject 恢复并启动每台 worker，核对 online、outbox 与 quarantine；最后恢复 caller token/config 并启动 OpenCode。
5. 若任一步身份、路径或数据库校验不一致，保持下游停止并回到同一组快照，不猜测重绑或部分拼接。跨主机快照是否处于同一业务时点由维护者负责，当前实现不提供分布式事务或自动一致性证明。

## Outbox 损坏

worker 会逐条解码 durable outbox。单条损坏时，它在同一 SQLite 事务中把原始 assignment/payload 移入 `worker_outbox_quarantine`，再从发送队列删除；健康事件继续发送。TUI 会持续显示损坏数量、有限个 event ID 和数据库路径，不把它伪装成网络断线。

处理步骤：

1. 停止对应 worker，并备份整个 outbox 数据库。
2. 只在受控主机查看 `worker_outbox_quarantine` 的 `event_id`、`reason` 与时间；assignment/payload 可能包含客户内容，不要贴到工单或公共日志。
3. 对照 gateway 的 task/request 终态和客户 Agent 工作区，裁决该副作用是否已经发生。
4. 保留隔离行作为证据；确认完成后才人工删除，并用新的 event/tool-call ID 继续。系统不会猜测损坏内容或静默重发。

## 当前部署边界

- 当前 RC 的可交付范围是 OpenCode 1.17.18 单机 `human local`；远程拆分流程是显式运维边界，不代表多租户、自动 failover 或全局一致灾备已经产品化。
- 官方持久实现是单实例 SQLite；多进程共享同一 DB、网络文件系统和自动 failover 尚不在承诺内。
- 公共 `gateway` package 要求 embedder 显式给出 `DatabasePath`，避免两个宿主意外共享用户级数据库。
- Unix 文件型 SQLite 拒绝 symlink、特殊文件和多 hardlink；默认用户数据目录是 mode `0700`。Windows 依赖 `%LOCALAPPDATA%` 继承 ACL，使用 `HUMAN_DATA_HOME` 覆盖到共享目录前必须由部署方核验 ACL。
- `human doctor` 是本地 onboarding 与运行状态检查，不替代上述经校验的离线 archive、TLS 证书监控或外部进程监督。
