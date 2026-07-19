# Go 库嵌入

Human 的进程形态不是封闭产品边界。`human local`、`human gateway`、`human worker` 是 HumanLLM 的官方 CLI 装配；库同时提供 `human.NewLLM()` 与 `human.NewAgent()` 两个根 facade，宿主可以保留自己的 listener、身份系统、路由、终端布局与进程生命周期。长期公共边界是“正确性内核 + 可替换 ports + 官方 adapters”，完整原子性、资源所有权、Protector 与 conformance 要求见 [10 · Human Framework 扩展合同](10-framework-contract.md)。

## 根 facade 与可组合 package

| package | 宿主获得什么 | 宿主仍负责什么 |
|---|---|---|
| `human` | `NewLLM` / `NewAgent` 两个明确命名的 lifecycle 构造器 | 选择并装配 HTTP/A2A/自定义 transport；根包不复制下层所有 DTO |
| `llm` | HumanLLM transport-neutral correctness core、Store/Codec/策略与 caller/worker endpoint ports | 注入 Store/DeploymentID，启动 caller/worker transport，管理关闭顺序 |
| `llm/sqlite` | 官方单进程 HumanLLM Store adapter | 私有路径、备份、owner 生命周期；也可完全替换为自有 Store |
| `llm/callerhttp` | 可挂载到自有 mux 的显式 route/auth/resolver HTTP adapter | listener/TLS、用户体系、route 前缀与 server timeout |
| `llm/workerws` | worker WebSocket transport/client 与 durable Journal contract | worker auth、Journal 资源、公开地址与网络边界 |
| `protect` / `protect/aead` | provider-neutral Protector/KMS port 与官方 AES-256-GCM adapter | key 来源、轮换、历史 key 可用性与 KMS/HSM 生命周期 |
| `agent` | durable Task/Context/Message/Event、content-only 或 Artifact Submission、命令幂等、Task revision CAS、commit-time lease/fence、原子 claim、apply receipt 与 Workspace confirmed-head CAS | 从已认证 principal 构造 AuthorityID；决定调度策略并为远程 worker 提供 transport/outbox |
| `agent/sqlite` | 官方单进程 HumanAgent Store adapter | 私有路径、备份、owner 生命周期；也可完全替换为自有 `agent.Store` |
| `a2a` | 官方 A2A 1.0 HTTP+JSON caller handler、Agent Card/extension 合同、SSE subscribe、Workspace Artifact 与 apply-receipt 映射 | 认证、Authority 和 Workspace 路由、HTTP listener/TLS；它不是 Human worker transport |
| `local` | loopback listener、gateway、SQLite、worker 与官方 TUI 的一体化实例 | 把 `BaseURL()` 和 `CallerToken()` 直接交给进程内 Agent 客户端；默认临时凭据在 `Close` 时撤销，如需跨重启复用须显式选择 preserve 并负责加密或 mode `0600` 的持久化 |
| `gateway` | completion 状态机、SQLite 恢复、模型 HTTP handler、worker WebSocket handler、内建 token 或自定义认证入口 | listener、路由前缀、TLS、反向代理、HTTP 超时、身份验证、secret 管理和优雅关闭 |
| `worker` | WebSocket 重连、durable outbox、worker state、Live Workspace mirror 与可运行/可组合的 Bubble Tea model | worker credential 的安全取得与保存、mirror 路径、外层终端程序和关闭时机 |
| `workspace` | opaque Revision/Digest/Payload/ApplyDecision，以及 transport-neutral `Store` / `ApplyIntent` / `CASApplier` ports | 实际文件树 fingerprint/CAS 与副作用授权；HumanLLM 尚未接入这条 revision chain |
| `workspace/sqlite` | 官方单 owner `workspace.Store` adapter | 独立路径、备份与 Resource 生命周期；也可完全替换为自有 Store |

`human.NewLLM` 现在直接构造 `llm.Service`，不是 `gateway.Open` 或 `local.Open` 的别名。
它不监听端口、不创建 SQLite、HTTP/WebSocket、凭据或 TUI；构造 context 只约束验证、
Store binding 与恢复，返回后的生命周期由 operation context 和 `Shutdown` 控制。
`human.NewAgent` 同样只打开 durable Agent 领域对象，不监听、不认证 caller；官方 A2A
caller 由 `a2a.NewHandler` 单独装配。Task 等领域类型来自 `human/agent`，避免根包复制
第二套 DTO。

## 嵌入 HumanLLM 框架

最小 composition 必须显式选择持久身份：

```go
store, err := llmsqlite.Open(ctx, llmsqlite.Config{Path: path})
config := human.DefaultLLMConfig() // 只填入三个 built-in codecs
config.DeploymentID = "my-human-deployment"
config.Store = store               // owned Resource 转交给 HumanLLM
service, err := human.NewLLM(ctx, config)
```

SQLite 不是构造器要求。第三方实现直接写
`config.Store = framework.Borrow[llm.Store](myStore)`；若把关闭责任转交给 Human，则用
`framework.Own[llm.Store](myStore, release)`。Go 泛型不协变，因此这里要显式写领域接口
类型，不能把 `Resource[*MyStore]` 直接赋给 `Resource[llm.Store]`。

随后宿主分别构造并启动 `llm.CallerTransport` 与 `llm.WorkerTransport`，它们只借用
`service` 的 endpoint；HTTP adapter 本身也是 `http.Handler`，但 listener 仍由宿主拥有。
`llm/callerhttp.BuiltinRoutes()` 返回一份可修改的新鲜 route 表，不启用 User-Agent 或字段
相似度猜测。自有 transport 可以直接实现相同 port，不需要 HTTP。

[`examples/custom-framework`](../examples/custom-framework/README.md) 是完整可运行证据：
它用自行实现全部事务原语并通过公开 conformance 的单文件 Store、自有 token authenticator、非 HTTP in-process transport 和独立
`protect.Protector` decorator 组合 `human.NewLLM`。示例不引用 `internal/`，并明确展示
owned/borrowed 转移与 transport → core → Protector → Store 的关闭顺序；同包另有 Store
middleware 示例，说明如何包装宿主已有的数据库实现。

桌面一体化仍可用 `local.Open`；旧的 `gateway.Open` 仍是现有 CLI/兼容服务的一体化
composition，但它不再定义根 `NewLLM` 的库边界。

`local.Open` 适合桌面应用把“人 = LLM”直接放进本机进程。最小示例见 [`examples/embed-local`](../examples/embed-local/main.go)：它使用 `local.DefaultConfig()`，因此数据库、outbox 和 TUI state 位于 OS 用户私有数据目录并按真实 workspace 隔离，不会默认写进客户仓库。示例只打印 loopback base URL，绝不打印 caller/worker token；宿主应把 `CallerToken()` 在内存中直接注入自己的 Agent 客户端。库自行签发的凭据默认采用 `IssuedCredentialsRevokeOnClose`，所以示例反复运行不会在 SQLite 留下仍有效但宿主已丢失明文的 token。确需跨重启复用时，必须在 `Open` 前显式设置 `config.IssuedCredentialPolicy = local.IssuedCredentialsPreserve`，并把 `Credentials()` 的 caller/worker 两个 secret 作为一组持久化；`Existing*` 或 `CredentialProvider` 提供的凭据本来就由宿主拥有，不会被 `Close` 隐式撤销。下面的 `go run` 命令用于源码 checkout；二进制归档中的 examples 是可复制到宿主 Go module 的参考源码。

```sh
go run ./examples/embed-local
```

`worker.Open` 可以单独连接远程 gateway。`Run` 启动官方 TUI；`Model` 返回 `tea.Model`，可以作为更大 Bubble Tea 应用的一部分。一个 `Worker` 只拥有一次 Tea program 生命周期；`Run` 返回后如需重启 UI，应先 `Close` 再 `Open` 新实例，不能复用可能仍有旧 command 回调收尾的 model。`Close` 会取消并等待由 `Worker.Run` 启动的活动程序，再关闭网络恢复、outbox 与 state store；如果宿主取出 `Model()` 后交给自己的 Tea program，那个外部 program 的取消与等待仍由宿主负责。

## 嵌入 HumanAgent 领域

[`examples/embed-agent`](../examples/embed-agent/main.go) 展示 `human.NewAgent` 的最小装配。构造器和配置位于根包，Task/Context/命令类型位于 `agent` 包：

```go
store, err := agentsqlite.Open(ctx, agentsqlite.Config{Path: path})
config := human.DefaultAgentConfig()
config.Store = store
service, err := human.NewAgent(ctx, config)
task, err := service.CreateTask(ctx, agent.CreateTaskCommand{/* ... */})
```

其中 `agentsqlite` 是 `github.com/vibe-agi/human/agent/sqlite`。示例的固定 ID 是有意的：反复运行会走同一命令的精确 replay，而不是重复创建 Task。缺失的数据库父目录由 `agent/sqlite.Open` 以 mode `0700` 创建，SQLite 文件由 adapter 收紧为 mode `0600`；已经存在的显式父目录不会被库改权限或代替宿主审计，因此仍必须位于宿主控制的私有目录。使用自定义 `agent.Store` 时，路径、权限、备份与迁移完全由该实现和宿主负责，`NewAgent` 不知道底层数据库类型。
第三方实现的注入形状同样是 `framework.Borrow[agent.Store](myStore)` 或
`framework.Own[agent.Store](myStore, release)`。

当前实现已经包含独立 SQLite schema、命令级精确 replay/冲突、Task revision CAS、多轮 `input_required`、终态不可重开、分页 Message/Event、不可变 Artifact、final Submission 原子发布、取消/失败丢弃 frozen Artifact，以及 apply receipt 推进 Workspace confirmed head。Agent worker mutation 需携带 `LeaseGrant`，并在 effect/command receipt 的同一 Store commit 内复验 worker、fence 与 expected revision；grant 不用墙钟到期，只由显式 fence 撤销。`ClaimLease` 把最早可领 Task 的选取、fence 增加、不可变 grant 历史和 command receipt 放在同一 Store transaction。Task、final message、Submission、Artifact publish 和 command result 也在同一 Store transaction 里；官方 SQLite adapter 用 SQLite 事务兑现它，自定义 Store 必须提供等价原子性。真实文件写入不在该事务中。

caller 侧依赖的是 `workspace.Store`。官方 `workspace/sqlite.Open` 返回 owned `framework.Resource[workspace.Store]`：它在调用宿主 `CASApplier` 前先持久 exact `ApplyIntent`；已完成记录只重放结果，重启发现的 `pending` 或 applier 错误被固化为 `indeterminate`，不盲目重跑外部副作用。宿主也可以注入自己的 Store。真实 applier 必须实现 exact-base 文件树 CAS；Store 不会自己解释 bundle 或授权 shell 副作用。终态结果再通过 A2A apply-receipt extension 回传 Agent confirmed head。

Workspace 第一个 Artifact 的 `ExpectedBaseRevision` 是受信 caller adapter 提供的 bootstrap，Agent 不会自行读取或 fingerprint 客户文件树；后续 freeze 必须精确匹配已确认 head。success receipt 还必须回显并观察到 Artifact 的 exact result revision。Artifact payload 默认最大 `16 MiB`，配置硬上限 `64 MiB`；它是声明式 bundle，不得夹带隐式 shell command。

`AuthorityID` 只能从宿主已经认证的 principal 派生，不能直接相信 A2A/HTTP body 里的
tenant 字符串。领域已将 lease/fence 校验收到 commit 边界；`agent/workerws` 提供独立的
远程 assignment/event/ACK/NACK transport 与 durable Journal，宿主也可实现自己的
`agent.WorkerTransport`。官方 HumanAgent TUI 仍是产品层后续项。A2A DTO、worker
assignment 和 completion response 都不能反向进入 `agent` 领域类型。

## 嵌入 A2A caller 面

[`examples/embed-a2a`](../examples/embed-a2a/main.go) 展示 `human.NewAgent` 与 `a2a.NewHandler` 的组合：宿主提供认证回调、从认证 principal 路由 Workspace，声明 A2A 1.0 HTTP+JSON Agent Card，再把 handler 挂到自有 mux。若 operations 使用 `/human-agent/` 之类前缀，标准 Agent Card discovery 仍应单独挂在 origin 的 `/.well-known/agent-card.json`；Card 里的 interface URL 则填写外部可达的 operations URL。示例故意不启动 listener，也不伪造 Human worker；没有真正 worker 领取 Task 时，Task 保持 `submitted` 才是正确表现。

handler 支持 A2A send/get/list/cancel、SSE send streaming 和 `/tasks/{id}:subscribe`。A2A 1.0.1 的 proto 注解使用 GET，而同版本生成规范与官方 Go SDK 使用 POST，因此入口接受两者并归一到同一个订阅实现。默认 send 等到 `input-required` 或终态，`returnImmediately` 可只取当前 Task；流到 `input-required` 就按协议关闭，caller 提交回复后为后续 working/terminal 状态开启新的 send/subscribe 流，不用一条连接跨越人工等待。Agent Card 中标记 `required:true` 的 extension，客户端必须通过 `A2A-Extensions` 显式协商；不存在与公开 Card 不一致的隐藏必需项。Workspace 和 apply-receipt 是两个独立 extension，暴露 Artifact 身份不等于授权修改 confirmed head。Card 声明 apply-receipt extension 时必须同时提供 `AuthorizeApplyReceipt`，反过来也一样；`NewHandler` 会拒绝不一致配置，避免广告一个实际不存在或未授权的端点。

当前 handler 没有持久化逐 Task 的 MIME 协商，因此采用可兑现的严格合同：Card 的 `defaultOutputModes` 必须覆盖 worker 可能发布的全部 Message/Artifact；非空 `acceptedOutputModes` 必须覆盖这整组输出；per-skill input/output mode override 会在构造时被拒绝，而不是公开一份运行时无法识别的分支能力。SDK 分发前还会验证请求是单一 UTF-8 JSON 值（send/apply 必须是 object）、递归拒绝重复字段，并拒绝已知标量 query 的重复值，避免官方 SDK 的 last-wins/first-wins 宽松解析进入幂等或过滤语义。

`Config.PollInterval=0` 使用 `250ms` 的领域事件轮询；`KeepAliveInterval=0` 表示不发送 SSE keep-alive；`MaxRequestBytes=0` 使用 `8 MiB`。这些是 handler 行为，不会替宿主设置 `http.Server` 的 header/read/write/idle timeout。默认非流式 send 在 Task 暂停或终态前不会返回，因此宿主应按产品等待时长选择代理和 server deadline，而不能把普通短 API 的绝对 write timeout 原样套到 HumanAgent。

`a2a.NewHandler` 不拥有 `agent.Agent`，也不拥有 listener。宿主应先停止 HTTP 流量，再 `Close` Agent。示例的 `X-Example-*` header **只是说明回调形状，不是生产认证**。生产可在 `Authenticate` 回调中直接验证 JWT/session/mTLS；若回调信任上游身份 header，入口必须先剥离客户端同名 header、验证受信代理跳，并阻止客户端绕过代理直达 handler。

## 嵌入旧的一体化 Gateway

这一节描述现有 CLI/兼容服务使用的 turnkey `gateway` package，不是新的
`human.NewLLM` 框架边界。[`examples/embed-gateway`](../examples/embed-gateway/main.go)
展示完整装配：

1. 以 `gateway.DefaultConfig()` 为基线，选择宿主控制的绝对 SQLite 路径。
2. 注入 `Authenticator` 和 `WorkerRouter`。
3. 将 `ModelHandler()` 与 `WorkerHandler()` 分别挂到宿主 mux。
4. 先停止 HTTP listener，再取消 gateway context、调用 `gateway.Close()`，确保活跃处理与 SQLite 关闭不竞态。

```sh
# 默认写入 OS 用户配置目录下的示例专用 SQLite；也可显式给绝对路径。
HUMAN_EMBED_GATEWAY_DB=/absolute/private/path/gateway.db \
  go run ./examples/embed-gateway
```

`gateway.Open` 自己不监听端口。`ModelHandler()` 处理 `/livez`、`/readyz`、兼容别名 `/healthz`、`/v1/models`、`/v1/chat/completions`、`/v1/messages` 与 `/v1/responses`；如果挂到前缀下，宿主需要像示例一样在进入 handler 前移除前缀。`WorkerHandler()` 是独立 WebSocket handler，可挂到宿主选择的私有路径。若直接使用 `Server` 作为 `http.Handler`，这些 model/health 路径同样可用，内建 worker 路径是 `/internal/v1/worker/ws`。

三个健康端点无需 caller/worker 凭据，响应只披露依赖状态和在线 worker 数，不披露数据库路径、身份或错误正文。`/livez` 只证明进程 handler 可响应，固定把 database 标为 `unchecked`；`/readyz` 在启动恢复完成且 SQLite 可执行查询时返回 200，`/healthz` 与它语义相同。没有在线 worker 会体现在 `workers.online=0` 与 `has_online=false`，但不会令 gateway readiness 失败。

### 身份扩展点

`Authenticator` 每次收到完整的 `*http.Request`，可从宿主已经验证的 Cookie、JWT、mTLS 证书或 request context 返回：

- `PrincipalCaller`：只能调用模型端点；
- `PrincipalWorker`：只能建立 worker WebSocket；
- `SubjectID`：业务主体的稳定 opaque ID；
- `KeyID`：可选但建议提供的稳定 credential/session ID，用于限流与审计维度。

`SubjectID` 和非空 `KeyID` 必须满足 `[A-Za-z0-9][A-Za-z0-9._:-]{0,127}`。邮箱、组织路径等复杂或可变标识应先在宿主身份层映射成稳定 opaque ID，而不是直接进入协议正确性命名空间。

示例里的 `X-Example-*` header **不是认证协议，也不能直接用于生产**。它只演示“受信反向代理已完成认证，然后把结果交给 embedder”的代码形状；marker header 和 loopback 检查都不能证明请求可信。生产系统必须同时做到：外部请求无法直达 Human handler；入口先删除所有客户端自带身份 header；代理到应用这一跳用 mTLS、签名或等价机制验证；只有验证成功后才把身份写入 request context 或受保护 header。若这些条件不成立，应在 `Authenticator` 内直接验证 JWT/session/mTLS，不能信任 header。

设置自定义 `Authenticator` 后，`Issue`、`PrepareToken`、`ActivateToken` 与 `Revoke` 会返回 `ErrBuiltInAuthDisabled`。这是有意的：一个 gateway 只能有一套身份真相。未设置自定义认证时，可以使用 gateway 的内建高熵 token；宿主仍负责只在签发时安全保存明文，数据库只保留 hash。

### 路由扩展点

`WorkerRouter` 只在**新任务准入**时运行。输入包含已经认证的 caller、model、capability tier，以及 workspace、task、idempotency、harness/session、root 与 exec opt-in 等解码后的正确性身份。返回值必须是目标 worker 已认证 `Principal.SubjectID` 的精确值。

路由结果会随任务持久化；continuation、幂等重放和崩溃恢复继续使用原 owner，不会再次路由到另一位专家。因此 tenant 隔离、专家池选择、区域/技能策略都应在首次路由时做出确定决定：

- 返回 `gateway.ErrWorkerRouteDenied` 表示策略拒绝，调用方得到有限的 `403`；
- 返回其它 error 表示路由基础设施失败，gateway fail-closed；
- 返回一个当前离线的 worker subject 不会悄悄回退到别人；
- 返回空字符串才显式请求 gateway 的确定性默认 worker。

## 生命周期与数据责任

公共库不会替宿主隐式接管进程资源：

- `gateway.Server` 不拥有 listener。先停止接收 HTTP，再调用 `Close`；`Close` 会主动终止并等待 `net/http` 无法随 `Shutdown` 收口的已劫持 worker WebSocket，全部 handler 退出后才关闭 SQLite。同一个 SQLite 文件不能被多个 gateway 实例当作集群共享存储。
- `llm.Service` 不拥有 caller/worker transport 或 listener。先停止并等待两个 transport，
  再调用幂等 `Shutdown`；它等待 in-flight core operation 后先 release owned Protector，
  再 release owned Store。构造 context 取消不等于关闭已经返回的 Service。
- `agent.Agent` 不拥有 listener/transport；先停止调用它的 adapter，再调用幂等 `Close`。传入 owned Store 时由 Agent release，borrowed Store 仍由宿主在 Agent 停止后释放。官方 `agent/sqlite` 要求单 owner，且其文件不能与 gateway 或其它 Store adapter 共用。
- `a2a.NewHandler` 不拥有 listener 或 `agent.Agent`，也没有独立的 `Close`；其请求和 SSE 订阅生命周期由宿主 HTTP server 与 request context 控制。必须先停止并等待这些 handler，再关闭 Agent。
- `workspace.Store` 本身不带 lifecycle；`workspace/sqlite.Open` 返回的 owned Resource 在 `Release` 时等待活跃 `Apply`/`Lookup`，再关闭第三个独立的单 owner SQLite。`CASApplier` 可调用 `Lookup`，但不得在回调内同步 release Store，也不得递归 `Apply` 同一 identity，否则会等待自己。
- `local.Local` 拥有自己的 loopback listener、gateway 与 worker，`Close` 是幂等的，并按显式 issued-credential policy 处理库签发凭据；一个实例只运行一次 TUI，重启需重新 `Open`。
- `worker.Worker` 拥有网络客户端、outbox 与可选 state store；`Close` 取消并等待其活动 `Run`，一个实例只运行一次 TUI，重启需重新 `Open`。
- TLS 终止、代理信任、HTTP/server 超时、secret persistence、备份与数据库文件权限最终都是宿主责任。默认路径是安全起点，不会替代部署审计。

官方 gateway、`agent/sqlite`、`llm/sqlite` 与 `workspace/sqlite` 都提供 SQLite 单进程
实现，各自要求独立 schema/identity，不能共用一个文件。核心只依赖 `agent.Store`、
`llm.Store` 与 `workspace.Store`；宿主可以实现 PostgreSQL、远程 durable service 或其它
backend。第三方 Agent/LLM driver 必须满足原子性、commit-unknown、byte ownership，并在
自己的测试中运行 `humantest.TestAgentStore` / `humantest.TestLLMStore`；Workspace driver
运行 `humantest.TestWorkspaceStore`，并另测物理崩溃后 pending 的恢复。公共接口可替换
不等于官方 SQLite 已支持多实例共享。
