# 09 TLA+ 模型与实现约束

## 1. 裁决

Human 下保留两个独立 public surface：

- `HumanLLM`：实时、增量、response-centric。目标 Go API 是 `human.NewLLM()`；当前 completion gateway、三方言 codec、TUI 和 Live Workspace 都属于这一面。
- `HumanAgent`：任务型、durable Task/Context、最终打包 Submission/Artifact。目标 Go API 是 `human.NewAgent()`；它可以采用 A2A-like transport，但不能复用 completion 的“一个 HTTP response 就是任务生命周期”假设。

两者共享 identity、auth、queue/lease、worker transport、durable outbox、workspace engine 和 observability；不共享 public lifecycle、terminal 语义或 wire DTO。共享的是机制，不是把两个状态机揉在一起。

## 2. 关键语义

### HumanLLM

一次 completion 必须先 durable admission 与 HTTP decision，之后才能暴露 stream frame。`text_final`、`tool_calls`、`clarification`、`error` 都关闭当前 response；其中只有 final/error 结束 logical task。`tool_calls` 等待 caller 在下一 completion 回传 result；clarification 等待 caller 在下一 completion 回话。失败 result 被保留但不推进该 logical task 的 confirmed-result baseline；跨 Task、跨 surface 的全局 workspace CAS 由 Runtime/System 模型负责。同一 request key + digest 必须重放相同抽象 trace，同 key + 不同 digest 必须 409/明确冲突。

### HumanAgent

Task 同时绑定两个正交身份：Context 只负责会话/展示分组，Workspace 才是 CAS、draft 和 baseline 的正确性边界；同一 Context 可以并行多个独立 Task。Task 状态为 submitted/working/input-required/terminal；`input_required` 可以多轮往返。Task 一旦 completed/canceled/rejected/failed 就不可重开，caller 的后续消息在同 Context 创建新 Task。Human 可以只提交 content；若提交 workspace Artifact，则冻结的 base/version/payload 不可变，final Submission 可见与 Task completed 必须原子发生。caller 对 Artifact 的 apply success/conflict/rejected/indeterminate 是独立 receipt；只有匹配的 success 推进 baseline。不同 Context 可以共享同一 Workspace，此时它们必须竞争同一条 CAS 写链，而不能各自成功。

模型中的 `ContextIds` 与 `WorkspaceIds` 是经过认证主体限定后的 opaque scope（例如 `principal + external_id`），不是直接信任客户传入的裸字符串。未来 `human.NewAgent()` 的构造/认证层必须完成这一步；若宿主自带用户体系，也必须提供等价的 authority-qualified key。

### Shared runtime

runtime 对两种 surface 使用带 `kind + scope + id` 的 key。准入先占容量，assignment 绑定 lease owner/fence；worker 事件先进入 durable outbox，再可能重复发送。gateway 在 durable effect/rejection 后才回 ACK/NACK，客户端只在 ACK/NACK 后移出发送 outbox。实际 WebSocket 的 ACK 是按序列号累计确认：服务端帧入 FIFO 时绑定当时的 watermark，迟到事件的 reject 与 client outbox 前缀删除原子发生，不能误删其后的健康事件。三方都可独立掉线，wire 可丢/重/乱序；有界故障且最终恢复时，未决项最终 settle。永久离线不承诺活性。

### Shared workspace

LLM 的修改是逐轮增量 intent；Agent 的最终修改是 bundle Artifact。二者都从 confirmed baseline 构造不可变 payload，并在 caller 侧做 CAS。迟到的旧 success receipt 只能确认已发送版本，不能清掉 Human 后来保存的新 draft。两个 surface 的 writes 共享一条 workspace version chain，但各自 baseline 与 public artifact identity 不混用。

## 3. Refinement obligations

下面的表是后续改 Go 时的门。`implemented` 表示当前 completion 产品已有对应机制；`planned` 表示 TLA 已裁决但公共实现尚未开始。

| Obligation | TLA oracle | 当前 Go 落点 | 状态 |
|---|---|---|---|
| durable admission/decision 先于可见 response | `DurableBeforeVisible`, `HTTPDecisionPrecedesVisibility` | `internal/completion/gateway` 的请求准入、store `BeginResponse`、response event log 与 dialect encoder | implemented |
| request/event 幂等不得把异 payload 当 replay，准入后的 result map 也不可变 | `IdempotencyObservationsAreExact`, `RequestIdentityImmutable`, `ReceiptConsistency` | completion request digest/result snapshot、worker event receipt；`hub.PublishFrom` 的 terminal digest replay | implemented |
| ACK/NACK 之后才能删除 outbox | `AckAfterDurable`, `OutboxAccounting` | `internal/workerclient` durable outbox 的 `Put`、ACK delete、`RejectAndAcknowledge`；`internal/workerws` 的 `event_rejected` | implemented |
| 累计 ACK 不得吸收未来 watermark 或误删 follower | `OutboundACKsBoundAtEnqueue`, `NoPrematureCumulativeDelete`, `RejectionDoesNotDeleteFollower` | worker WebSocket server outbound FIFO 与 workerclient sequence outbox/rejected inbox | implemented；精确顺序由 `HumanWorkerSequence.tla` 检查 |
| worker/session 归属和旧 lease 不得提交 | `LeaseFencingOK`, `EffectAuthorizedAtCommit` | `hub.Reserve/Enqueue/Restore/RegisterInstance/PublishFrom` 与 worker subject ownership | implemented（Go 使用 owner/session receipt；TLA 用抽象 fence） |
| 三方掉线只丢 volatile wire，恢复后 exactly-once settle；durable 历史不得被后续步骤擦除 | `FaultPreservesDurable`, `DurableHistoriesAppendOnly`, `OutboxEventuallySettles`, `LateEventsDoNotPoison` | gateway recovery、hub retired receipt、workerclient reconnect/outbox replay、三方故障测试 | implemented；三方同时离线与五次连续故障分别有 reachability gate |
| task-local result baseline 与全局 workspace baseline 都只由当前精确 success 推进 | `BaselineAdvancesOnlyOnSuccess`（task-local），`WorkspaceCASOK`, `BaselineConfirmed`, `BaselineChangesOnlyOnExactReceipt`（全局写链） | `internal/mirror` 的 `deliveryIntent`/`baselineState`/result reconcile；Remote shim 的 caller-side CAS | implemented for HumanLLM |
| 已冻结的 workspace intent/version/base 不得改写或提前丢弃 | `FrozenPayloadImmutable`, `FrozenIntentsChangeOnlyOnReset` | mirror `deliveryIntent`、caller-side CAS payload 与 conflict/cancel reconciliation | implemented for HumanLLM；Agent refinement planned |
| 当前 response terminal 与 logical task terminal 分离，连续 progress 不被 final 覆盖 | `ToolCallsCloseOnlyTheirResponse`, `ClarificationsCloseOnlyTheirResponse`, `TerminalTaskCannotBeReopened`, `MultipleProgressSegmentsEventuallyClose`, `ResponseTracesAppendOnly` | completion gateway task/request 状态、持久 response frames 与 TUI continuation | implemented for HumanLLM |
| Task/Context/Workspace/多轮 input-required/并行 Task/新 Task follow-up | `TaskIdentityWellFormed`, `TaskIdentityImmutable`, `TerminalTasksImmutable`, `TwoInputRoundsEventuallyComplete`, `ParallelContextEventuallyWorking`, message properties | 尚无独立 public Agent surface；TUI 的 Tasks 列表不是它 | planned `human.NewAgent()` |
| final Submission/Artifact 与 Task completion 原子 | `PublicationAtomic`, `ArtifactAtomic` | 尚无独立 Agent store/transport | planned `human.NewAgent()` |
| 不同 Context 共享 Workspace 时只允许一个同-base apply 成功 | `NoForkedWorkspaceSuccess`, `SharedWorkspaceEventuallyResolves` | 独立 Agent store 尚未实现；底层 caller-side CAS 已存在 | planned `human.NewAgent()` |
| 两个 surface API/key 分开，但共享 Workspace 写链 | `SurfaceIsolation`, `KeysStable`, `HumanRuntimeBothSurfaces.cfg`, `HumanSystemRace.cfg` | completion public packages 存在；统一 `human.NewLLM/NewAgent` facade 与 Agent implementation 尚无 | partial/planned |

TLA 的 abstract fence 不要求 Go 暴露同名字段；Go 可以用稳定 owner + session generation/receipt 实现同一约束。但任何替代机制都必须有故障测试证明旧连接、旧 outbox event 或错误 worker 不能提交 effect。

## 4. 实现顺序

1. 先把现有 completion public facade 明确命名为 HumanLLM，同时保持当前 CLI/package 可用；不要借重命名改变 wire 或持久 schema。
2. 抽出真正共享且无 surface lifecycle 的 runtime ports：identity/auth、admission/lease、worker transport/outbox、workspace CAS 与 receipts。
3. 新建 HumanAgent 自己的 Task/Context/Message/Submission store 与 handler；不要让它调用 completion request 状态机来伪装长任务。
4. 为每条 refinement obligation 增加 Go fault/contract test。尤其要把 TLA mutant 对应为实现层故障注入：ACK-before-commit、late event、stale owner、same-id/different-digest、late receipt/new draft、non-atomic final artifact。
5. 完成 A2A/自定义 transport codec 后，再把外部协议 transcript golden 加入；transport 兼容不得反向改变领域状态机。

## 5. 验收口径

`make formal-check` 是协议设计门；`make check` 和真实 fault/e2e gate 是实现门。只有两者都过，某项才可以从 “TLA 已裁决” 提升为 “Go 已实现并验证”。有限状态结果、mutant 列表、环境假设和未建模边界记录在 [`formal/README.md`](../formal/README.md)，不能用“完整正确”省略这些限定词。
