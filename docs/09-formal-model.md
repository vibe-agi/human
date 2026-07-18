# 09 TLA+ 模型与实现约束

## 1. 裁决

Human 下保留两个独立 public surface：

- `HumanLLM`：实时、增量、response-centric。`human.NewLLM()` 已实现为 `gateway.Open` 的零行为差异 facade；当前 completion gateway、三方言 codec、TUI 和 Live Workspace 都属于这一面。
- `HumanAgent`：任务型、durable Task/Context、最终打包 Submission/Artifact。`human.NewAgent()`、独立 Agent SQLite 领域与 Artifact/receipt 已实现；A2A/worker transport 尚未实现，且不能复用 completion 的“一个 HTTP response 就是任务生命周期”假设。

目标架构中，两者应共享 identity、auth、queue/lease、worker transport、durable outbox、workspace engine 和 observability；不共享 public lifecycle、terminal 语义或 wire DTO。当前 Go 只共享 `workspace` opaque 值类型，其余 runtime 机制仍在 HumanLLM/completion 内，必须等 Agent transport 成为第二个真实 consumer 后再抽取，不能把目标架构写成已接线事实。

## 2. 关键语义

### HumanLLM

一次 completion 必须先 durable admission 与 HTTP decision，之后才能暴露 stream frame。`text_final`、`tool_calls`、`clarification`、`error` 都关闭当前 response；其中只有 final/error 结束 logical task。`tool_calls` 等待 caller 在下一 completion 回传 result；clarification 等待 caller 在下一 completion 回话。失败 result 被保留但不推进该 logical task 的 confirmed-result baseline；跨 Task、跨 surface 的全局 workspace CAS 由 Runtime/System 模型负责。同一 request key + digest 必须重放相同抽象 trace，同 key + 不同 digest 必须 409/明确冲突。

### HumanAgent

Task 同时绑定两个正交身份：Context 只负责会话/展示分组，Workspace 才是 CAS、draft 和 baseline 的正确性边界；同一 Context 可以并行多个独立 Task。Task 状态为 submitted/working/input-required/terminal；`input_required` 可以多轮往返。Task 一旦 completed/canceled/rejected/failed 就不可重开；caller 后续消息由受信宿主在同 Context 显式 `CreateTask`，领域不会偷偷重开旧 Task。Human 可以只提交 content；若提交 workspace Artifact，则冻结的 base/version/payload 不可变，final Submission 可见与 Task completed 必须原子发生。caller 对 Artifact 的 apply success/conflict/rejected/indeterminate 是独立 receipt；只有匹配的 success 推进 baseline。不同 Context 可以共享同一 Workspace，此时它们必须竞争同一条 CAS 写链，而不能各自成功。

Agent SQLite 无法直接观察客户文件树的 `dirty` 状态，也不执行 bundle。首个 Workspace head 的 `ExpectedBaseRevision` 来自受信 caller adapter 的 bootstrap；success receipt 必须表示 caller 已用 exact base 完成真实 CAS 并观察到 exact result。Go 领域只对这份受信 receipt 做 confirmed-head CAS。这是 `HumanAgent.tla` 的环境/refinement obligation，不是 Agent store 已自行证明文件落盘。

模型中的 `ContextIds` 与 `WorkspaceIds` 是经过认证主体限定后的 opaque scope（例如 `principal + external_id`），不是直接信任客户传入的裸字符串。`NewAgent` 是不负责认证的领域构造器；调用它的受信宿主或 transport adapter 必须从已认证 principal 构造 `AuthorityID`，不能直接复制请求 body 的 tenant 值。

### Shared runtime

runtime 对两种 surface 使用带 `kind + scope + id` 的 key。准入先占容量，assignment 绑定 lease owner/fence；worker 事件先进入 durable outbox，再可能重复发送。gateway 在 durable effect/rejection 后才回 ACK/NACK，客户端只在 ACK/NACK 后移出发送 outbox。实际 WebSocket 的 ACK 是按序列号累计确认：服务端帧入 FIFO 时绑定当时的 watermark，迟到事件的 reject 与 client outbox 前缀删除原子发生，不能误删其后的健康事件。三方都可独立掉线，wire 可丢/重/乱序；有界故障且最终恢复时，未决项最终 settle。永久离线不承诺活性。

### Shared workspace

LLM 的修改是逐轮增量 intent；Agent 的最终修改是 bundle Artifact。二者都从 confirmed baseline 构造不可变 payload，并在 caller 侧做 CAS。迟到的旧 success receipt 只能确认已发送版本，不能清掉 Human 后来保存的新 draft。两个 surface 的 writes 共享一条 workspace version chain，但各自 baseline 与 public artifact identity 不混用。

## 3. Refinement obligations

下面的表是后续改 Go 时的门。`implemented` 只表示表中明确写出的 Go surface/层已经有对应机制与测试；`planned` 表示 TLA 已裁决但该层公共实现尚未开始。

| Obligation | TLA oracle | 当前 Go 落点 | 状态 |
|---|---|---|---|
| durable admission/decision 先于可见 response | `DurableBeforeVisible`, `HTTPDecisionPrecedesVisibility` | `internal/completion/gateway` 的请求准入、store `BeginResponse`、response event log 与 dialect encoder | implemented |
| request/event 幂等不得把异 payload 当 replay，准入后的 result map 也不可变 | `IdempotencyObservationsAreExact`, `RequestIdentityImmutable`, `ReceiptConsistency` | completion request digest/result snapshot、worker event receipt；`hub.PublishFrom` 的 terminal digest replay | implemented |
| ACK/NACK 之后才能删除 outbox | `AckAfterDurable`, `OutboxAccounting` | `internal/workerclient` durable outbox 的 `Put`、ACK delete、`RejectAndAcknowledge`；`internal/workerws` 的 `event_rejected` | implemented |
| 累计 ACK 不得吸收未来 watermark 或误删 follower | `OutboundACKsBoundAtEnqueue`, `NoPrematureCumulativeDelete`, `RejectionDoesNotDeleteFollower` | worker WebSocket server outbound FIFO 与 workerclient sequence outbox/rejected inbox | implemented；精确顺序由 `HumanWorkerSequence.tla` 检查 |
| worker/session 归属和旧 lease 不得提交 | `LeaseFencingOK`, `EffectAuthorizedAtCommit` | HumanLLM 的 `hub.Reserve/Enqueue/Restore/RegisterInstance/PublishFrom` 与 worker subject ownership；Agent 尚无 transport grant | implemented for HumanLLM；planned for Agent |
| 三方掉线只丢 volatile wire，恢复后 exactly-once settle；durable 历史不得被后续步骤擦除 | `FaultPreservesDurable`, `DurableHistoriesAppendOnly`, `OutboxEventuallySettles`, `LateEventsDoNotPoison` | gateway recovery、hub retired receipt、workerclient reconnect/outbox replay、三方故障测试 | implemented；三方同时离线与五次连续故障分别有 reachability gate |
| task-local result baseline 与全局 workspace baseline 都只由当前精确 success 推进 | `BaselineAdvancesOnlyOnSuccess`（task-local），`WorkspaceCASOK`, `BaselineConfirmed`, `BaselineChangesOnlyOnExactReceipt`（全局写链） | `internal/mirror` 的 `deliveryIntent`/`baselineState`/result reconcile；Remote shim 的 caller-side CAS | implemented for HumanLLM |
| 已冻结的 workspace intent/version/base 不得改写或提前丢弃 | `FrozenPayloadImmutable`, `FrozenIntentsChangeOnlyOnReset` | HumanLLM mirror `deliveryIntent`；Agent `agent_artifacts` 不可变 payload/digest，cancel/fail 只改 state 为 discarded | implemented for both domain stores；真实 Agent bundle apply planned |
| 当前 response terminal 与 logical task terminal 分离，连续 progress 不被 final 覆盖 | `ToolCallsCloseOnlyTheirResponse`, `ClarificationsCloseOnlyTheirResponse`, `TerminalTaskCannotBeReopened`, `MultipleProgressSegmentsEventuallyClose`, `ResponseTracesAppendOnly` | completion gateway task/request 状态、持久 response frames 与 TUI continuation | implemented for HumanLLM |
| Task/Context/Workspace/多轮 input-required/并行 Task/新 Task follow-up | `TaskIdentityWellFormed`, `TaskIdentityImmutable`, `TerminalTasksImmutable`, `TwoInputRoundsEventuallyComplete`, `ParallelContextEventuallyWorking`, message properties | `agent` 独立领域、SQLite command ledger/revision CAS/page API；根 facade 为 `human.NewAgent` | implemented for trusted in-process domain；transport planned |
| final Submission/Artifact 与 Task completion 原子 | `PublicationAtomic`, `ArtifactAtomic` | `agent` 的 freeze、final Message、Artifact publish、Submission、Task completed 与 command result 同一 SQLite 事务 | implemented for Agent domain；caller apply 仍是外部 ledger/journal |
| 不同 Context 共享 Workspace 时只允许一个同-base apply 成功 | `NoForkedWorkspaceSuccess`, `SharedWorkspaceEventuallyResolves` | 受信 receipt 驱动的 `agent_workspace_heads` exact-base SQL CAS；goroutine 并发回归强制一成一冲突 | implemented for Agent domain；真实文件 dirty/apply 是 adapter obligation |
| 两个 surface API/key 分开，但共享 Workspace 写链 | `SurfaceIsolation`, `KeysStable`, `HumanRuntimeBothSurfaces.cfg`, `HumanSystemRace.cfg` | `human.NewLLM/NewAgent` 与独立 DB/schema 已分开；`workspace` 提供共享值类型；统一 caller-side apply runtime 尚未接线 | partial |

TLA 的 abstract fence 不要求 Go 暴露同名字段；HumanLLM 当前用稳定 owner + session generation/receipt 实现同一约束。Agent transport 不能只在调用领域方法前验权：必须把 grant/fence 与 mutation 的 commit boundary 绑定，并用故障测试证明旧连接、旧 outbox event 或错误 worker 不能提交 effect。

## 4. 实现顺序

1. **已完成**：把现有 completion public facade 明确命名为 HumanLLM，同时保持当前 CLI/package、wire 与持久 schema 不变。
2. **部分完成**：共享的 opaque workspace Revision/Digest/Payload/ApplyDecision 已落 `workspace` 包；identity/auth、admission/lease/fence、worker transport/outbox 仍须在出现第二个真实 consumer 时从 completion 中抽出，不能先发布空壳兼容层。
3. **领域已完成、transport 待做**：HumanAgent 已有自己的 Task/Context/Message/Submission/Artifact/receipt store；下一步是带 commit-time grant 的 handler/A2A adapter，不能调用 completion request 状态机来伪装长任务。
4. **持续进行**：为每条 refinement obligation 增加 Go fault/contract test。Agent 领域已覆盖 replay/digest conflict、revision CAS、原子 final Artifact、discard、receipt immutable、字段 mismatch 与同-base 并发；ACK-before-commit、late event、stale owner、late receipt/new draft 等 transport/system oracle 要等共享 runtime 接线后继续。
5. 完成 A2A/自定义 transport codec 后，再把外部协议 transcript golden 加入；transport 兼容不得反向改变领域状态机。当前不能因 `NewAgent` 构造器存在就宣称 A2A 已交付。

## 5. 验收口径

`make formal-check` 是协议设计门；`make check` 和真实 fault/e2e gate 是实现门。只有两者都过，某项才可以从 “TLA 已裁决” 提升为 “Go 已实现并验证”。有限状态结果、mutant 列表、环境假设和未建模边界记录在 [`formal/README.md`](../formal/README.md)，不能用“完整正确”省略这些限定词。
