# 09 TLA+ 模型与实现约束

## 1. 裁决

Human 下保留两个独立 public surface：

- `HumanLLM`：实时、增量、response-centric。`human.NewLLM()` 构造 transport-neutral
  `llm.Service`；Store、Codec、caller/worker transport、Protector 和策略均为公共 port，
  三方言与 SQLite/HTTP/WebSocket 是可替换的官方 adapter。现有 CLI gateway/TUI/Live
  Workspace 仍是这一面的 turnkey 产品 composition，但不再定义根 facade 的边界。
- `HumanAgent`：任务型、durable Task/Context、最终打包 Submission/Artifact。
  `human.NewAgent()`、独立 `agent.Store` 与官方 `agent/sqlite` adapter、commit-time 无时钟
  lease/fence、官方 A2A caller、`workspace.Store`、apply receipt 与远程 worker transport/Journal 已实现；
  官方 HumanAgent TUI 尚未实现，且不能复用 completion response 充当任务生命周期。

两者可以复用 framework/protection/Workspace 基础合同，但不共享 public lifecycle、terminal
语义、Store schema 或 wire DTO。当前两个 surface 都有各自的 worker transport 与 durable
Journal；它们共享的是 ACK/NACK 原则，不是同一个 assignment 类型。公共 `workspace` 值
类型与 caller apply Store 目前仍主要由 Agent 链路使用，HumanLLM mirror 尚未接入同一
revision chain，不能把目标 Workspace 统一写成已接线事实。

## 2. 关键语义

### HumanLLM

一次 completion 必须先 durable admission 与 HTTP decision，之后才能暴露 stream frame。`text_final`、`tool_calls`、`clarification`、`error` 都关闭当前 response；其中只有 final/error 结束 logical task。`tool_calls` 等待 caller 在下一 completion 回传 result；clarification 等待 caller 在下一 completion 回话。失败 result 被保留但不推进该 logical task 的 confirmed-result baseline；跨 Task、跨 surface 的全局 workspace CAS 由 Runtime/System 模型负责。同一 request key + digest 必须重放相同抽象 trace，同 key + 不同 digest 必须 409/明确冲突。

### HumanAgent

Task 同时绑定两个正交身份：Context 只负责会话/展示分组，Workspace 才是 CAS、draft 和 baseline 的正确性边界；同一 Context 可以并行多个独立 Task。Task 状态为 submitted/working/input-required/terminal；`input_required` 可以多轮往返。Task 一旦 completed/canceled/rejected/failed 就不可重开；caller 后续消息由受信宿主在同 Context 显式 `CreateTask`，领域不会偷偷重开旧 Task。Human 可以只提交 content；若提交 workspace Artifact，则冻结的 base/version/payload 不可变，final Submission 可见与 Task completed 必须原子发生。caller 对 Artifact 的 apply success/conflict/rejected/indeterminate 是独立 receipt；只有匹配的 success 推进 baseline。不同 Context 可以共享同一 Workspace，此时它们必须竞争同一条 CAS 写链，而不能各自成功。

Agent Store/core 无法直接观察客户文件树的 `dirty` 状态，也不执行 bundle。首个 Workspace head 的 `ExpectedBaseRevision` 来自受信 caller adapter 的 bootstrap；success receipt 必须表示 caller 已用 exact base 完成真实 CAS 并观察到 exact result。Go 领域只对这份受信 receipt 做 confirmed-head CAS。这是 `HumanAgent.tla` 的环境/refinement obligation，不是 Agent Store 已自行证明文件落盘。

模型中的 `ContextIds` 与 `WorkspaceIds` 是经过认证主体限定后的 opaque scope（例如 `principal + external_id`），不是直接信任客户传入的裸字符串。`NewAgent` 是不负责认证的领域构造器；调用它的受信宿主或 transport adapter 必须从已认证 principal 构造 `AuthorityID`，不能直接复制请求 body 的 tenant 值。

Agent worker grant 没有墙钟到期语义：只有显式 `FenceLease` 才撤销当前 owner，下一次 acquire/claim 单调增加 fence。`ClaimLease` 必须在同一 durable transaction 中选出最早可领 Task、记录不可变 grant 历史与 exact command receipt，并返回该时刻的 Task snapshot。Prepare 或 adapter 调用前验权不是授权点；每个 worker mutation 必须在 effect + command receipt 的同一 commit 内复验 worker、fence 和 expected revision。已提交命令的 exact replay 可以先于当前 grant 检查返回原结果；未提交的旧 fence 命令必须被拒绝。

### Shared runtime

runtime 对两种 surface 使用带 `kind + scope + id` 的 key。准入先占容量，assignment 绑定 lease owner/fence；worker 事件先进入 durable outbox，再可能重复发送。gateway 在 durable effect/rejection 后才回 ACK/NACK，客户端只在 ACK/NACK 后移出发送 outbox。实际 WebSocket 的 ACK 是按序列号累计确认：服务端帧入 FIFO 时绑定当时的 watermark，迟到事件的 reject 与 client outbox 前缀删除原子发生，不能误删其后的健康事件。三方都可独立掉线，wire 可丢/重/乱序；有界故障且最终恢复时，未决项最终 settle。永久离线不承诺活性。

### Shared workspace

在目标组合语义中，LLM 的修改是逐轮增量 intent，Agent 的最终修改是 bundle Artifact；二者都从 confirmed baseline 构造不可变 payload，并在 caller 侧做 CAS。迟到的旧 success receipt 只能确认已发送版本，不能清掉 Human 后来保存的新 draft。两个 surface 的 writes 应共享一条 workspace version chain，但各自 baseline 与 public artifact identity 不混用；当前 Go 尚未将 HumanLLM mirror 接入这条链。

## 3. Refinement obligations

下面的表是后续改 Go 时的门。`implemented` 只表示表中明确写出的 Go surface/层已经有对应机制与测试；`planned` 表示 TLA 已裁决但该层公共实现尚未开始。

| Obligation | TLA oracle | 当前 Go 落点 | 状态 |
|---|---|---|---|
| durable admission/decision 先于可见 response | `DurableBeforeVisible`, `HTTPDecisionPrecedesVisibility` | `llm.Service` 的 Store admission/decision/event/receipt 原子边界与 deterministic Codec；旧 gateway 仍有对应实现 | implemented |
| request/event 幂等不得把异 payload 当 replay，准入后的 result map 也不可变 | `IdempotencyObservationsAreExact`, `RequestIdentityImmutable`, `ReceiptConsistency` | completion request digest/result snapshot、worker event receipt；`hub.PublishFrom` 的 terminal digest replay | implemented |
| ACK/NACK 之后才能删除 outbox | `AckAfterDurable`, `OutboxAccounting` | `internal/workerclient` durable outbox 的 `Put`、ACK delete、`RejectAndAcknowledge`；`internal/workerws` 的 `event_rejected` | implemented |
| 累计 ACK 不得吸收未来 watermark 或误删 follower | `OutboundACKsBoundAtEnqueue`, `NoPrematureCumulativeDelete`, `RejectionDoesNotDeleteFollower` | worker WebSocket server outbound FIFO 与 workerclient sequence outbox/rejected inbox | implemented；精确顺序由 `HumanWorkerSequence.tla` 检查 |
| worker/session 归属和旧 lease 不得提交 | `LeaseFencingOK`, `EffectAuthorizedAtCommit`；Agent 还有 `HumanAgentTransport` 的 commit-time oracles | LLM/Agent 各自的公共 worker endpoint/transport、durable Journal、lease/receipt；Agent mutation 在事务内复验 grant/revision | implemented for both surfaces |
| 三方掉线只丢 volatile wire，恢复后 exactly-once settle；durable 历史不得被后续步骤擦除 | `FaultPreservesDurable`, `DurableHistoriesAppendOnly`, `OutboxEventuallySettles`, `LateEventsDoNotPoison` | gateway recovery、hub retired receipt、workerclient reconnect/outbox replay、三方故障测试 | implemented；三方同时离线与五次连续故障分别有 reachability gate |
| task-local result baseline 与全局 workspace baseline 都只由当前精确 success 推进 | `BaselineAdvancesOnlyOnSuccess`（task-local），`WorkspaceCASOK`, `BaselineConfirmed`, `BaselineChangesOnlyOnExactReceipt`（全局写链） | `internal/mirror` 的 `deliveryIntent`/`baselineState`/result reconcile；Remote shim 的 caller-side CAS | implemented for HumanLLM |
| 已冻结的 workspace intent/version/base 不得改写或提前丢弃 | `FrozenPayloadImmutable`, `FrozenIntentsChangeOnlyOnReset` | HumanLLM mirror `deliveryIntent`；Agent Artifact Store record；`workspace.Store` 的 exact ApplyIntent 与 pending 恢复为 indeterminate（官方 `workspace/sqlite` 实现） | implemented for both domain stores and Agent caller Store；宿主真实文件 CAS 仍是 obligation |
| 当前 response terminal 与 logical task terminal 分离，连续 progress 不被 final 覆盖 | `ToolCallsCloseOnlyTheirResponse`, `ClarificationsCloseOnlyTheirResponse`, `TerminalTaskCannotBeReopened`, `MultipleProgressSegmentsEventuallyClose`, `ResponseTracesAppendOnly` | completion gateway task/request 状态、持久 response frames 与 TUI continuation | implemented for HumanLLM |
| Task/Context/Workspace/多轮 input-required/并行 Task/新 Task follow-up | `TaskIdentityWellFormed`, `TaskIdentityImmutable`, `TerminalTasksImmutable`, `TwoInputRoundsEventuallyComplete`, `ParallelContextEventuallyWorking`, message properties | `agent` typed Store/domain；官方 A2A auth/send/get/list/cancel/subscribe/input-required；独立 worker transport/Journal | implemented for domain and both transports；official Agent TUI planned |
| final Submission/Artifact 与 Task completion 原子 | `PublicationAtomic`, `ArtifactAtomic` | `agent` 的 freeze、final Message、Artifact publish、Submission、Task completed 与 command result 同一 Store transaction（官方 SQLite adapter 为单一 SQLite transaction）；A2A Artifact 映射 | implemented for Agent domain and caller adapter |
| 不同 Context 共享 Workspace 时只允许一个同-base apply 成功 | `NoForkedWorkspaceSuccess`, `SharedWorkspaceEventuallyResolves` | `workspace.Store` 先持久 pending 再调宿主 CAS；A2A apply-receipt extension 回传终态；Agent Store 的 confirmed-head exact-base CAS | implemented vertical slice；宿主文件树 CAS 正确性仍是 obligation |
| 两个 surface API/key 分开，但共享 Workspace 写链 | `SurfaceIsolation`, `KeysStable`, `HumanRuntimeBothSurfaces.cfg`, `HumanSystemRace.cfg` | `human.NewLLM/NewAgent` 与独立 Store namespace/contract 已分开（官方 SQLite schema 也独立）；`workspace` 有值类型与 Agent caller Store；HumanLLM mirror 尚未使用同一 revision chain | partial |

TLA 的 abstract fence 不要求所有 surface 暴露同名字段；HumanLLM 用稳定 owner + lease/
receipt 实现这一约束，Agent 显式暴露 no-clock `LeaseGrant` 并在 mutation commit 复验。
两个远程 transport 都先持久 outbox/Journal、再发送，并以 ACK/NACK 收口；它们仍须由持续
故障测试证明旧连接、旧 event 或错误 worker 不能提交 effect。

## 4. 实现顺序

1. **已完成**：`human.NewLLM/NewAgent` 是两个 transport-neutral composition root，
   Store、Protector 与生命周期分离；旧 CLI/package/wire 继续作为独立 composition。
2. **已完成**：两个 surface 各自发布 worker endpoint/transport/Journal，并保持 DTO、lease
   与 terminal 语义隔离；身份认证仍属于具体 transport adapter。
3. **已完成**：Agent caller 垂直切片包含官方 A2A、extension negotiation、`workspace.Store`
   （含官方 SQLite adapter）、authenticated receipt 与远程 worker transport；宿主真实文件 CAS 仍是 obligation。
4. **部分完成**：公共 workspace 值类型与 Agent caller Store 已落地，HumanLLM mirror 尚未
   接入同一 revision chain。
5. **持续进行**：为每条 refinement obligation 增加参数化 Store/网络故障测试，并在真实
   宿主中验收文件 CAS、caller/worker/service 不同恢复顺序和用户可见状态。

## 5. 验收口径

`make formal-check` 是协议设计门；`make check` 和真实 fault/e2e gate 是实现门。只有两者都过，某项才可以从 “TLA 已裁决” 提升为 “Go 已实现并验证”。当前 runner 共有 **91 个 gate**：35 个正向检查、7 个指定环境/coverage 反例与 49 个 mutant；其中新增的 LLM preempt 正向门要求 caller 已断开且 cleanup 身份仍匹配 exact active request，live caller 或旧 handler 的迟到 cleanup 都不能授权夺权；另有 **11 个 Agent transport mutant** 专门攻击 owner-only 验权、stale prepare/revision、错误 replay、非原子 effect/receipt、过早 ACK、终态遗留 grant、未提交 grant 可见、异 digest 伪回放与 NACK 不出队。有限状态结果、mutant 列表、环境假设和未建模边界记录在 [`formal/README.md`](../formal/README.md)，不能用“完整正确”省略这些限定词。
