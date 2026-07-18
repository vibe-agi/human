# Human 的 TLA+ 协议模型

这里先把 `human.NewLLM()` 与 `human.NewAgent()` 的语义边界固定下来，再让 Go 实现沿 refinement obligations 收敛。模型没有把两种产品揉成一个状态机：

| 模块 | 负责什么 |
|---|---|
| `HumanCommon.tla` | 两个 surface 的公共名字、带 surface/scope 的稳定 key、终态集合 |
| `HumanRuntime.tla` | 共享 gateway runtime：准入容量、queue/lease/fence、持久 outbox、append-only durable history、ACK/NACK、重复/乱序/丢包、caller/gateway/worker 三方掉线与恢复、workspace CAS |
| `HumanWorkerSequence.tla` | 实际 worker WebSocket 的有序/累计 ACK 小模型：旧队头帧、迟到事件 NACK、健康 follower、并发 producer 的 stale ACK snapshot |
| `HumanLLM.tla` | 实时 completion：一个 logical task 跨多个 HTTP response；每个 response 独立落库、流式并关闭；tool result 与 clarification 只能进入下一 completion |
| `HumanAgent.tla` | 任务型 Agent：正交的 Context/Workspace、可并行 Task、多轮 `input_required`、不可重开的终态，以及最终 Submission/Artifact 的原子发布 |
| `HumanAgentTransport.tla` | Agent worker transport：无时钟 grant/fence、prepare 后 commit-time 复验、领域 command receipt 与 delivery ACK/NACK 分层、旧 grant 精确 replay 与确定性 NACK 收口 |
| `HumanSystem.tla` | 两个 surface 共享一个 workspace 时的组合语义：key 隔离、冻结 intent/base、精确 receipt 推 baseline、更新草稿保留、Agent bundle 原子性与强制同-base 竞态 |

这是一组分层的 assume/guarantee 模型，不把 HTTP codec、网络、任务协议和文件树全部做笛卡尔积。`HumanRuntimeBothSurfaces.cfg` 会让 LLM/Agent 同时穿过一个 runtime；`HumanWorkerSequence.cfg` 精确检查有序累计 ACK，丢包/重连则由 Runtime 的无序 wire 模型负责；`HumanSystemRace.cfg` 强制两种 surface 从同一 workspace base 发布后再竞争 CAS。这样既覆盖跨层契约，又让每个状态空间仍可完整枚举。

`HumanAgent` 的 `ContextIds` 与 `WorkspaceIds` 是 authority-qualified opaque scope（例如 `<<authenticated principal, external id>>`），不是客户可直接指定的裸 tenant-local 字符串。这是模型的显式环境假设，也是未来 public API 的 refinement obligation。

## 一键复现

需要 Java 11 或更高版本。仓库固定使用 TLA+ `v1.7.4` / TLC `2.19`，下载脚本同时固定 release URL 与 SHA-256：

```sh
make formal-check
```

也可以只跑一部分：

```sh
TLA_CHECK_PHASE=positive formal/run-checks.sh
TLA_CHECK_PHASE=mutants formal/run-checks.sh
TLC_WORKERS=4 formal/run-checks.sh
```

runner 不传 `-deadlock`；TLC 的死锁检查保持开启。临时状态目录和 mutant 都通过 `mktemp` 创建并在退出时清理。关键大模型还用两个独立 fingerprint 完整重跑。

## 当前检查矩阵

正向模型的默认有限常量与最后一次完整探索结果如下。数字不是被脚本硬编码的“快照答案”；runner 只设置足以发现配置意外缩小/路径不可达的下限。

| 配置 | 覆盖重点 | distinct states |
|---|---|---:|
| `HumanRuntimeSafety.cfg` | 无故障完整 runtime safety | 161 |
| `HumanRuntimeFaults.cfg` | 任一 crash/link/data/ACK fault | 1,000 |
| `HumanRuntimeLiveness.cfg` | 有界故障后公平恢复 | 1,000 |
| `HumanRuntimeTripleOutage.cfg` | caller/gateway/worker 全部离线及任意恢复顺序 | 3,657 |
| `HumanRuntimeRetryStorm.cfg` | 五次任意 crash/link/data/ACK 故障后的恢复与收口 | 6,403 |
| `HumanRuntimeDigestConflict.cfg` | 同 event id、不同 digest 必须拒绝 | 1,533 |
| `HumanRuntimeFencing.cfg` | 两 worker、两 fence epoch、旧 owner 迟到事件 | 8,309 |
| `HumanRuntimeWorkspaceRace.cfg` | 两项从同一 base 提交，只能一成一冲突 | 17 |
| `HumanRuntimeBothSurfaces.cfg` | LLM 与 Agent 共用 runtime 但不串 key/receipt/lease | 25,921 |
| `HumanWorkerSequence.cfg` | 累计 ACK 绑定、reject 原子删除、follower 与 stale producer | 40 |
| `HumanLLMSafety.cfg` | 两 task/两 request 的 completion safety | 29,753 |
| `HumanLLMLiveness.cfg` | tool result、clarification 与后续 completion 最终收口 | 4,489 |
| `HumanLLMNoCaller.cfg` | caller 缺席时只守 safety，不伪造进展 | 97 |
| `HumanLLMTransitionOracles.cfg` | 四种 response 终态和 result 分桶的精确 transition | 1,481 |
| `HumanLLMProgress.cfg` | 同一 response 连续两个 progress 后再 final | 7 |
| `HumanAgentSafety.cfg` | 两 Task 的消息、终态、Artifact 与 receipt | 356,409 |
| `HumanAgentLiveness.cfg` | 在明确 Human/caller fairness 下最终收口 | 667 |
| `HumanAgentNoHuman.cfg` | Human 不在线时 safety 仍成立 | 197 |
| `HumanAgentFollowup.cfg` | terminal 后同 Context 创建 fresh Task | 66,293 |
| `HumanAgentConversation.cfg` | 两轮 input-required/reply 后 content-only 完成 | 1,963 |
| `HumanAgentSharedWorkspace.cfg` | 两 Context 共用 Workspace，同-base 只一成一冲突 | 953 |
| `HumanAgentParallelContext.cfg` | 同 Context 两个独立 Task 可同时 working | 13 |
| `HumanAgentIdentityOracles.cfg` | Task 的 Context/Workspace identity 不可变 | 69 |
| `HumanAgentApplyOracles.cfg` | caller-side CAS 的 dirty/base/success 精确前置条件 | 153,249 |
| `HumanAgentBaselineOracles.cfg` | 非 success receipt 不得推进 baseline | 197 |
| `HumanAgentTransportSafety.cfg` | 旧 fence、prepare/commit 竞态、精确 replay、ACK/NACK 与 outbox 收口 | 103,997 |
| `HumanAgentTransportLifecycle.cfg` | accept/input-required/complete 与 revision commit-time 复验 | 51,178 |
| `HumanAgentTransportConflict.cfg` | authority-scoped command id 的异 input/digest 冲突 | 23,350 |
| `HumanAgentTransportLiveness.cfg` | 一次 crash/link/drop 故障后 durable outbox 最终 settle | 639 |
| `HumanSystem.cfg` | 两 surface 的 workspace 组合交错 | 308 |
| `HumanSystemRace.cfg` | 两 surface 同-base 发布后只能一成功一冲突 | 20 |

`HumanLLM`、`HumanAgent` 大安全模型和 `HumanSystem` 会再用 fingerprint 1 重跑。runner 共执行 90 个门：34 个正向检查（含三个 alternate fingerprint）、7 个指定环境/coverage 反例和 49 个 mutant。新增的两个 Agent transport witness 分别证明：旧 fence 上已提交命令可在 fence 后精确 replay 并收口；`input_required` 回合可达且不会隐式撤销 grant。

## Mutant oracle

`make-mutant.py` 会在临时目录生成 **49 个**故意错误的模型；runner 要求它们全部被指定 oracle 抓住：

- 旧队头帧偷未来 ACK、NACK 删除不原子、并发 producer 的 stale snapshot 让累计 ACK 倒退。
- 在 durable receipt 前发 ACK；gateway crash 丢 durable outbox；已持久的 terminal/history 被擦除；旧 fence owner 提交 effect；迟到事件成为队头毒丸；workspace CAS 丢更新。
- digest conflict 返回错误观察；HTTP 200 决策前暴露 stream；已准入 request 的 result map 被篡改；LLM task 终态重开；失败 result 推进 baseline；四种 response terminal 与 task terminal 对错；result 被吞或分错桶；final 覆盖已有 progress trace。
- Agent 终态重开；Submission 与完成不原子；dirty/错 base 仍 apply 成功；取消遗留 frozen Artifact；消息角色错误；Task identity 被改；conflict 错推 baseline；caller reply 没有恢复 working。
- Agent transport 只校验 owner 不校验 fence、只信 prepare 快照、revision 不在 commit 复验、replay 错要求当前 grant 或重复 effect、effect/command receipt 拆提交、ACK 早发、终态遗留 grant、未提交 grant 先可见、异 digest 冒充 replay、NACK 后不 dequeue。
- LLM/Agent surface key 相撞；发布即推进 baseline；冻结 payload/base 被改或提前丢弃；确认新版本时回退到旧 success；迟到旧 receipt 清掉更新草稿；Artifact 与 Task completion 不原子；跨 surface 同-base CAS 允许双成功。

mutant 不是展示用样例，而是属性的覆盖测试：若未来改动让错误路径不可达，或把 oracle 写成恒真，formal CI 会失败。

## 已证明的协议结论

在配置给定的有限集合、原子 durable transition 与公平性假设下，TLC 穷举确认：

- 外部可见 assignment、response、workspace version 都已有相应 durable 事实；ACK/NACK 只在 receipt/rejection 之后出现。
- crash 只清 volatile wire，不回滚 durable admission、outbox、receipt、effect、baseline 或 workspace decision；admission、fence、receipt/NACK、settlement、terminal outcome 与 workspace decision 历史只能追加。
- duplicate/reorder/drop 不会让同一 event id 产生两个 effect；相同 digest 可精确 replay，不同 digest 会明确拒绝并从 outbox 收口。五次有界故障与三方同时离线均在状态图中真实可达，且从每个可达状态最终恢复。
- 累计 ACK 在 server frame 入 FIFO 时冻结；旧帧不能吸收未来 ACK，NACK 与 client prefix 删除原子，后续健康 event 不会被迟到事件误删；并发 producer 的旧 snapshot 会提升到 queue watermark。
- lease 被 fence 或 session 已 retire 后，迟到事件不能提交 effect，也不能永久堵住后续 outbox。
- workspace 的同 base 竞态只能一个成功，其余进入显式 conflict；冻结后的 intent/version/base 不得改写或提前丢弃，消费 apply success 时只能用当前 intent 的精确 receipt 推进 baseline，不能选任意旧 success。该结论同时覆盖同 surface、跨 Context 与跨 surface 的强制竞态。
- HumanLLM 的 `tool_calls` / clarification 关闭的只是当前 response；logical task 可以由下一 completion 继续。response terminal 与 task terminal 不混用；同一 response 的多个 progress 按序保留，final 只能追加不能覆盖。其 `baselineVersion` 只代表 task-local confirmed result；全局 workspace 串行性来自 Runtime/System。
- HumanAgent 的 final Submission/Artifact 与 Task `completed` 是同一个可见原子边界；取消/失败只会 discard 未发布 Artifact；两轮 input-required/reply 后仍可 content-only 完成；终态后消息创建同 Context 的新 Task；同 Context 也允许并行的独立 Task。
- HumanAgent worker grant 没有墙钟到期语义：Fence 明确撤销并让下一次 Acquire 单调增加 generation；prepare 不是授权点，worker/fence 与 expected revision 必须在 effect + command receipt 的同一 commit action 复验。精确已提交 command 的 replay 先于当前 grant/revision 检查；未提交的旧 fence event 则 durable NACK 并从 outbox 收口。
- 两个 surface 可共用 runtime、authority-qualified identity system 和 workspace，但 key namespace、公开状态机与最终交付语义保持分离。

## 没有证明什么

“TLC 全绿”不是对任意规模和 Go 字节码的数学总证明。当前边界必须保持诚实：

- 集合、版本、cursor、fault 次数都是有限常量；结果可以外推协议结构，不能外推无限容量或无限敌对网络的活性。
- liveness 只在故障预算有限、网络最终恢复、机器步骤公平时成立。Human 或 caller 永久离线只要求 safety；负向配置明确证明不能保证 progress。
- payload 被抽象为不可变 digest，stream 被抽象为有序 frame kind trace。逐字节 codec、JSON/SSE 方言、SHA-256 实现与 SQLite 事务仍由 Go golden/fault tests 验证。
- 文件树被抽象为 version + CAS；realpath/symlink、权限、进程执行、磁盘损坏、SQLite 驱动行为不由这些模型证明。
- timeout 被抽象成 retire/terminal transition，没有建模真实墙钟、调度延迟或代理 idle timeout。
- `HumanWorkerSequence` 不重复建模 drop/reconnect（由 Runtime 负责），也未覆盖 duplicate rejection 与 UI confirm 后的 payload-free tombstone；两个模型的保证要组合使用。
- Agent 的消息严格交替只描述 caller/agent 对话消息，不包含 progress/status event；中间流式 Artifact 不在 final Submission 模型内。`ResolveLocalEdit` 是受信任的冲突解决 oracle，`indeterminate` receipt 当前不承诺自动二次对账。
- Go Agent 的 confirmed-head CAS 由**已认证的 caller receipt**驱动：领域库不观察真实文件树 `dirty`、不计算首个 base，也不执行 bundle。公共 `workspace` SQLite journal 已 refine “先持久 pending、再调用 CAS、终态精确重放、崩溃遗留转 indeterminate”，A2A Artifact/apply-receipt 垂直切片也已接通；但真实文件树 fingerprint、bundle 原子 CAS 与权限边界仍由宿主 applier 承担，当前测试不能外推为通用多文件 apply 已完成。
- Go 已实现独立的 `human.NewAgent` 领域/SQLite、消息循环、Artifact 原子发布、commit-time 无时钟 lease/fence、原子 claim、官方 A2A 1.0 caller handler 与 caller apply journal。尚未实现的是远程 Agent worker transport、durable worker outbox/ACK/NACK 和官方 HumanAgent TUI；因此仍不能宣称完整 HumanAgent 网络产品已经交付。

模型与 Go 的具体 refinement 对照见 [`docs/09-formal-model.md`](../docs/09-formal-model.md)。
