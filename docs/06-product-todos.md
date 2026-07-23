# 06 · 产品 TODO 与证据账本

本页只回答三件事：什么已经成立、这轮还要验收什么、哪些必须等真实 harness。它不把“代码存在”写成“客户已兼容”。

> **产品定义**：Human 让人直接参与客户 Agent 的工作流，不按编码、排障、咨询、运维等业务场景限制边界。实时形态 HumanLLM 是**人占据客户 Agent 的 LLM / model-provider 协议位置**，客户 Agent 继续拥有上下文、权限、工具循环和真实执行环境；任务形态 HumanAgent 则用 durable Task/Message/Submission/Artifact 表达可暂停、可恢复、最终打包交付的协作。两者共享内容与 Workspace 语义，目标是复用同一写链机制，但不是同一个公开状态机；当前公共 `workspace` 值类型和 journal 只有 HumanAgent 链路在使用，尚非两面共享实现。

> **RC 交付边界**：当前候选发行的单机 `human local` 已冻结 OpenCode 1.17.18、Claude Code 2.1.217 与 Codex 0.145.0 三个 exact profile。三端具备原生 Live Workspace 和各自真实 Tasks/Plan 工具；Codex Workspace 额外要求请求实际声明精确的 Responses `custom` freeform `apply_patch` grammar。其它版本和远程多 worker/多租户仍是扩展证据；不能把 exact profile 外推成任意版本兼容。

Live Workspace 是核心闭环（镜像为空时可先逐文件 exact bootstrap）：

```
OpenCode exact profile 可选 :pull path → caller Agent 返回 base64 精确字节
  → Human mirror 保存
  → fsnotify + fresh full review
  → 人工 confirm，或显式 auto-send 干净改动
  → 持久记录 exact delivery intent
  → exact harness profile 生成原生 edit/write/bash/tasks
  → 客户 Agent CLI 执行
  → tool result 随下一 completion 回流
  → continuation + baseline reconcile
```

## 已完成

### 协议与持久正确性

- OpenAI Chat Completions、OpenAI Responses、Anthropic Messages 的 stream/aggregate codec、canonical 转换、持久 HTTP decision 与逐字节重放均有仓库测试，并有默认执行的三协议 Web-human 产品门。协议证据不自动外推给具体 CLI；Codex 0.145.0 的真实公共栈 gate 只把其 Responses RemoteTools 文本/函数路径提升为实测。
- 官方 SDK 契约门已固定 `openai-go/v3 v3.37.0` 与 `anthropic-sdk-go v1.58.1`，真实覆盖官方请求序列化、三方 aggregate/stream 工具调用及 tool result continuation、三方 SSE 文本、标准错误 envelope 及 Anthropic `count_tokens`。三种顶层 envelope 的字段都必须显式映射、校验后 no-op 或拒绝；未知控制不再被静默吞掉。该门不外推为 Batches/Files/Models、provider 会话存储或 server tools/MCP 支持。
- Responses 现代契约已按真实 Codex wire 冻结：显式 serial/parallel policy、普通与 namespace functions、typed reasoning history、provider-hosted `web_search`。namespace/name 作为一个正确性身份，serial 响应最多一个 call；hosted capability 不进入可调用 tools。reasoning 私有状态只留 SHA-256，不进入 transcript/worker state。顶层 envelope 严格拒绝未知字段，未实现且会改变行为的控制项 fail-closed。
- Chat Completions 接受标准 `developer` role 并规范化为 system；`/v1/models` 与 completion 一样只允许 caller principal，worker 身份不能借模型目录端点越权。
- caller request、worker event/receipt、shim tool ledger 三层幂等边界已经分开；统一 `8 MiB` wire budget 在持久准入/outbox 前检查。Remote tools / Workspace 的 `tool_call_id` 在整个 task 内唯一：不同 event 的同/异 digest 复用都在 durable step 前走 `event_rejected`，不会形成 outbox 队头毒丸。
- 工具实际执行后，ledger completion 使用脱离已取消 HTTP request、但有 10 秒上限的 durability context；Unix exec 超时杀整个私有进程组并以 `WaitDelay` 封住孙进程继承管道。确定性非法且尚无 durable effect 的 worker event 走 `event_rejected + ACK`，从 outbox 移除并保留人工草稿，不再成为重连队头毒丸。
- 可解码或 canonical 已完全损坏的单条 completion 都会通过 store 级 raw quarantine 得到持久有限终态，健康记录照常恢复。200 前固定重放 `500 recovery_failed`；200 后保留不可逆的 HTTP 200 并追加方言终态，Responses 的序号接在已提交 partial stream 后。同 key/同 digest 稳定重放，异 digest 仍是 409；只有 dialect 字段自身也损坏时才退化为稳定的通用 SSE error。
- 正确性 payload 完成后默认 24h grace 再裁成 tombstone；audit payload 默认关闭、开启后默认 TTL 7 天，两套数据策略不混用。

### 运行与嵌入

- 根包已提供 transport-neutral `human.NewLLM()` / `human.NewAgent()`。前者构造公共 `llm.Service`，要求显式 Store/DeploymentID；后者打开独立 Agent 领域并显式要求 `agent.Store`。两者都不启动 listener/TUI，也不按路径选择或隐藏创建 SQLite；`llm/sqlite` 与 `agent/sqlite` 只是宿主可选的官方 adapter。Agent 已覆盖 authority-qualified Context/Workspace、并行 Task、多轮 input-required、终态、分页、command replay/revision CAS、Artifact/final 原子发布与 confirmed-head CAS。Worker mutation 在同一 commit 内复验无墙钟 lease 的 worker/fence/revision；`ClaimLease` 原子选择 Task、增加 fence、记录 grant 与 command receipt。
- 公共 `a2a` 包已实现官方 A2A 1.0 HTTP+JSON caller adapter。Agent Card 以标准 well-known 路径暴露，其余操作先认证，且只用 principal 派生 Authority，不信任 body tenant。已覆盖 send/get/list/cancel、GET/POST subscribe、SSE、`input-required` 多轮、exact message retry、Workspace/Apply Receipt negotiation。这是 caller transport；独立的 `agent/workerws` 已实现远程 Human worker assignment/event/ACK/NACK 与 durable Journal。
- `workspace` 公共包已有 transport-neutral `Store` / `ApplyIntent` / `CASApplier`；官方单 owner SQLite 实现位于 `workspace/sqlite`。Store 合同要求调外部 CAS 前先落 `pending`，exact retry 只重放终态，重启发现 pending 或 applier 返错会终结为 `indeterminate`，而不盲目重执行副作用。A2A Artifact → workspace Store → authenticated apply-receipt → Agent confirmed head 的垂直切片已有回归。宿主仍必须实现真实文件树的 CAS applier；HumanLLM 尚未接入同一全局 revision chain，也没有把逐文件 callerfs 冒充多文件原子 apply。
- `human local` 已无条件装配公共栈：loopback `llm/callerhttp`、`llm.Service`、`llm/sqlite`、进程内 `workerkit`、Web 人侧与 mirror/state 同处一个生命周期，不再经过 legacy gateway、worker WebSocket 或 outbox。`human gateway` + `human worker` 仍是独立的远程部署产品线，不能与 local 的进程内拓扑混写。
- local 的 service DB 与 workerkit state 各自维护公开 adapter 的版本化 schema；空库初始化，已支持的旧 workerkit schema 显式迁移。远程 gateway/outbox/caller ledger 仍按自己的独立 schema 和恢复合同运行；它们不是 local 的隐藏第二份状态。
- 新 `llm` package 公开 Store、Codec、Caller/Worker Transport、Protector、Router/Policy、Observer，以及显式由宿主调度的 retention/expiry ports；官方 `llm/sqlite`、`llm/callerhttp`、`llm/workerws`、`workerkit` 与 `protect/aead` 提供基础实现，但宿主可替换策略、存储、认证、身份解析、传输和人侧。策略面已收口三条安全语义：AdmissionPolicy 必填（全放行须显式 `llm.AdmitAll()`）；caller 认证属性经 `Identity.Attributes` / `CallerAttributes` 只作为 AdmissionPolicy/WorkerRouter 的 advisory 输入，不进入请求身份或持久化；多 worker 且未配 Router 的 admission 以 `worker_router_required` fail-closed，不伪装成可重试的容量不足。`humantest` 另提供 LLM/Agent 两套歧义提交注入器与对账套件，官方 SQLite 与示例 custom Store 均通过演练。官方持久 adapter 当前只实现 SQLite，但 `agent.Store` / `llm.Store` 已是公共第三方 driver contract。
- local 只持久化一个 caller token，凭据文件严格为 mode `0600`、拒绝 symlink/未知字段并原子替换；`--reset-credentials` 生成并稳定保存新的 caller token。进程内 worker 由服务主体身份连接，不再需要第二个明文 token、credential pair 或轮换 journal。远程 worker 的 token/outbox 仍服从远程产品线自己的合同。
- `human local backup / verify-backup / restore` 已迁到公共 v3 archive：离线锁定 service DB 与 workerkit state，以 SQLite snapshot、固定布局、mode/size/SHA-256、单 caller credential、caller/worker identity、mirror workspace/baseline 做交叉验证；归档与凭据均为 `0600`。restore 默认拒绝非空目标，`--force` 使用同目录 staging、old/new rename 和可恢复 journal 整套提交；安装前用真实 `llm/sqlite`、`workerkit/sqlite` adapter 验 schema，安装后再 quick-check。旧 local v1/v2 archive 明确不混读；mirror symlink/特殊节点只记录为 skipped、不跟随。

### OpenCode 1.17.18 exact profile

- profile 只映射真实捕获的 `read/write/edit/bash`，使用绝对 `filePath/workdir`、允许并行调用；不虚构 search/delete/rename。它不是阻断其它 caller-declared tools 的全局 allowlist，同时维护 exact native tool 授权分类：mapped/已审 standard 默认可发，command/network/sub-agent 等 privileged 与未分类 custom/MCP 工具必须显式 active-capability opt-in（当前复用 `X-Human-Allow-Exec`）。
- 本地 exact resolver 从认证 caller、Human 配置的逻辑 workspace 与
  `opencode@1.17.18` 原生 `X-Session-Id` 建立 affinity，不接收 Agent 绝对 root。通用
  HeaderResolver 只接收 tier/workspace key/harness/session。clarification → followup →
  tool call → result continuation 因而同 task；terminal 后下一顶层 user 才采用新 candidate。
  文件 tool call 使用项目相对路径，由 Agent cwd 解析。
- 无显式 key 时，caller/workspace/**harness session**、canonical digest 与完整 JSON 语义 digest 派生每个 request 的 retry key，材料不含可变 `task_id`：同请求重试同 key，历史/选项变化则新 key。精确 OpenCode 的无工具标题/摘要请求清空 task/workspace、隔离为 Chat，但保留 exact request retry key；声明任意工具则保留 Workspace，只有映射工具获得专用行为，privileged/unclassified 通用工具仍须 active-capability opt-in。
- 精确逐文件 bootstrap 已完成：Command 的 `:pull path` 经已声明 `bash` 与 caller Agent 权限闸运行 `opencode debug file read --pure`，持久绑定 hydration intent，严格解码 base64 exact bytes；空文件可 hydrate，前导 `-` 路径以 `./` 消歧。整个回流请求仍受 `8 MiB` wire budget 约束，大文件 fail-closed，不承诺 `16 MiB`。普通 OpenCode `read` 仍是有损展示，绝不拿来 hydrate。
- 真实 OpenCode 1.17.18 CLI gate 已完成：Human 会话目录修改后生成项目相对原生
  `write/edit`，CLI 在自己的 cwd 实际执行并回传成功 result，Human reconcile 后继续
  同一工具循环里的 `bash + todowrite`、final。
- Basic/Chat 的文本、多 tool call、`write → edit → bash → todowrite → final` 也已有真实演练。10m/2h 心跳结果仅作历史证据，**不再增加长挂测试**。

### Live Workspace 与 TUI

- mirror 按 caller/workspace 隔离；递归 fsnotify 忽略 `.git`、debounce 后以 full Review 为真相源。Remote shim 的真实文件系统围栏在 EvalSymlinks 后再次拒绝任一 `.git` 路径段，因此 `alias -> .git` 不能绕过 write/edit/delete/rename 禁令；这不扩写为对任意本地进程 symlink swap 的跨进程原子保证。
- Review 遇 symlink、非普通文件、单文件读错或超限文件时逐路径隔离并显示 path/reason；被跳过的已跟踪路径及目录后代不会误判为 delete，其它改动继续 review。Search 同样跳过单个超长行/坏文件并返回诊断，不再拖垮整个 workspace。
- 默认保存后只刷新 review、等待 preview/confirm；`workspace.auto_send=true` 时，也只有完整 Review 的 change-level 均为 `allow`、没有被跳过或因 adapter 缺能力而留待处理的改动才自动发送。安全 warning/block、冲突、逐文件 skip 或部分不可交付批次暂停；OpenCode “无 remote CAS”adapter warning 会展示，但在显式 opt-in 下不单独阻断发送。
- exact profile 未映射 delete 时，该删除保留 pending 并明确 warning；同批可映射 edit/write 继续交付，preview、fresh-confirm 与 delivery intent 全程只使用 positionally aligned 的 deliverable 子集。
- caller shim 的 `human_read_file` 默认上限 1 MiB，所有工具的最终编码结果另有统一 2 MiB 上限；超限固化为小型 `result_too_large` tool result，同 call ID 可安全重放，新 call ID 可在输入缩小后重试。read、search 或其它工具都不会把无法回传的巨大 payload 永久写进 ledger。
- caller shim ledger 的崩溃歧义已显式终结：文件型 ledger 先取得持有至 `Close` 的跨进程 owner lock，第二个 shim 在 `recoverPending` 前 fail-fast；当前进程内的重复调用只会看到真实执行期的短暂 `pending`。重启发现遗留 `pending`，或工具已执行但 result 提交失败时，会持久化并重放 `execution_outcome_indeterminate`；若 completed/indeterminate 两次持久化都暂时失败，同 ID 后续只重试收口，不重跑副作用，待用户核对工作区后用新 call ID 继续。
- 每次文件 event 都按 `exact pending row → mirror delivery intent → intent-recorded phase → durable outbox` 顺序提交；只有 phase=true 的精确 pending payload 可发送。恢复时无法区分“仅 pending row 已落盘”与“同一 event 已进 outbox、尚未回到 TUI”，因此所有 recovered pending assignment 都保持原字节，改变任一字段的 replay 不得改写 journal/outbox。准备/确认失败先写 terminal discard tombstone 并移除 intent，再幂等删除 pending row；commit-ambiguous Put 也必须完成 Delete 才确认 rejected inbox。同一 rejected task/session scope 的 cleanup 未完成时禁止新 event，无关 scope 继续服务；高级工具草稿用旧 event ID 与行号确定性派生全新 call ID，不复用 tombstone。镜像新目录、原子发布和自身 rename 删除都同步相应父目录；进程/OS 崩溃恢复不会把已丢弃 call 复活。成功 result 只推进到该 delivery intent；等待期间保存的更新草稿继续作为 Review diff，不会被迟到 result 静默确认。
- Reply/Command/Tasks/Advanced tool-call 草稿和最多 32 个 parked continuation 默认写入 worker-local SQLite；仓库测试已证明多个相互独立的 parked continuation 可整体跨重启恢复、按各自 scope 续接，坏 state 记录逐条隔离且不阻断健康记录。
- durable worker outbox 按行解码；单条 assignment/payload 损坏时会在同一 SQLite 事务中保留原始行到 quarantine 并移出发送队列，健康事件继续发送。TUI 持续显示有限 event ID、数量和数据库路径；人工裁决流程固定在 [08 部署与运维](08-operations.md#outbox-损坏)，不会猜测副作用或静默复用该 event ID。
- Tasks/Plan 已由真实容器 CLI + Playwright 正式面板验证完整三态：Claude 2.1.217 `TaskCreate → TaskUpdate(in_progress) → TaskUpdate(completed) → TaskList`，OpenCode 1.17.18 `todowrite` 与 Codex 0.145.0 `update_plan` 均走 pending→in_progress→completed，每次都等 tool result continuation 后再继续。Web 的可替换 `PlanProfileResolver` 只对 exact 版本与 behavioral schema 开面板；schema 漂移 fail closed。

### 网络与服务异常的自动化矩阵

- caller：同 key 在 200、首帧、progress 后连续断开 5 次，第 6 次续接；单 request/assignment/final，wire 相同。
- 真实 OpenCode 1.17.18：受控反向代理在 gateway 已接单但下游尚未收到 response headers、完整 stream-start 首帧后、完整 Human progress 帧后三个场景并行运行；每个场景连续强制断 TCP 5 次，第 6 次都以完全相同的请求 body 与 `X-Session-Id` 重试，并命中同一个 durable Human idempotency key、同一个 assignment，重放后继续原回合并正常退出。三个并行场景单轮约 70 秒；Makefile 默认 `REAL_NETWORK_DROPS=5`，release 以 `REAL_COUNT=3` 重复整套门。CLI 最终输出的 progress/final 各出现一次；race `count=1` 也已通过。race 暴露过的“前一终态其实已提交但忙碌文案未清”是 TUI 状态刷新缺口，已有确定性回归，不是 outbox/ACK 死锁。该真实门不重启 gateway/worker/caller 进程，不能用来宣称真实进程恢复顺序已通过。
- caller 单向半开：首次流和同 key replay 都用阻塞 writer 验证逐次 `Write+Flush` deadline；默认 10s，写入超时后释放 handler，不给整个等待 Human 的 stream 设置绝对超时。
- worker：gateway 初始离线、连续 5 次 WebSocket flap、半开 ping timeout、ACK/outbox 重放、401/403 终态；同进程 instance ID 跨重连稳定，另一进程共用 token 不会顶替 incumbent，而是有界退避并在旧半开连接释放后自动接管。
- gateway/SQLite：response/event 多个崩溃窗口、单条坏恢复记录隔离。
- 重启时 durable backlog 可暂时超过新调小的 queue capacity 并继续排空；新 admission 在 active 降回阈值前保持拒绝，不会因第 N+1 条恢复记录让整个 gateway 下线。
- `max_pending` 超时会持久化为稳定 `expired`；迟到 worker event 进入 durable rejected inbox/TUI 草稿，不复活旧请求、不触发 WebSocket 重连，也不 poison 后续 live work。
- 在线 worker 遭 gateway/SQLite 重启：caller 已见 partial SSE，worker 离线写 final；服务恢复后 worker 自动重连/outbox 重放，caller 同 key 精确恢复。
- caller + worker + gateway 三方重叠离线并按不利顺序恢复：项目内部 request/event/receipt 保持单次，无队头 poison。
- Workspace 三方故障正式测试：reviewed v1 原生 edit/delivery intent 已持久化后让 caller、worker、gateway 重叠离线，edit 只留在 worker 磁盘 outbox；重启 gateway/SQLite 与 worker 后，3 个并发 same-key replay 逐字节相同且各含同一 call ID 一次，数据库只有一份 receipt/step/applied；result continuation 与 ledger replay 只推进一次 v1 baseline，并精确保留离线期间保存的 v2 diff。它不证明真实 OpenCode CLI 只执行一次。

上述 exactly-once 只约束 Human 自己的 request/event/outbox/ledger。OpenCode 原生 edit/write 没有 shim ledger 与 remote SHA，不能把它扩写成客户文件工具 exactly-once。

## 本轮待验收

不再跑更长 soak；三处真实客户端网络断点的连续 5 次失败/第 6 次恢复门已经完成，剩余门禁只验尚未覆盖的真实进程恢复顺序、外部副作用和用户可见状态。默认 save → fsnotify review → preview/confirm 的真实 TUI 保存链已通过；`workspace.auto_send` 仍只有内部安全不变量测试，不作默认产品路径。多 parked continuation 重启恢复、outbox 单坏行 quarantine 与内部 fault matrix 已落为仓库门和运维流程，不再列作待办。

1. **真实 OpenCode 进程恢复与外部执行观察**
   - response headers 前、stream-start 后和 progress 后都已由真实 CLI 完成每点连续 5 次掉线、第 6 次同 body/session/idempotency 恢复；这项不再列为待办。
   - 项目内部 Workspace 三方重叠故障、outbox/SQLite 重启和 save-ahead 已正式通过；这里不再重复证明内部不变量，只观察真实 OpenCode CLI 在“worker 先恢复”和“caller 先恢复”时的 retry/身份/用户可见行为。
   - 验证真实 CLI 是否出现重复原生文件执行、是否能继续工具循环及后续 session；不能拿已通过的服务端 request/event 去重替代外部观察。

2. **真实运维可见性**
   - 仓库自动化已证明 `max_pending` 超时、迟到 event rejected/no-poison 与 worker 401/403/吊销终态；此处只观察真实 CLI/TUI 是否给出准确、可操作的用户可见状态。
   - outbox quarantine 的持久告警与人工裁决步骤已经固化；外部验收只需确认真实终端不会把该状态误报为普通断线，也不会掩盖仍可继续发送的健康事件。

3. **HumanAgent 远程 worker 与产品面**
   - commit-time 无时钟 lease/fence、原子 `ClaimLease`、官方 A2A 1.0 caller adapter 和 caller `workspace.Store` 垂直切片已完成，不再列为待办。
   - 独立的 `agent/workerws` transport 与 durable Journal/outbox 已完成：claim/grant 和领域 event 使用 Agent 自己的 DTO，ACK/NACK 只在领域 commit/rejection 后收口，不暴露 HumanLLM completion WebSocket DTO。
   - 尚需官方 HumanAgent TUI/worker 产品装配，以及一个真实 caller 宿主的文件树 CAS applier 验收；当前可宣称可嵌入 A2A caller 面、远程 worker transport 和 apply Store，但不能宣称已有现成的 Agent TUI/daemon 或通用多文件原子落盘。
   - HumanLLM 的增量 mirror intent 与 HumanAgent Artifact 仍未接到同一条 workspace revision chain；当前只有 Agent 使用公共值类型和 caller Store，不能把目标架构写成已共享。

项目内部的可重复 network/service fault matrix 用 `make fault-test`；它同时运行旧 gateway/workerclient 网络矩阵、新 LLM/Agent 的 SQLite/Memory Store 语义矩阵、两套 worker Journal 的 release/reopen 与 Memory abandon 套件，以及 Agent/LLM SQLite Store、SQLite Journal、示例 custom Store 的无 `Release` 子进程退出恢复测试。对并发时序做重复抽样用 `make fault-test FAULT_COUNT=3`。安装了精确 OpenCode 1.17.18 后，完整产品链用 `make real-opencode-web-test REAL_COUNT=3`，三断点真实网络恢复门用 `make real-opencode-network-test REAL_COUNT=3`；后者默认每个断点 `REAL_NETWORK_DROPS=5`。故障点、证据边界和 outbox 人工处置见 [08 部署与运维](08-operations.md)。

## 架构决策（方向性，不进入本轮验收）

### 双栈边界：local 已收敛，远程部署保留为独立产品线

`human local` 已使用公共 `llm.Service`，公共 codec/Store/Policy/Transport 组合真实到达
CLI 产品；local 不再依赖 legacy completion gateway、workerbridge、worker WebSocket 或
outbox。`human gateway` + `human worker` 仍服务远程、多进程部署，现阶段继续使用
`internal/completion`，因此不能删除其 WebSocket、token 与 durable outbox 语义。决策是：

1. 公共 `llm`/`agent` 是所有新协议、策略和嵌入扩展的唯一目标；
2. `internal/completion` 仅维护远程产品兼容性和已有故障门，不再反向渗入 local；
3. `llm/builtin` 暂复用 legacy canonical/dialect 的纯 codec 代码；待移出公共 codec 后，
   才能机械删除对应 internal 包，不能用删行数驱动错误迁移；
4. 社区实现可替换 Store、Codec、Authenticator、Resolver、Policy/Router、Protector、
   Observer、caller/worker transport 和人侧；`workerkit`、Web 与 SQLite 是基础实现，
   不是不可替换的框架内核。

### Harness SPI：协议解析已抽象，产品能力继续按实测 profile 收口

`llm/callerhttp.RequestResolver` 已是 caller 身份与任务上下文的公共 SPI；官方
`harnessresolver` 基础实现精确识别 OpenCode 与 Claude；对精确 Codex 0.145.0 Responses
`codex_exec/` + canonical turn metadata，它提供稳定请求 key 和同 turn RemoteTools affinity，
因此工具结果 continuation 能回到同一任务。未知 Codex 版本保持 Chat 安全降级。解析与认证
不再散落在路由 handler 中。以下产品能力
仍是 exact harness profile，不能因已有 Resolver 就放宽成启发式猜测：

- `ResultCodec` 闭合枚举 + 按 harness 白名单的 `Validate()`（`internal/completion/adapter/profile.go`）；
- gateway 手写的按 harness 身份/幂等分发（`internal/completion/gateway/server.go` 的 OpenCode/Codex 分支）；
- `:pull` hydration 的 OpenCode 命令字面量（`internal/mirror/profile_tools.go` 与 `internal/tui/model.go` 两侧）；
- TUI 任务面板的闭合 harness 枚举与工具名 switch（`internal/tui/tasks.go`）；
- 产品 gateway 的三方言硬编码路由。

后续应把 hydration、任务工具 schema 和 reconciliation codec 继续收口成声明式
Profile + 少量策略接口；在这些能力落地并经过真实 harness gate 前，仍不宣称“新增
harness 无需改核心”。Claude 的已冻结文件/Task lifecycle profile 与 Codex 的
RemoteTools/Plan/`apply_patch` profile 只代表上述精确版本和实际声明的 behavioral schema。

## 后续真实 harness

以下记录 exact 客户端仍缺的长流程与部署扩展证据；未完成项不外推到其它版本或多租户支持范围。

### Codex

- Codex CLI 0.144.4 已有 500/响应前断 TCP 的重试黑盒、稳定 turn metadata 与仓库内派生-key测试。
- Codex CLI 0.145.0 已在空 `CODEX_HOME`、`--ignore-user-config --ephemeral` 下真实完成两套 Responses 门：独立 gateway 门实收 serial `exec_command`、namespace function 与 hosted `web_search` 的分类；公共 `human local` 门只经 Web API 操作 Human，CLI 实际执行命令、以同一 `call_id` 回传 `function_call_output`，再收到 final 并 exit 0。
- Responses partial SSE 与 `update_plan` 已由真实 Codex 0.145.0 容器门验证。retry 保持 canonical turn metadata 与服务端 idempotency key；body 只允许冻结 profile 中 `client_metadata` 的无语义重排/冗余 session 省略。
- 同一真实 CLI 在模型目录声明 `apply_patch_tool_type=freeform` 时已完成 Human/Agent 隔离目录下的 create→native `apply_patch`、modify→native `apply_patch` 和最终字节核对。resolver 只有在 exact version、turn identity、声明工具以及 `custom` grammar envelope 全部吻合时才升级 Workspace；未知模型 fallback、schema 漂移或 deferred tool 降级。仍缺 Workspace 断流/重启恢复门。

### Claude

- Claude Code 2.1.217 已通过真实容器 CLI + Playwright Web 人侧的 Messages RemoteTools 闭环：final、第二进程 `--resume`、`Bash` 成功 result→final、失败 result 回流后恢复 final，以及有界拒单。
- 2.1.217 捕获并冻结的 wire 事实：`POST /v1/messages?beta=true`；`x-api-key` 认证；不发 `Idempotency-Key`；`User-Agent: claude-cli/2.1.217 ...`；一个 canonical `X-Claude-Code-Session-Id`；`metadata.user_id` 内嵌 JSON 的同一个 canonical `session_id`。basic resolver 要求 header/body 双 UUID 相等后才授予 exact RemoteTools affinity；只有同时出现冻结的闭合 `Write`/`Edit` schema 才提升到 Workspace。未知版本只保留 request retry dedup，不继承工具 affinity。Stainless retry 仍以完整 JSON 语义摘要区分 retry 与下一 continuation。
- Claude 2.1.217 已真实验证 `TaskCreate → TaskUpdate → TaskList` 长流程、原生 `Write`→`Edit` Live Workspace、最终 caller 字节，以及完整 progress 帧后断 SSE 的同 body/session/idempotency 恢复。尚未完成的是接近 context window 的自动压缩行为；不能用普通多轮 continuation 代替该证据。

### 其它版本与扩展

- OpenCode 其它版本必须新增独立 profile 和 golden/真实 gate，不能静默放宽 `1.17.18`。
- OpenCode 1.17.18 普通 `read` 是带行号的有损展示文本且无 remote hash，不允许把它当 byte-exact hydrate；`:pull` 已提供逐文件 exact bootstrap，完整 clone/bundle/整仓播种仍是后续能力。
- 公共 `llm.Service + callerhttp + llm/workerws.Client` 已在 host-owned HTTPS/WSS 下验证两个 tenant/两个 durable worker 的 claims 路由、伪造 tenant header 不越权、无效身份 401、身份提供方故障 503，以及断线后 `HeaderProvider` 获取新短期 worker 凭据。独立 WSS 门还同时轮换服务端证书和客户端信任根、切断旧 socket，并由宿主注入的 `http.Client` 完成新 TLS session。它证明可嵌入的团队部署扩展点，不等于内置 OIDC/SSO 或自动证书管理。
- 多实例 active-active 存储、真实 IdP/代理滚动发布（含重叠信任窗口与回滚）和长期真实用户产品门仍需部署级验收；这些不进入单机 RC，但进入相应扩展的 go/no-go。

## 明确不做的替代品

- 不再用更长的健康连接 soak 替代故障注入。
- 不用“只做短排障”之类业务限制掩盖某个 harness 的协议缺口。
- 不根据相似 tool schema 猜 harness 语义，不用本机 matcher 冒充真实 Claude/Codex 支持。
- 不把完整 clone/bundle 当 Live Workspace 成立前提；它只是可选的可信上下文播种方式。
