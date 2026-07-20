# 06 · 产品 TODO 与证据账本

本页只回答三件事：什么已经成立、这轮还要验收什么、哪些必须等真实 harness。它不把“代码存在”写成“客户已兼容”。

> **产品定义**：Human 让人直接参与客户 Agent 的工作流，不按编码、排障、咨询、运维等业务场景限制边界。实时形态 HumanLLM 是**人占据客户 Agent 的 LLM / model-provider 协议位置**，客户 Agent 继续拥有上下文、权限、工具循环和真实执行环境；任务形态 HumanAgent 则用 durable Task/Message/Submission/Artifact 表达可暂停、可恢复、最终打包交付的协作。两者共享内容与 Workspace 语义，目标是复用同一写链机制，但不是同一个公开状态机；当前公共 `workspace` 值类型和 journal 只有 HumanAgent 链路在使用，尚非两面共享实现。

> **RC 交付边界**：当前候选发行只以 **OpenCode 1.17.18 单机 `human local` 路径**为可交付目标。Codex、Claude、其它 OpenCode 版本和远程多 worker/多租户是扩展证据，不阻塞该目标；本页仍逐项保留其缺口，避免把 codec 或局部真实 gate 写成完整兼容。

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

- OpenAI Chat Completions、OpenAI Responses、Anthropic Messages 的 stream/aggregate codec、canonical 转换、持久 HTTP decision 与逐字节重放均有仓库测试。codec 证据不自动外推给具体 CLI；Codex 0.144.5 的独立真实 gate 只把其 Responses Basic 文本/函数路径提升为实测。
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
- `human local` 已把 loopback HTTP、gateway、SQLite、worker 与 Bubble Tea model 合成一进程；`human gateway` 独立部署，`human worker` 只连远端。没有第二套 daemon 命令或裸 worker 兼容入口。
- gateway、worker outbox/state 与 caller ledger 各自只有一个带 version + fingerprint marker 的当前 SQLite schema；空库直接初始化，无 marker 或 marker 不匹配的开发库明确要求 recreate，不存在 ALTER/backfill、双格式读取或独立 migrate 命令。
- 新 `llm` package 公开 Store、Codec、Caller/Worker Transport、Protector、Router/Policy ports；官方 `llm/sqlite`、`llm/callerhttp`、`llm/workerws` 与 `protect/aead` 都是可替换 adapter，并由完整 custom-framework 示例验证。策略面已收口三条安全语义：AdmissionPolicy 必填（全放行须显式 `llm.AdmitAll()`）；caller 认证属性经 `Identity.Attributes` / `CallerAttributes` 只作为 AdmissionPolicy/WorkerRouter 的 advisory 输入，不进入请求身份或持久化；多 worker 且未配 Router 的 admission 以 `worker_router_required` fail-closed，不伪装成可重试的容量不足。`humantest` 另提供 LLM/Agent 两套歧义提交注入器与对账套件(`CommitUnknownLLMStore`/`TestLLMServiceCommitUnknownReconciliation`、`CommitUnknownAgentStore`/`TestAgentCommitUnknownReconciliation`),官方 `llm/sqlite`、`agent/sqlite` 与示例 custom Store 均通过演练。旧 `gateway` package 继续提供 turnkey handler/token/恢复，`worker` 暴露 Bubble Tea model，`local` 负责 loopback 组合。官方持久 adapter 当前只实现 SQLite，但 `agent.Store` / `llm.Store` 已是公共第三方 driver contract。
- local caller/worker token 可成对复用，CLI 只把明文写入 mode `0600` 文件，SQLite 只存 SHA-256；`--reset-credentials` 使用两阶段 journal：先持久化未激活的新 pair，再在同一 gateway 激活并原子切换 active，最后逐个标记并撤销旧 key。任一写盘、激活或撤销边界崩溃后，下次 `human local` 都从 journal 继续，不会先撤销唯一可用凭据。worker outbox 在 gateway 认证 hello 后按规范化 endpoint + 稳定 worker subject 绑定，不按 token 文本分库；轮换后的新 token 能继续补发同一 subject 的未 ACK 事件，跨 gateway/subject 隔离已有回归。
- `human local backup / verify-backup / restore` 已形成离线灾备闭环：按 canonical path 排序全持有 gateway/outbox/state owner lock，排除 local、独立 worker、gateway/token-admin 并发写；SQLite 用 source quick-check + `VACUUM INTO` + snapshot/staging/installed quick-check 吸收 sidecar 已提交状态；v2 archive 对固定 core 以及所选 `workspace_key` 的 `mirror/workspace`、`mirror/state` 逐项绑定 mode/size/SHA-256，并交叉验证 active credential secret 与 gateway token row、gateway identity/worker subject 与全部 outbox/state correctness row；旧 v1 caller-wide archive 明确拒绝。restore 默认拒绝所选 workspace 的非空目标，`--force` 经同目录 staging、持久 journal、old/new rename 整套提交，同时清除旧 sidecar，但不检查或替换同 caller 的其他 workspace；换 canonical DB 路径时 staging 内事务重绑 transport identity，不重写旧 in-flight workspace/root。任一崩溃边界由 `--resume` 收口且普通 local 启动 fail-closed。mirror symlink/特殊节点显式列入 skipped、不跟随。

### OpenCode 1.17.18 exact profile

- profile 只映射真实捕获的 `read/write/edit/bash`，使用绝对 `filePath/workdir`、允许并行调用；不虚构 search/delete/rename。它不是阻断其它 caller-declared tools 的全局 allowlist，同时维护 exact native tool 授权分类：mapped/已审 standard 默认可发，command/network/sub-agent 等 privileged 与未分类 custom/MCP 工具必须显式 active-capability opt-in（当前复用 `X-Human-Allow-Exec`）。
- 静态 provider headers 给出 capability tier、workspace key、`opencode@1.17.18` 与绝对 caller root。原生 `X-Session-Id` 只是 task affinity/candidate 材料：`session + model/system + 截止最新 user 消息的 canonical 历史` 先生成 candidate；同一 caller/workspace/exact harness/session 已有唯一非终态 task 时复用它。clarification → followup → tool call → result continuation 因而同 task；terminal 后下一顶层 user 才采用新 candidate。可选 affinity 必须与 session 一致。
- 无显式 key 时，caller/workspace/**harness session**、canonical digest 与完整 JSON 语义 digest 派生每个 request 的 retry key，材料不含可变 `task_id`：同请求重试同 key，历史/选项变化则新 key。精确 OpenCode 的无工具标题/摘要请求清空 task/workspace、隔离为 Chat，但保留 exact request retry key；声明任意工具则保留 Workspace，只有映射工具获得专用行为，privileged/unclassified 通用工具仍须 active-capability opt-in。
- 精确逐文件 bootstrap 已完成：Command 的 `:pull path` 经已声明 `bash` 与 caller Agent 权限闸运行 `opencode debug file read --pure`，持久绑定 hydration intent，严格解码 base64 exact bytes；空文件可 hydrate，前导 `-` 路径以 `./` 消歧。整个回流请求仍受 `8 MiB` wire budget 约束，大文件 fail-closed，不承诺 `16 MiB`。普通 OpenCode `read` 仍是有损展示，绝不拿来 hydrate。
- 真实 OpenCode 1.17.18 CLI gate 已完成：从空 Human mirror 走 `:pull native.txt` bootstrap，修改 mirror，生成绝对路径原生 `edit`，CLI 实际执行并回传成功 result，Human reconcile 后继续同一工具循环里的 `bash + todowrite`、final。独立的产品 gate 使用真实 `io.Pipe` 原始键值驱动生产 Bubble Tea model，覆盖 accept、连续两段流式回复、`:pull`、fsnotify review、preview/confirm、Tasks、Command 与 final；终态后重开 mirror 为零 pending change。该 gate 在本机连续 3 次通过，不再把内部 `Model.Update` 调用当作用户输入证据。
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
- Tasks 真实验证范围目前只有 OpenCode 1.17.18 `todowrite`。Claude/Codex matcher 存在，但不列为真实客户端支持。

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

### 双栈收敛：internal/completion 是 legacy composition，公共 `llm` 内核是唯一扩展面

现状必须诚实：`gateway`/`worker`/`local` 产品线构建在 `internal/completion/*` 上，
**不使用**公共 `llm.Service`；两者语义受同一 TLA+ 模型约束，但 canonical 类型与
completion 状态机在代码里各有一份。由此决策：

1. `internal/completion` 定位为 **legacy 产品 composition**：只接受 bug fix 与当前
   RC 必需的改动，新协议、策略、扩展点一律先落公共 `llm`/`agent` 内核。
2. RC 交付后启动 gateway → `llm.Service` 的迁移（先 caller 面，后 worker hub），
   目标是 CLI 产品与嵌入宿主运行同一个正确性内核。
3. 社区贡献（codec、Store、policy、transport）只对准公共内核与 `humantest`
   conformance；不为 `internal/*` 增加任何新扩展点。
4. 迁移完成前，公共内核的新 codec/Store **不会**自动出现在 `human local`；对外文档
   不得把 CLI 产品描述成可插拔。

### Harness SPI：绑定第二个真实 harness 提取，不提前抽象

当前只有 OpenCode 1.17.18 一个真实验收过的 harness，先具体后抽象是正确的；但接入
第二个真实 harness（Codex Workspace 或 Claude）时**必须在该工作中**提取 Harness
SPI，而不是继续复制以下五处硬编码：

- `ResultCodec` 闭合枚举 + 按 harness 白名单的 `Validate()`（`internal/completion/adapter/profile.go`）；
- gateway 手写的按 harness 身份/幂等分发（`internal/completion/gateway/server.go` 的 OpenCode/Codex 分支）；
- `:pull` hydration 的 OpenCode 命令字面量（`internal/mirror/profile_tools.go` 与 `internal/tui/model.go` 两侧）；
- TUI 任务面板的闭合 harness 枚举与工具名 switch（`internal/tui/tasks.go`）；
- 产品 gateway 的三方言硬编码路由。

形状草案：声明式 Profile 数据 + 少量策略接口（reconciliation result codec、
session/幂等 key 派生、hydration 命令构造与回读、任务工具 schema），OpenCode
1.17.18 成为第一个官方实现，TUI 按 profile 声明的能力渲染。SPI 落地前不宣称
"新增 harness 无需改核心"。

## 后续真实 harness

以下都是兼容性和部署面的扩展证据，不阻塞上述 OpenCode 1.17.18 单机 RC；未完成前也不列入对应客户端或多租户支持范围。

### Codex

- Codex CLI 0.144.4 已有 500/响应前断 TCP 的重试黑盒、稳定 turn metadata 与仓库内派生-key测试。
- Codex CLI 0.144.5 已在空 `CODEX_HOME`、`--ignore-user-config --ephemeral` 下真实完成两轮 Responses：首轮 serial `exec_command`，CLI 实际执行；第二轮用同一 `call_id` 回传含标记的 `function_call_output`，再收到 Human final 并 exit 0。gate 同时实收 namespace function 与 hosted `web_search`，并确认后者不可由 Human 调用。
- 待真实验证 Responses partial SSE 后是否重试、Tasks、Live Workspace 路径与故障恢复。在捕获并冻结 exact session/path/result 契约前，不注册 Codex Workspace profile，也不把 Basic 工具 gate 外推成完整 Codex 支持。

### Claude

- Claude Code 2.1.215 的 **Basic 文本闭环已真实验证**:真实 `claude -p` 连嵌入内核 `/v1/messages`,人侧全程 web API,final 逐字回流(`web/claude_real_e2e_test.go`,`HUMAN_REAL_CLAUDE_E2E=1`)。
- 黑盒捕获(2.1.215)已固化的 wire 事实:`POST /v1/messages?beta=true`;`x-api-key` 认证(非 Bearer);不发 `Idempotency-Key`,Stainless SDK 重试时 body 不变、仅 `X-Stainless-Retry-Count` 递增(门用 body 摘要作 retry 身份);`system` 是块数组;顶层携带 `thinking`(enabled+budget)与 `context_management`,builtin codec 均能解码;`metadata.user_id` 内嵌 JSON 含 `session_id`(未来 session 亲和材料);断连时按 SDK 节奏重试直至客户端超时(`X-Stainless-Timeout: 600`)。
- 待真实验证:tool_use/tool_result 工具闭环、`TodoWrite` 真实同步、部分 SSE 后的重试语义、上下文压缩行为;之后再决定 exact Workspace profile。Basic 门不外推为完整 Claude Code 支持。

### 其它版本与扩展

- OpenCode 其它版本必须新增独立 profile 和 golden/真实 gate，不能静默放宽 `1.17.18`。
- OpenCode 1.17.18 普通 `read` 是带行号的有损展示文本且无 remote hash，不允许把它当 byte-exact hydrate；`:pull` 已提供逐文件 exact bootstrap，完整 clone/bundle/整仓播种仍是后续能力。
- 多 worker 归属/负载、远程凭据与证书轮换、多实例存储、真实用户产品门另行验收；这些不进入单机 RC，但进入相应扩展的 go/no-go。

## 明确不做的替代品

- 不再用更长的健康连接 soak 替代故障注入。
- 不用“只做短排障”之类业务限制掩盖某个 harness 的协议缺口。
- 不根据相似 tool schema 猜 harness 语义，不用本机 matcher 冒充真实 Claude/Codex 支持。
- 不把完整 clone/bundle 当 Live Workspace 成立前提；它只是可选的可信上下文播种方式。
