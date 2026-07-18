# 05 · M0 可执行契约

[02](02-gateway.md) 讲"为什么"，本篇是"照着实现什么"——M0/M1 的精确字段、状态、时序。本文定义身份、adapter、循环、拒单与 read/search 的实现边界，02 相关节引用此处为准。

> **当前落点**：循环状态机、Live Workspace 与 Remote tools 核心已在 `internal/completion/`、caller shim、SQLite store、gateway、worker 协议和 TUI 中实现；仓库内覆盖重复 POST/tool-call、三方言聚合/重放、fsnotify/auto-send、持久多 continuation 与崩溃恢复。OpenCode 1.17.18 的 exact Workspace 已用真实 CLI 跑通 `空 Human mirror → :pull 精确字节 → native edit/result → bash + todowrite → final → 同 session terminal 后下一 user turn 新 task`。故障矩阵覆盖 caller 同 key 5 次断流、worker 冷启动/反复抖动/半开、在线 worker 遭 gateway/SQLite 重启及三方重叠掉线；Workspace 正式测试又覆盖离线原生 edit、gateway/SQLite 与 worker outbox 重启、并发重放、result continuation 和 save-ahead diff。Codex 0.144.4 已完成响应前重试黑盒，0.144.5 已真实完成 Responses Basic 的串行函数调用/result/final；Claude 只有本机契约测试。Codex Workspace/Tasks/Live Workspace 与 Claude 真实 harness 仍不能宣称支持。

## 1. 身份：三个正交概念，不可混用

旧设计把"conversation"既当路径/能力载体又当显示分组，是 bug 源；当前实现已拆开：

| 概念 | 负责 | 稳定性 | 来源 |
|---|---|---|---|
| `workspace_key` | 路径根 R、能力档、baseline、镜像目录名 | 稳定（一个工作区一个） | Remote tools/Workspace 档由 caller 提供 |
| `task_id` | 一次串行委托及其 clarification/tool 循环、lease（粘接单人）、幂等 | 稳定（一次委托一个） | shim 由受信配置显式提供；OpenCode 1.17.18 exact profile 先派生 candidate，再优先续用同 harness session 的唯一非终态 task |
| `ui_conversation_group` | 仅 TUI 显示分组 | 可错、可变 | 历史指纹启发式（G-05） |

在鉴权得到的 `caller_id` 命名空间内，**正确性只依赖前两个**；完整边界写作 `caller_id/workspace_key/task_id`。指纹只影响 TUI 卡片聚合，错了不影响状态/镜像/baseline。镜像目录按 `caller_id/workspace_key` 建（不是 `<conv>`）。

## 2. 接入三档要求的字段

| 档 | 必需字段 | 边界保证 |
|---|---|---|
| **Basic** | base_url + token | 文本 + 本次请求明确声明的原生 tools；真实 Agent 执行；每次 completion 独立，无需 adapter/shim |
| **Workspace** | exact `harness_id/version` + `workspace_key` + 根 `R` + 版本化 session/task 身份；每项操作仍须由请求实际声明对应原生工具 | Live mirror 保存/核对、原生 tool call、result reconcile 与跨 completion continuation；不要求 snapshot/完整镜像 |
| **Remote tools** | stable caller/workspace/task/key + `human-shim@1` 或等价边界 | 持久执行 ledger、强 CAS、realpath/symlink 与执行围栏；该 profile 同样可承载 Live mirror |

增强字段来自**版本化契约**，不是通用推断。缺稳定字段或未知 profile 时安全降级 Basic：保留本次声明的原生 tools，但清空 caller 提供的稳定 task/workspace，不承诺跨回合粘连、ledger/CAS 或精确幂等。adapter 不是全局工具 allowlist：它映射登记工具的语义并对 exact 增强档的 native tools 做授权分类。请求声明的其它工具仍可走通用入口，但不能自动获得镜像/path/result codec；mapped/已审 standard 默认可用，privileged 或未分类 custom/MCP 工具须显式 active-capability opt-in。精确 OpenCode 的无工具标题/摘要请求是版本化的辅助调用例外，会清空 task/workspace、隔离为 Chat，同时保留 exact request-level retry key。`human-shim@1` 从受信启动配置注入 caller/workspace/task/key，并把 caller 声明与 token principal 比对；错配返 `403`，缺声明返 `428`。

`opencode@1.17.18` 的静态 provider headers 必须给出 tier、`workspace_key`、harness ID/version 与**绝对** caller root；OpenCode 自己发送单一非空 `X-Session-Id`。gateway 以 `session_id + model + system + canonical messages through latest user` 先派生 candidate `opencode-task:v1:<sha256>`，再查 `(caller_id, workspace_key, harness_id/version, harness_session_id)`：存在唯一非终态 task 时复用它并覆盖 candidate。数据库唯一部分索引保证该 affinity 最多一个非终态 task。故 clarification → followup → tool call → result continuation 始终同 task；只有现有 task terminal 后，下一顶层 user 才采用新 candidate。若存在 `X-Session-Affinity`，必须与 session ID 完全一致。UA、版本或身份不匹配就 fail-closed/降级，不套用到其它 OpenCode 版本。

## 3. adapter 握手

每个 `harness_id@version` 一个版本化 profile，声明工具的**真实语义**（不靠 schema 猜）：

```
tools:
  read:    {name, args, 返回格式}
  search:  {name, args}            # grep/glob
  write:   {name, 整文件覆盖?}
  edit:    {name, 匹配语义: exact|fuzzy|line, old/new 字段名}
  delete/rename: {name, args}
  exec:    {name, cwd 语义, 超时, 审批, 输出/错误格式}
concurrency: 是否支持并行 tool_calls
path_style: workspace_virtual | absolute
result_codec: 版本化结果成功/失败与对账规则
session_identity: task 来源与一致性约束
error_shape: 工具失败的回传结构（供对账区分成功/失败/部分）
```

当前 exact profiles：

- `human-shim@1`：虚拟 `/workspace`，read/search/write/edit/delete/rename/exec，SHA 前置条件、caller-side ledger/CAS；
- `opencode@1.17.18`：绝对路径，且只登记已捕获的 `read/write/edit/bash`，并行调用；普通 `read` 是带行号的有损展示文本，不能用于 byte-exact mirror hydrate；`:pull path` 通过已声明的 `bash` 调用 `opencode debug file read --pure`，校验持久 hydration intent 后以 base64 精确播种单文件，支持空文件并用 `./` 消歧前导 `-` 路径；整个回流请求受 `8 MiB` wire budget 约束，不承诺 `16 MiB` 文件；write/edit 成功按精确结果文本及 delivery intent 对账，没有 remote SHA/CAS 证明。

未登记 harness → Basic，仍可调用本次请求声明的原生工具。已登记 adapter 只为映射工具提供专用语义，不把映射集合变成全局 allowlist；其它已声明工具仍可走通用入口，但 exact 增强档中的 privileged/unclassified 工具必须显式 opt-in。schema 形状绝不授予 caller 未声明的能力，也不允许凭相似 schema 虚构 search/delete/rename 或把未知 custom/MCP 工具静默判为 standard。

## 4. 循环状态机 + 幂等（核心）

真实工具流是**循环**，不是线性：

```
admitted ─→ leased ─→ awaiting_human ─→ responded
                ↑                          │
                │                          ├─(最终文本/交付)→ final:completed
                │                          ├─(澄清问题)→ awaiting_caller ─(下一请求)─┐
                │                          └─(含 tool_calls)→ tools_dispatched
                │                                                   │
        reconciled ←── awaiting_results ←──────────────────────────┘
        （tool result 或 caller 答复回流；对账后回 leased 继续）◄────┘
  任意态 ─→ final:{canceled | rejected | expired | failed}
```

规则：

1. **reconciled 回到 leased**（同一 `task_id` + 原接单人 lease），不重进 FIFO——工具循环靠此闭环。
2. **请求幂等**：默认由 caller 为每个逻辑 completion 分配随机 `idempotency_key`，同一次重试不变、下一逻辑请求换新；显式 key 始终优先。只有两个版本化自动 profile：
   - Codex Basic/Chat + Responses：精确 UA 与单一、非空、≤16 KiB metadata 中的 `request_kind="turn"`、canonical UUID `turn_id`，派生 `auto:codex-turn:v1:H(caller_id, turn_id, canonical_request_digest, full_wire_JSON_semantic_digest)`；它不授予 Workspace/ledger/CAS，可用 kill switch 关闭。
   - OpenCode 1.17.18 Workspace/Remote tools + Chat Completions：exact UA/profile 与稳定 caller/workspace/session 下，先按 §2 解析 candidate/活动 task；request key 独立派生为 `auto:opencode-turn:v1:H(caller_id, workspace_key, harness_session_id, canonical_request_digest, full_wire_JSON_semantic_digest)`，**不含可变 task_id**。相同完整请求的传输重试复用 key；clarification/followup/tool/result 可换 request key但复用非终态 task；terminal 后下一顶层 user 采用新 candidate。无工具的标题/摘要请求降级 Chat；声明了任意工具的请求仍保留 Workspace 身份。adapter 只为已登记工具提供专用映射，并对其它 exact native tools 分类；privileged/unclassified 调用要求 active-capability opt-in。

   两种语义摘要都保留数值精度、忽略对象键顺序并拒绝重复键/歧义 JSON；**禁止通用 body-hash 猜测合并**。阶段 A 先查 `(caller_id, key)`：同 key/摘要复用原任务并续接或重放，同 key/异摘要返 `409`。流式在派发前持久裁决 `200/SSE`，聚合在终态原子裁决 status/content-type/body/complete；并发重试只能遵循原 decision。完成后正文默认保留 24 小时，之后只留 tombstone：同摘要 `410`、异摘要 `409`，不能重新准入。
3. **执行幂等 + tool-call 对账**：每个 tool_call 有唯一 ID；Remote tools / Workspace 在整个 task 内也禁止不同 event 复用同一 ID，并在 durable step 前拒绝。`human-shim@1` 以 `(caller_id, task_id, tool_call_id)` 记 SQLite ledger；文件型 ledger 有持有至 `Close` 的跨进程独占 owner lock，第二个 shim 不会把首进程的 live `pending` 提前收口。正常完成后重启仍只重放原 result，崩溃遗留的 `pending` 则收口为可重放的 `execution_outcome_indeterminate`；当前进程两次持久化都失败时，同 ID 后续只重试终态提交而不重跑工具。所有歧义都要求核对工作区后换新 ID，绝不盲目重跑可能已发生的副作用。回流 result 按 ID 匹配，成功推进 baseline，失败/缺失保留未确认 diff。Live Workspace 在 tool event 进入 durable outbox 前持久记录 reviewed mutation、exact call ID/digest、base fingerprint 与已发送内容；迟到 result 只确认这份 delivery intent。若 Human 已保存更领先的草稿，baseline 只推进到已发送版本，新 diff 保留。**OpenCode 原生工具没有 shim ledger/remote SHA 保证**：该 intent 保证 Human 不会确认错本地版本，不证明客户侧工具 exactly-once 或远端内容 CAS。
4. **迟到 result / 重试**：result 晚到并入原 `awaiting_results`；客户端在部分 SSE 后重试 → 从持久 cursor 续接；聚合 decision 前的重复请求等待，decision 后逐字节重放完整 body。已经 `BeginRequest`、但尚未跨过 HTTP response boundary 的内部失败只终结该 request，不把整个 task 永久打成 `failed`：task 保持原 `admitted/reconciled` 并持久化失败请求摘要。caller 换新 key 重试时，只有同一 task、无其他 active request 且 canonical 摘要完全一致才能重新准入；task 级唯一部分索引保证并发新 key 最多一个成功。旧 key 始终逐字节重放原错误，正常状态推进或成功 response boundary 会清除这次窄重试授权。
5. **状态和响应边界在写出前持久化**：两种模式都先创建或幂等复用任务并落库 `admitted + canonical stream mode`。流式再持久化 stream start 与 `200/SSE` 裁决，写出并同步 flush `200 + start`，最后才 Enqueue 让 TUI 可见；flush 后 worker 消失只能持久化流内 `unavailable`。聚合不提前写 200：admitted/mode 落库后 Enqueue，终态才原子提交 status/body/complete，成功后一次性写出。重启分别恢复“已提交 200、尚未 Enqueue”的流式 assignment，或“已 admitted、HTTP 尚未裁决”的聚合 assignment。人按 `a` 接单后才进入 `leased`。
6. **worker 事件分阶段可恢复**：每个事件以稳定 `event_id + digest` 唯一，按 `step → state effects → applied → response complete/decision → receipt/ACK` 收口；`step/applied` 精确重放复用原行，冲突 fail closed。codec 的 response/event 时间种子与 wire 一并持久化：streamer 重建并逐帧核验；aggregate finalizer 重放同一 canonical 事件，逐字节核对终态 JSON 与已存 HTTP decision，绝不解析 SSE。任一阶段出现可重试存储错误后，当前 consumer 串行续跑该事件，不让 heartbeat、expiry 或下一事件越过；已 complete 但缺 receipt 的请求也会在启动恢复中补齐。服务端已因 `human_timeout` 等原因关闭 session、且无可精确重放的 receipt 时，原 owner 的迟到事件会收到携带 `caller_id + idempotency_key + event_id` 的非致命一等 `event_rejected` envelope。当前 worker 会把被拒 event、脱敏 assignment scope 与同帧累计 ACK 原子解析为有序 rejected inbox，socket reader 只提交并 ACK，独立 dispatcher 按序交给 TUI；TUI 首次应用后显式确认，事务以无 payload 的 event/rejection digest tombstone 取代 inbox 正文，因此确认提交前始终保留 durable 恢复来源、被拒 event 也不会重新进入发送队列，重复 rejection 不会重新入队。跨 owner 事件仍以 ownership violation 断链；同一 durable owner 的同 event ID/异 digest 冲突则按单条 `event_rejected + ACK` 隔离，不阻塞后续会话。未知 rejection 继续 fail-closed。启动恢复按请求隔离：可解码或 canonical raw bytes 已完全损坏的记录都会经 store 级 raw quarantine 持久有限化；200 前固定 500，200 后保留已提交状态并追加方言终态，同 digest 重放稳定、异 digest 409，不阻止其他健康请求。只有 dialect 自身损坏时退化为稳定通用 SSE error。无法取得可信快照的数据库级错误仍阻止启动。
7. **消息预算在持久准入前闭合**：gateway、caller shim 与 worker WebSocket 双向共用 `8 MiB` 上限。raw HTTP body 先受粗粒度限制；canonical assignment 再按完整 worker envelope（含最坏 `seq/ack`）精确编码检查，超限在 `BeginRequest` 前返 `413 request_too_large`。人类 event 也必须在进入 durable outbox 前通过同一检查。
8. 终态含 `canceled`（caller）/`rejected`（人拒单，§5）/`expired`（超 max_pending）/`failed`（人声明无法完成）。

## 5. 拒单时序：人工拒单在 assignment 可见之后

人只有在 assignment 推到 TUI 后才能按 `r`。所以人工拒单不可能伪装成阶段 A 的“无人在线/容量不足”，但 HTTP 表达取决于请求模式。

- **阶段 A（派发前）**：只做**机器可判断**的——鉴权、接单人在线、队列容量、限流；失败直接返回对应 HTTP 错误。
- **`stream:true`**：持久化并 flush 200/start 后才让 assignment 可见；人工拒单、超时、中途不可用只能走流内 error / 断流。
- **`stream:false`**：持久化 admitted + mode 后即可让 assignment 可见，但 HTTP decision 仍为 0；人工拒单在终态原子提交 `409 + 方言 JSON error`，超时/不可用/失败相应提交 `504 / 503(Anthropic 529) / 500`。
- **不在机器准入阶段阻塞等真人接单**。聚合请求的 HTTP 等待是它明确选择的一次性响应语义，不把 TUI 等待误算成 pre-admission 容量检查。

## 6. 原生工具 UI、Live Workspace 与可选 caller-side CAS

Basic 的所有工具都来自客户侧 Agent 本次请求的明确声明，Human 侧绝不执行。exact Workspace/Remote tools 还要求 privileged/unclassified 工具取得 active-capability opt-in（现由 `X-Human-Allow-Exec: true` 表达），随后仍由客户 Agent 做自身权限裁决。TUI 在此能力集合上提供三层输入：

1. **Tasks 全量列表编辑器**：仅完整匹配 `todowrite.todos[{content,status,priority}]`、`TodoWrite.todos[{content,status,activeForm}]` 或 `update_plan.plan[{step,status}]` 时启用；从历史 tool call/result 按 `tool_call_id` 恢复，编辑后调用原工具同步。它只表示 caller Agent 的计划，与 Inbox 分离。OpenCode 1.17.18 已有真实 fixture/闭环；Claude/Codex 只有本机契约适配和仓库测试，尚未真实 harness e2e。
2. **Command 编辑器**：仅 caller 声明兼容 `bash`，或 Remote adapter 对当前任务授权 exec 时启用；输入命令后只生成 tool call，由客户 Agent 执行，绝不在 Human 本地执行。精确 OpenCode Workspace 的 `:pull relative/path` 会生成 `opencode debug file read --pure` 调用，要求 exec opt-in 和 Agent 权限，严格解码 base64 后只 hydrate 该文件；空文件合法，前导 `-` 路径以 `./` 消歧，超出统一 `8 MiB` wire budget 时 fail-closed。
3. **高级 fallback**：其它声明工具按 `t` 输入 `<tool-name> <JSON object>`，一行一个；schema 不匹配时禁用专用编辑器，不猜语义，也不授予声明之外的能力。

因此 Basic 的 read/search 直接使用客户侧 Agent 本次声明的原生工具；Agent 执行并在下一 completion 回传 result。Live Workspace 还会让 mirror watcher 在保存后 fresh review，再按 exact profile 生成原生 edit/write：默认人工 preview/confirm；显式 auto-send 仅自动发送 change-level `allow` 的改动，安全 warning/block 或冲突停住。OpenCode 的“无 CAS”adapter warning 会展示但不阻断显式 auto-send，最终权限仍在客户 Agent。

OpenCode 1.17.18 的真实 CLI gate 已从空 Human mirror 生成 `:pull native.txt`，让真实 CLI 执行 `opencode debug file read --pure` 并以精确字节建立 baseline；随后修改 mirror、生成绝对路径原生 `edit`，收到成功 result 后按 delivery intent reconcile，再执行原生 `bash + todowrite`、final，并在同一 OpenCode session terminal 后的下一 user turn 建立新 task。该具体用例没有 save-ahead，所以 Review 归零；实现另有单元和 Workspace 三方故障测试钉住 save-ahead 时新 diff 必须保留。普通 OpenCode lossy `read` 仍不能 hydrate，`:pull` 也不是整仓同步；原生 edit/write 不提供 shim ledger 或 remote SHA/CAS。

需要强 ledger/CAS 时，再使用 **caller shim 或等价 harness adapter**：

1. 客户侧集成以受限 token 读取 `GET /internal/v1/tools/schema`，获得 `human_read_file`、`human_search`、`human_write_file`、`human_edit_file`、`human_delete_file`、`human_rename_file`，以及显式开启时才出现的 `human_exec`。
2. 人的 completion 响应发出相应 tool call；客户侧 Agent 调用 `POST /internal/v1/tools/execute`。shim 不信任 body 内的 caller/task 身份，而是用受信启动配置覆盖，再在客户工作区执行。
3. tool result 由客户侧 Agent 放入下一次 completion，请求回到同一 `task_id` 与原 lease；gateway 对账后继续循环。

**caller-side CAS（仅 shim/等价边界）**：read 返回内容指纹；write/edit/delete/rename 带 `expected_sha256`。shim 在真实文件系统上校验指纹、realpath 与 symlink 后落盘，并在解析后真实相对路径上再次拒绝 `.git` 段；所以 `alias -> .git` 同样不能写、改、删或作为 rename 任一端。不匹配显式失败，随后重新 read。执行账本以 tool-call ID 保证重试只重放原结果。这是 Remote tools 的强化保证，不能套在 OpenCode 原生 Workspace 上，也不宣称与任意外部进程的 symlink swap 跨进程原子。

## 7. 本契约对应的验收点（M0）

**仓库内功能已验证**：项目自有 gateway + caller shim 通过直接 HTTP/tool 协议连续跑通 20 次 `read→result→edit→result→exec→final`；显式 key 的重复 POST 与重复 tool-call 复用持久结果；两个同 body 独立 key 不折叠，工具账本可跨 shim 重启重放。

**仓库内故障矩阵已验证**：

| 故障 | 注入与恢复 | 被钉住的不变量 |
|---|---|---|
| caller TCP/SSE 断开 | 同一显式 key 用 5 个新 TCP 连接分别在 200、首帧和多个 progress 边界断开，第 6 次恢复 | 只有 1 个 task/assignment/final；恢复 wire 与持久记录逐字节相同 |
| caller 单向半开 | caller 保持 TCP 但不再读取 SSE；分别命中首次流与同 key replay 写入 | 只限制每次 Write+Flush，默认 10s 后 handler 返回；空闲等待 Human 不受绝对 deadline 限制；durable session 可由下一连接续接 |
| 未识别 caller 无 key 重发 | OpenCode Chat/普通 profile 用 5 次相同 body，不带 key | 得到 5 个独立请求/task；不做通用 body-hash 猜测合并 |
| Codex Responses 派生 key | 无显式 key 时连续 5 次断流、第 6 次恢复；另做 30 个并发 + 1 个顺序重放 | 两组都只创建 1 个 task/assignment，所有响应 wire 逐字节相同 |
| OpenCode exact task/request 身份 | candidate 由 session + 最新 user 历史生成；构造 clarification→followup→tool→result→terminal→下一顶层 user；同请求并发 retry；无工具辅助请求 | 非终态 task 覆盖 candidate；terminal 后才新 task；retry key 不因 task 解析变化且同请求稳定；辅助请求隔离 Chat；歧义身份 fail-closed |
| worker 冷启动时 gateway 离线 | 连续 5 次 503 或 connection-refused，然后启动 gateway | worker 不退出，有界退避后恢复接 assignment 与 ACK |
| worker 运行中抖动 | 连续 5 次切断 WebSocket，每次离线期先将 progress 写入 outbox | 每个事件只交付一次，ACK 后 outbox 清空，后续 final 不被队头毒丸阻塞 |
| worker 半开/凭据失效 | 对端保持 TCP 但不读；初始或重连时返 401/403 | ping timeout 使半开进入重连；凭据错误是终态且不自旋 |
| 两个 worker 共用 token | 同一进程重连复用 instance ID；第二个进程用不同 instance ID 连接 | incumbent 保持在线；第二个收到明确 policy close 并停止重试，不形成互相顶替循环 |
| gateway/SQLite 重启 | 在持久 response/event 不同阶段中断并复用原库 | 启动恢复重建原请求；单条坏记录隔离，不阻止健康记录 |
| 重启时调小 queue capacity | 原库有 3 条在途，配置从 3 降为 1 | 3 条 durable backlog 仍可恢复排空；active 回到阈值前拒绝新 admission，gateway 不因第 2 条恢复记录退出 |
| gateway/SQLite 重启、worker 进程仍活着 | caller 已见 partial SSE；切断服务和 worker socket，worker 离线写 final；同 key caller 离线失败 5 次后重启 gateway | worker 自动重连、outbox 回放 final；两次 caller replay 逐字节相同；1 request/1 task/3 distinct receipts |
| caller + worker + gateway 重叠掉线 | caller 断流，worker 离线写 final 后进程退出，gateway/store 关闭；离线期 5 次 caller 失败，再先恢复 gateway 与 5 个并发同 key caller，最后重开 worker/outbox | 5 个 caller 收到相同单次 final/`[DONE]`；数据库只有 1 个请求/1 个 task，无重复 assignment，outbox/rejected inbox 收空 |
| Workspace 三方重叠掉线 | reviewed v1 edit/delivery intent 已落盘；caller、worker、gateway 同时断开，edit 仅进 worker 磁盘 outbox；重启 gateway/SQLite 与 worker，3 个同 key caller 并发 replay，再回传 tool result；期间 Human 已保存 v2 | 3 份响应逐字节相同且各含同一 call ID 一次；数据库只有 1 份 edit receipt/step+applied；两次 completion 同一 task；result ledger 重放不重复推进；baseline 到 v1，Review 仍精确保留 v1→v2 diff。真实 CLI 执行次数不由此测试证明 |
| 消息尺寸边界 | raw body 恰好等于/大于限制；构造 raw 较小但 JSON 转义后 assignment 超限；event 超限 | 边界内可转发；超限统一 `413` 且不落请求/不派发；超限 event 不进 outbox |

上表的恢复有三个边界：

1. caller 精确续传需要同一 key 与同一 canonical 摘要。默认显式提供；严格匹配的 Codex Responses turn 与 OpenCode 1.17.18 Workspace turn 可分别派生。OpenCode Basic/Chat 与其它未识别无 key 请求仍是新请求。Codex 0.144.4 黑盒只证明响应前 retry 身份；0.144.5 真实 gate 已证明 Responses Basic 的串行函数调用/result/final，两者都不能代替 partial SSE 恢复或 Codex Workspace profile。
2. 恢复只对 `max_pending` 剩余窗口内的请求成立。超过后原请求已 `expired`；迟到 worker 事件可被持久拒绝并恢复为草稿，但不会使旧请求复活。
3. worker outbox 保护已发送事件；默认 worker state DB 另持久化 Reply/Command/Tasks/Advanced tool-call 草稿、rejected drafts 与最多 32 个 continuation。mirror preview 等未列入 state DB 的瞬时 UI 状态仍不承诺恢复。

**OpenCode 外部已验证**：1.17.18 + OpenAI-compatible Chat 完成 text SSE、同轮 `todowrite + write`、后续 `edit → bash → todowrite → final` 及成功/失败 read result 回流。exact Workspace 的 opt-in 真实 CLI gate 从空 Human mirror 发出精确 `:pull` bootstrap，真实 CLI 以 `--pure` 返回 base64 文件字节；随后修改 Human mirror、执行绝对路径原生 `edit`、回传 `Edit applied successfully.`、reconcile baseline，再在同一工具循环执行 `bash + todowrite` 并 final。第二条顶层 user 消息复用同一 OpenCode session，但因旧 task 已 terminal 而正确得到新的 Human task 和 request key。无工具标题/摘要请求会隔离 Chat；声明任意工具的请求不会仅因工具未被 adapter 映射而丢掉 Workspace 身份。

历史持续流依次通过 10m 与 2h；这些数字只证明当时 OpenCode Basic/Chat + `stream:true` 的心跳链，**不再继续增加时长，也不是当前验收门**。当前门只测断网、半开、ACK 丢失、进程/SQLite 重启与三方恢复顺序。

**Codex 重试黑盒已验证（狭范围）**：Codex CLI 0.144.4 在捕获端返 500 与读完 POST 后断 TCP 时均显示 `Reconnecting 1/5…5/5`，两组捕获端各收到 30 个 POST；UA 为 `codex_exec/<version>`，没有显式 key。metadata 中 `turn_id` 在同一用户 turn 的 A/B/B/B 工具循环不变，下一用户 turn 更换。这支持当前 profile 的派生 key 决策，不证明 Codex 已能通过 gateway 完成部分 SSE 恢复或工具闭环。

**Codex Responses Basic 工具闭环已验证（狭范围）**：Codex CLI 0.144.5 在隔离空 `CODEX_HOME` 中实收串行策略、普通 `exec_command`、namespace functions 与 hosted `web_search`；Human 发出命令后 CLI 实际执行，并用相同 `call_id` 回传 result，再消费 Human final 后正常退出。它证明当前 Basic 文本/函数 wire，不证明 partial SSE retry、Tasks、Workspace 或 Live Workspace。

**M0 仍待外部验证**：OpenCode 真实 CLI 在 partial SSE、gateway/worker 反复掉线与恢复顺序中的行为，以及完整 TUI 保存/auto-send 体验；Codex Responses 的 partial SSE/故障恢复与 Workspace/Tasks/Live Workspace；Claude/Anthropic 的真实工具闭环；真实凭据/证书轮换和多 worker。项目内部 fault tests 证明 request/event/outbox 不变量，不证明无 shim 的 OpenCode 原生文件执行 exactly-once。后续清单见 [06](06-product-todos.md)。
