# 02 · Human Gateway 设计

Human 的模型端对外扮演 Anthropic / OpenAI API，对内把请求交给人、把人的响应转回
原方言。人占据的是 **LLM 协议位置**；上下文仍由 harness 带来，工具仍由调用方 Agent
在自己的工作区与权限系统中执行。当前本机实现是 `llm.Service + callerhttp +
workerkit + web`：Service 负责三方言无关的准入、幂等、lease、事件、响应和恢复；codec
负责 wire bytes；transport 负责 HTTP/WS；人侧只消费公共 worker port。`human gateway`
仍是独立远程兼容产品，不再定义 `human local` 或新嵌入 API 的内部结构。

> 本篇吸收 codex review 的 P0 修正：接入分档、跨回合状态机、准入/响应模式错误边界、显式 adapter、稳定标识、默认安全。

> **实现状态**：本篇协议核心、SQLite 持久化、worker WebSocket、caller shim、Chat/Messages/Responses 的 stream/aggregate codec 与脱敏 golden fixtures 已有 Go 实现和仓库内测试，三协议 aggregate/stream 还共同通过默认 Web-human 产品门。OpenCode 1.17.18 的 Basic 工具闭环及 exact Workspace 的真实 CLI `空 Human mirror → :pull 精确字节 → 原生 edit/result → bash + todowrite → final → 同 session terminal 后下一 user turn 新 task` 已通过。Codex CLI 0.145.0 已在空 `CODEX_HOME` 下真实跑通公共 local 栈 Responses `exec_command → function_call_output → final`、namespace/hosted 工具分类，以及模型目录启用 freeform 时的 custom `apply_patch` Workspace create/modify。Claude Code 2.1.217 已通过 Messages + Web 的 `Bash` 成功/失败、`Write`/`Edit` Workspace 与 `TaskCreate → TaskUpdate → TaskList` 闭环，并以 header/body 双 session UUID 进入 exact affinity。三端正式 Web Tasks 面板均已跑完 pending→in_progress→completed。Workspace 三方故障正式测试还覆盖原生 edit 在 caller/worker/gateway 重叠离线时进入持久 outbox、gateway/SQLite 与 worker 重启、并发 caller 重放、result continuation 与 save-ahead reconcile；该故障证据尚未外推到 Codex Workspace。

## 1. 接入分三档（"一行配置"属于 Basic 档）

"改一行 base_url"只对最浅的档成立。三档表达协议/正确性能力，不限制可以处理的业务，也不是必须依次升级的产品阶段：

| 档 | 需求方要做 | 能力 |
|---|---|---|
| **Basic** | base_url + token | 标准模型协议的文本 + caller 本次明确声明的原生 tools；真实 Agent 执行工具并在下一 completion 回传 result。每次请求独立，不需要 adapter/shim |
| **Workspace** | base_url/token + exact `harness_id/version` + `workspace_key` + 根 `R` + 受验证的 session/task 身份；具体操作仍须由请求声明相应原生工具 | Live Workspace：本地保存 → review/可选 auto-send → 客户 Agent 原生 edit/write/bash/tasks → result 回流与 continuation；不要求完整仓库预传 |
| **Remote tools** | stable caller/workspace/task/key + `human-shim@1` 或等价 caller 边界 | 在原生工具循环之外提供持久执行 ledger、强 CAS、realpath/symlink 与执行围栏；该 exact profile 也可承载 Live mirror（字段见 [05](05-m0-contract.md) §2） |

代码已覆盖三档核心边界。`opencode@1.17.18` Workspace profile 只映射实际捕获的 `read/write/edit/bash` 文件/命令语义，使用 caller 的绝对根，不把工具 schema 猜成能力；profile 不是拒绝其它 caller-declared tools 的全局 allowlist，同时显式维护 native tool 授权分类。mapped 文件工具与已审 standard 工具（如 `todowrite`）默认可发；`bash/task/webfetch` 等 command/network/sub-agent 工具，以及未分类 custom/MCP 工具，必须由任务显式设置 active-capability opt-in（当前复用 `X-Human-Allow-Exec: true`）。普通 `read` 结果是带行号的有损展示文本且没有远端 hash，仍不能用于 byte-exact hydrate；但 TUI 的 `:pull path` 已能通过客户 Agent 的原生 `bash` 权限闸运行 `opencode debug file read --pure`，校验持久 hydration intent 后从 base64 精确播种单个文件。空文件可 hydrate，前导 `-` 路径会改写为 `./-...`，避免被 CLI 当选项。整个 tool result/request 仍受 `8 MiB` wire budget 约束，过大 pull fail-closed，不承诺 `16 MiB` 文件。真实 CLI 已从空 Human mirror 完成该 bootstrap，再生成原生 `edit`、核对 result、执行 `bash + todowrite`、final，并在同一 OpenCode session terminal 后的下一 user turn 建立新 Human task。`:pull` 不是整仓同步；原生 edit 也没有 remote SHA/CAS 证明，外部编辑竞态仍须显式披露。`human-shim@1` 则使用虚拟 `/workspace` 并提供更强 CAS/ledger。这些是 profile 边界，不把 OpenCode 证据外推给 Codex、Claude 或其它版本。

## 2. 三方言端点

| 方言 | 端点 | `stream:true` | `stream:false` | 工具 |
|---|---|---|---|---|
| OpenAI | `POST /v1/chat/completions` | `data:` SSE，`[DONE]` 收尾 | `chat.completion` JSON | `tools`/`tool_calls`/`finish_reason` |
| OpenAI Responses | `POST /v1/responses` | 具名 SSE event 流 | `response` JSON | `function_call`/`function_call_output`/`response.completed` |
| Anthropic | `POST /v1/messages` | SSE 具名 event 流 | `message` JSON | `tool_use`/`tool_result`/`stop_reason` |

\+ `GET /v1/models`（列 `human-expert-*` 伪模型名）与 Anthropic
`POST /v1/messages/count_tokens`（人类模型没有 tokenizer，返回稳定估算）。鉴权走各方言标准头。

三条模型调用路径由官方 `openai-go/v3 v3.37.0` 与
`anthropic-sdk-go v1.58.1` 做黑盒契约门：SDK 负责真实请求序列化，Human codec
解码后产生 text/tool terminal，再由 SDK 解析 aggregate、SSE 文本和流式 tool call，并把 tool result
作为下一请求回传 Human；标准错误 envelope 也由两套 SDK 反序列化，Anthropic SDK 还直接调用 `count_tokens`。SDK 版本固定在 codec manifest，wire 投影变化必须提升各自 codec version/
fingerprint。该门只覆盖模型调用面，不声称支持 Models/Batches/Files、存储响应查询、
provider container/conversation、server tools/MCP 等厂商产品面。

**Responses API 边界**：Responses request/stream codec 已作为第三 adapter 实现并有 golden 回归；流先按 `response.created → response.in_progress` 宣告生命周期，文本再按 `output_item.added → content_part.added → delta* → output_text.done → content_part.done → output_item.done → completed`，函数调用按 `output_item.added → arguments.delta/done → output_item.done → completed` 输出。成功但正文为空时仍输出一个显式的 assistant / 空 `output_text` 生命周期，不会退化为 `output:[]`；流内失败以含完整 response 快照、`status:failed` 与 error 对象的 `response.failed` 收口，已经流出的文本在该快照中标为 `incomplete`。wire 层的 `error.code` 使用 Responses API 的 `server_error` 枚举，项目内部的 `human_timeout` 等细分原因仍保留在持久事件与错误消息中，不把私有码泄漏给严格 SDK。普通 function 与 namespace function 以 `(namespace, name)` 作为正确性身份；`parallel_tool_calls:false` 在 canonical、持久 stream seed、gateway 与 TUI 全程保持，并限制每个响应只能发一个 call。provider-hosted `web_search` 保留为不可由 Human 调用的 capability 提示。typed reasoning input 只以 SHA-256 参与请求摘要，不进入人类 transcript、assignment 原文或 worker state。完整 transcript golden、accumulator、恢复 seed 与逐字节重放测试共同钉住事件顺序和 `sequence_number`。

**Codex 0.144.4 重试 profile（只是 Basic/Chat 请求级幂等）**：真实 CLI 黑盒中，捕获端返 500 或读完 POST 后断 TCP，CLI 都显示 `Reconnecting 1/5…5/5`，两组捕获端各收到 30 个 POST。请求没有 `Idempotency-Key`，但具有 `User-Agent: codex_exec/<version>` 与单一 `X-Codex-Turn-Metadata` JSON；观测到工具循环 A/B/B/B 共用同一用户 turn `turn_id`，下一用户 turn 才更换。gateway 在显式 key 缺失时，仅对能力解析/降级后的最终 `TierChat` + Responses + `codex_exec/<version>` + `request_kind="turn"` + canonical non-nil UUID `turn_id` 启用，且 metadata 必须恰好一个、非空且 ≤ 16 KiB。派生 key 为：

```
auto:codex-turn:v1:H(caller_id, turn_id, canonical_request_digest, full_wire_JSON_semantic_digest)
```

显式 key 始终优先；原 metadata header 不落库，完整 wire JSON 语义摘要保留所有字段与数值精度、忽略对象键顺序，并拒绝重复键/歧义 JSON。已匹配 Codex + Responses 但身份 material 畸形时返 `400`；未知 UA、无 header、无 `request_kind` 或非 `turn` 请求安全降级为普通随机 key。该 key 只在 Basic/Chat 的一次 request 粒度去重，**不授予** Remote tools 的 task/workspace/ledger/CAS 能力；可用 `--disable-codex-auto-idempotency` 紧急关闭。因 `turn_id + 两种请求摘要` 是唯一可观测身份，**同一 turn 内两个主动且完整 JSON 语义相同的逻辑请求无法与 retry 区分，会被合并**。这是当前严格 Codex profile 的显式边界，不是通用 Responses body-hash 策略。

**Codex 0.145.0 Responses 工具 gate**：测试用精确版本的真实 `codex exec`、空 `CODEX_HOME`、`--ignore-user-config --ephemeral` 和受限测试容器，不读写用户配置。独立 gateway 门实收 `parallel_tool_calls:false`、普通 `exec_command`、`multi_agent_v1::*` namespace functions 与 hosted `web_search`；公共 `human local` 门再证明 Human 从正式 Command 面板发出 `exec_command` 后，Codex 在调用方工作区实际执行，并以相同 `call_id` 回传 `function_call_output`，随后消费 Human final、exit 0。正式 Tasks 面板还完成 `update_plan` 三态 continuation；fault proxy 在完整 progress 帧后切断 SSE，真实 CLI 以冻结的 body/session/idempotency profile 恢复同一 durable turn。模型目录明确 `apply_patch_tool_type=freeform` 时，Codex 声明 Responses `custom` grammar tool；resolver 只有在 exact version/turn/tool/grammar 全部匹配时升级 Workspace，Human 与 Agent 的隔离 repo 已真实完成 create/modify→原生 patch 和最终字节核对。缺少元数据、未知模型 fallback、schema 漂移或 deferred tool 都保持 Chat/RemoteTools。

**OpenCode 1.17.18 exact Workspace profile**：只有同时满足 `harness_id=opencode`、`harness_version=1.17.18`、Workspace/Remote-tools tier、`User-Agent` 前缀精确匹配、稳定 `workspace_key`/绝对根 `R` 与可验证的 session 身份时才启用。OpenCode 自带的单一非空 `X-Session-Id` 不是 `task_id` 本身；gateway 先以 session、model/system 和截止最新 user 消息的 canonical 历史生成 candidate task：

```
opencode-task:v1:H(session_id, model, system,
                     canonical_messages_through_latest_user)
```

若存在 `X-Session-Affinity`，它必须与 session ID 完全一致。candidate 之后再按 `(caller_id, workspace_key, harness_id/version, harness_session_id)` 查询：若已有唯一非终态 task，就以现有 task 覆盖 candidate。唯一部分索引保证每个 exact harness affinity 最多一个非终态 task。因此 clarification → followup → tool call → result continuation，即使增加了新的 user 消息，也始终复用当前 task；只有它进入 `completed/canceled/rejected/expired/failed` 后，下一条顶层 user 消息才采用新 candidate。

没有显式 key 时，每个完整请求独立派生：

```
auto:opencode-turn:v1:H(caller_id, workspace_key, harness_session_id,
                          canonical_request_digest, full_wire_JSON_semantic_digest)
```

retry key **不含最终解析出的 task_id**：活动任务查询可能在并发 transport retry 之间改变 candidate 的归属，但同一 harness session/body 的请求身份不能随之变化。完整请求语义相同的传输重试精确复用原 key；历史或选项改变则新 key。显式 key 仍优先；重复/空 session header、歧义 JSON fail-closed。静态 provider header 也会随标题/摘要辅助请求发送，所以精确 OpenCode 的**无工具**请求会清空 task/workspace、隔离为 Chat，但保留已派生的 exact request retry key；声明了任意工具的请求仍保留 Workspace 身份，其中只有 profile 已映射的工具可驱动专用 mirror/Command 能力。其它声明工具仍可走通用入口，但 exact 增强档中的 privileged/unclassified 工具须显式 active-capability opt-in。真实 OpenCode 1.17.18 CLI 已通过空镜像 `:pull`、绝对 root、Human mirror `edit`、原生 tool result 对账、`bash + todowrite`、final，以及同 session terminal 后下一 user turn 新 task 的可执行 gate。

**当前兼容边界**：三方言同时接受 `stream:true` 与 `stream:false`。聚合模式不是把已编码 SSE 读回来再拼 JSON，而是独立 finalizer 消费同一 canonical worker event：progress 只累积、不向客户端泄漏中间帧，最终 text/tool/error 才生成一份方言原生 JSON。终态 HTTP decision 与完整 body 原子落库，重复 key/摘要逐字节重放；进程在 decision 后、worker receipt 前退出时，恢复会从持久 step 重建并核对 body/status 后补 receipt。聚合连接没有应用层 heartbeat，因此长挂与动态体验仍优先使用 `stream:true`。

每个顶层字段必须落入三类之一：转换进 canonical、经类型/范围校验后成为显式 no-op、或明确拒绝。`parallel_tool_calls` 已在 Chat Completions 与 Responses 中转换为 canonical 串行/并行策略并受核心强制；三方言的 `tool_choice:none` 转换为禁用策略，内置 codec 不向 Human 暴露工具，公共 Service 与旧 gateway 仍二次拒绝越权调用。token 上限、采样、缓存、service tier、verbosity、OpenAI reasoning 与 Anthropic thinking/output effort 对人类模型没有 provider tokenizer/scheduler 可控制，因此校验后显式忽略；不会再靠 Go JSON 的未知字段行为意外吞掉。指定/强制工具、结构化输出、Responses 的非空 `previous_response_id`/conversation/prompt、`store:true`、Anthropic container/fallback/provider MCP，以及除 Responses `web_search` 不可调用身份提示以外的 provider-hosted tool 仍 fail-closed。Responses 的 `include:["reasoning.encrypted_content"]` 与合法 reasoning hint 被接受，但 Human 不伪造 provider reasoning item。三种顶层 envelope 均严格拒绝未登记字段。

## 3. canonical 与转换矩阵

逻辑在方言无关的内部格式上进行，每方言一个 adapter：

```
/v1/chat/completions ─┐                         ┌─ OpenAI stream / aggregate encoder
/v1/messages ─────────┼─► canonical ─► [人] ─►──┼─ Anthropic stream / aggregate encoder
/v1/responses ────────┘   request/response      └─ Responses stream / aggregate encoder
```

| 概念 | OpenAI | Anthropic | canonical |
|---|---|---|---|
| system | `role=system` | 顶层 `system` | `system: string` |
| 多模态 | `image_url` | `image(source)` | `blocks[]`(text/image) |
| 工具定义 | `function{parameters}` | `input_schema` | `tools[]`(统一 JSON schema) |
| 工具调用 | `tool_calls{name,arguments(串)}` | `tool_use{name,input(对象)}` | `tool_uses[]`(对象) |
| 工具结果 | `role=tool` | `tool_result` block | `blocks[tool_result]` |
| 结束 | `finish_reason` | `stop_reason` | `stop: end\|tool_use\|max` |

**canonical schema 版本化**：每次演进有版本号；转换器是纯函数。仓库已为三种方言加入脱敏代表性 request + 工具 schema + canonical golden，并由测试实际执行 decode→canonical→stream。真实 harness 抓取样本仍需在 M0 脱敏后补入，不能用合成 fixture 替代外部兼容证据。

## 4. 请求：准入 / 两种响应模式（修 200-vs-503 冲突）

**一旦写出 HTTP header 就不能再改状态码**。准入先持久化，再按请求模式选择不同的可见边界：

```
── 阶段 A · 准入（200 之前，可返标准 HTTP 错误）────────────
  鉴权/解析 → 按 (caller_id, idempotency_key) 查记录并校验请求摘要
    · 同 key + 同摘要：复用原任务与响应事件日志（即使当前离线/满载）
    · 同 key + 不同摘要：409 idempotency conflict
    · 未命中：限流 → 接单人在线状态 → 队列容量 → 持久化 admitted（§5）
  其他失败：直接 401/429/503/400（真 HTTP 状态，harness 原生重试接管）
── 阶段 B1 · stream:true ──────────────────────────────
  持久化 200/SSE decision → 写并 flush 200 + 起始帧 → 才推送 TUI
  人接单后进入 leased；期间可吐 progress delta，并用 SSE heartbeat 保活
  终态由方言 streamer 输出 text/tool/error 帧；HTTP 状态已不可改变
── 阶段 B2 · stream:false ─────────────────────────────
  admitted + canonical mode 已持久化后推送 TUI；HTTP decision 暂不决定
  progress 进入同一持久 worker step/state machine，但只在 finalizer 内累积
  终态原子提交 {HTTP status, application/json, 完整 body, complete}
    · text / tool_calls → 200 + 方言原生聚合对象
    · reject / timeout / unavailable / failed → 409 / 504 / 503(Anthropic 529) / 500 + 方言错误对象
  提交成功后才向客户端一次性写 header + body；重复请求等待同一 decision
  ▼
审计落库；连接关闭
（人无法主动发起——completion 无反向通道；主动信息搭下次响应）
```

两种模式共享 canonical event、任务状态转换、工具账本与 worker receipt，但拥有不同的 HTTP 可见边界。流式恢复逐帧核验；聚合恢复从持久 step 重放 finalizer，并将重建 body/status 与已提交 decision 逐字节核对，绝不解析自己的 SSE。

**端到端消息上限是一个协议预算，不是三处独立默认值**：completion gateway、caller shim 与 worker WebSocket 双向统一为 `8 MiB`。HTTP 先用该值限制 raw body；解析成 canonical 后、`BeginRequest` 之前，gateway 再把完整 assignment（含路由身份、adapter、tool schema 和最坏 `seq/ack` 位数）按 worker 实际 JSON envelope 编码并精确计数，超过上限返 `413 request_too_large`，不落任务、不进队列。第二层不可省略：raw JSON 中一个字节的 `<` 等字符在 worker JSON 中可能转义成六字节，所以“小于 8 MiB 的请求”不等于“一定可派发”。反向的人类 event 同样在写入 durable outbox 前检查完整 envelope；超限显式报错，不允许形成会令 WS 反复断开的 poison record。

HTTP handler 等待新事件或终态 decision 时使用进程内、按 request 隔离的广播通知；5 秒定时器只在本进程漏掉通知或未来由其他进程提交时重新读取 durable store，作为低频兜底。每次唤醒都从同一 SQLite 只读快照读取 response 状态与 cursor 后事件，因此通知只降低延迟，不承担正确性；进程崩溃后的连续性由客户端同 key 重试与启动恢复负责。流没有绝对写 deadline：等待 Human 时 socket 可以长期空闲；只有实际写一个或一组已持久化 SSE frame 并 Flush 时才设置逐次 deadline，成功后立即清零。默认 `10s`，公共 `gateway.Config.StreamWriteTimeout` 以及 `human gateway/local --stream-write-timeout` 可调；底层 `ResponseWriter` 不支持 deadline 时保持标准 `net/http` 行为。这样 caller 单向半开、保持 TCP 却停止读取时不会永久占住 handler。关机时 runtime context 会同时停止 session consumer 与 HTTP waiter；gateway 进程先停止接收 HTTP，再等待所有 completion consumer 退出，最后关闭 SQLite。尚未完成的持久请求留给下次启动恢复；旧 socket 若能在短时独立读取中确认 durable decision 就精确写出，否则中止 transport，绝不由 `net/http` 合成空 `200`。自动故障注入已证明显式同 key 的 caller 可连续 5 次在不同 SSE 边界断开，第 6 次精确恢复；受识别的 Codex Responses turn 也在无显式 key 时通过派生 key 通过同一测试。OpenCode exact profile 使用上述 harness-session affinity、candidate/活动 task 与 request key；其无工具辅助调用虽降级 Chat，仍保留 exact request retry key。只有未被严格识别的 OpenCode Basic/Chat 和其它未识别无 key 请求，重复 POST 才仍是独立请求。

**"已吐进度后再失败"必须单独测**（[04](04-milestones.md)）：客户端未必自动重试部分响应。**流式是动态 LLM 体验的关键**：人的响应须以 delta 分块吐出，否则 harness 流式 UI 与"首 token 超时"失配；`stream:false` 主要覆盖标题生成等辅助请求，不能用一次性 JSON 冒充长挂保活能力。

## 5. Remote tools/Workspace 的跨 completion 持久状态机

assistant 返回 tool_call 后本次 completion 响应即结束，执行结果只能在**下一次** completion 请求回流。Basic 档依靠 Agent 带来的完整上下文，每次请求独立排队；只有 caller 显式提供稳定身份，或 exact adapter 能按上述版本化规则解析稳定 task/request key 时，gateway 才能按下述状态机提供粘连、对账和精确幂等：

```
admitted ─→ leased ─→ awaiting_human ─→ responded
     ↑                                     ├─(最终文本/交付)→ final:completed
     │                                     ├─(澄清问题)→ awaiting_caller ─┐
     │                                     └─(含 tool_calls)→ tools_dispatched
     │                                                                  │
 reconciled ←── awaiting_results ←──────────────────────────────────────┘
 （tool result 或 caller 答复随下一请求回流；对账后回 leased 继续循环）
 任意态 ─→ final:{canceled | rejected | expired | failed}
```

**真实工具流是循环**（read→result→edit→result→exec→final），reconciled 回到原 leased 继续——精确的字段、幂等、对账规则见 [05](05-m0-contract.md) §4。要点：

1. **会话粘连原接单人**：`awaiting_results` 的下一次请求直接回原接单人 lease，不重进 FIFO——否则换人接就丢现场。
2. **只有成功 tool result 才推进镜像 baseline**：交付前先持久绑定 exact tool-call ID/digest 与已审内容；result 只确认这份 delivery intent。若人在等待期间又保存了更新草稿，baseline 只推进到已发送版本，更新 diff 仍保留在 Review，而不是被迟到 result 误清空。
3. **多文件部分失败保留未确认 diff**，针对性重试，不整批回滚也不静默丢弃。
4. **请求幂等**：先查 `(caller_id, idempotency_key)`；同 key 同请求摘要复用原任务并重放/续接持久响应事件日志，同 key 不同摘要返 409。若请求已落库却在 HTTP response boundary 前内部失败，只完成该 request 并让原 task 保持 `admitted/reconciled`；换新 key 只允许在无 active request 时重试完全相同的 canonical 摘要，并发新 key 由 task 级唯一约束裁成至多一个，不能借重试改写本轮输入。
5. **执行幂等**：caller shim 按 `(caller_id, task_id, tool_call_id)` 在执行前落 `pending`。文件型 ledger 在打开 SQLite 和恢复 `pending` 前先取得持有至 `Close` 的跨进程独占 owner lock；第二个 shim fail-fast，不能把首进程的 live execution 误判为崩溃遗留。正常完成后重复 ID 只重放原 result，绝不再次执行；若进程在副作用与 result 提交之间崩溃，重启会把遗留 `pending` 原子收口为可重放的 `execution_outcome_indeterminate` 终态。若当前进程的 completed 与 indeterminate 两次提交都失败，Executor 只记住“工具已结束”的 key/digest 并在同 ID 重试时继续收口，绝不保存可重跑 input 或再次执行副作用。最终都要求先核对工作区再换新 `tool_call_id`，不会猜测副作用未发生。
6. **任何响应可见前先落 `admitted` 与请求模式**：流式在 assignment 可见前再持久化并 flush 200；聚合在终态原子落完整 HTTP decision，杜绝“响应已发、任务或 body 未落库”的崩溃窗口。
7. **漂移恢复（T-17）属最小闭环**：edit 失败 → 重新 read 回填 → 重做，早期里程碑就要有。

## 6. 心跳、超时与故障恢复

harness/中间层有 idle timeout（常 30–120s），人要几分钟到几小时。`stream:true` 的保活靠“流已开始且仍在产出”：Anthropic `event: ping` 空事件；OpenAI `:` 注释行。挂起期按间隔（默认 15s）发送。`stream:false` 在终态前没有可写的协议 body，也不会把 heartbeat 混进 JSON；它只能依赖 HTTP 层/中间层自身 timeout。OpenCode Chat 的真实 10m/2h 结果作为已归档的心跳兼容证据，当前不再扩大持续时长。

- **进度 delta 兼作强心跳**（比空 ping 更像模型在产出）；
- **caller 恢复需要稳定 key**：同 key + 同摘要才能续接持久 cursor 并精确重放；默认由 caller 显式提供，严格识别的 Codex Responses turn 与 OpenCode 1.17.18 Workspace turn 分别使用上述版本化派生策略；其它无 key 请求是新 completion；
- **worker 恢复靠持久 outbox**：初始连接被拒或运行中断网时有界退避重连，已提交事件在 ACK 前保留；客户端 ping 会将“TCP 尚在但对端不读”的半开连接拉回重连路径；401/403 是终态凭据错误，不做无限重试；
- **gateway 恢复靠原 SQLite 与原身份**：重启后重建未完成 session，worker 复用原 outbox、caller 复用同 key，才是同一逻辑请求的恢复；
- **`max_pending` 是不可跨越的业务边界**：不稳定网络只能在剩余窗口内恢复。超过后原请求按阶段 B 的 `expired` 收尾（§8），迟到 worker 事件进 rejected inbox，不会“复活”旧请求。

仓库内的通用三方故障 E2E 已在 caller 流中断、worker 离线保留 final、gateway + SQLite 进程重启同时发生时，按最难的顺序先恢复 gateway/caller 重试、最后恢复 worker；5 个并发同 key caller 最终收到逐字节相同的单次 final，没有重复 assignment 或重复执行。Workspace 正式故障测试进一步把原生 OpenCode `edit` 置于三方重叠离线窗口：delivery intent 已落盘，edit 只进入 worker 磁盘 outbox，gateway/SQLite 和 worker 都重启；恢复后的并发 caller 得到逐字节相同、各含同一 call ID 一次的响应，数据库只有一份 edit receipt/step/applied，result continuation 只确认 v1 baseline，并保留期间保存的 v2 Human diff。这证明项目自有 request/event/outbox 与 mirror intent 的恢复不变量，不证明真实 OpenCode CLI 只执行一次。Codex 0.144.4 黑盒补充证明了 500/响应前 TCP 断开的真实重试策略；0.145.0 公共栈 gate 补充证明了 Responses RemoteTools、partial SSE 恢复和正常网络下的 custom `apply_patch` Workspace。仍未证明 Codex Workspace 的进程/网络故障恢复。

## 7. 会话与工作区标识：正确性用稳定 key，指纹只做 UI

- **正确性绑稳定标识**：鉴权所得 `caller_id` 命名空间 + `workspace_key`（路径/能力/baseline）+ `task_id`（循环/lease/幂等），由接入契约提供——**误合并会串工作区，不只是显示问题**。三概念的正交拆分见 [05](05-m0-contract.md) §1。
- **自有 shim 不自报 caller 命名空间**：它从受信启动配置注入 `X-Human-Caller-Id`，gateway 必须在准入落库前确认该值等于 token principal；这把 caller 侧执行账本与 gateway 侧任务命名空间绑在同一身份上。
- **OpenCode exact 身份以原生 session 为锚**：`X-Session-Id + model/system + 截止最新 user 消息的 canonical 历史` 只生成 candidate；同一 caller/workspace/exact harness/session 的唯一非终态 task 优先，故 clarification/followup/tool/result 都留在该 task，terminal 后下一顶层 user 才使用新 candidate。request key 则直接绑定 harness session 与两种请求摘要，不含 task_id。这是版本化 adapter 契约，不是 UI 历史指纹猜测。
- **历史前缀指纹只用于 `ui_conversation_group`（UI 聚合）**：把无 id 的请求在 TUI 呈现为连续对话是锦上添花；指纹断裂 → 降级新卡片，人可手动合并，**不影响正确性**。

## 8. 错误语义：准入与聚合错误是真 HTTP，流式错误是流内

| 情形 | 阶段 | OpenAI | Anthropic |
|---|---|---|---|
| 无人在线/队列满/限流（机器可判断） | A（200 前） | `503`+`Retry-After` | `529 overloaded_error` |
| raw body 或 canonical worker assignment 超过统一 `8 MiB` wire budget | A | `413 request_too_large` | `413 request_too_large` |
| 格式无法转换 | A | `400` | `400` |
| 同一 idempotency key 对应不同请求摘要 | A | `409` | `409` |
| 同一 key/摘要的精确重放正文已过 grace | A | `410 replay_payload_expired` | `410 replay_payload_expired` |
| key 无效/超限 | A | `401`/`429` | `401`/`429` |
| **人工拒单**/挂起超时/中途不可用，`stream:true` | B1（200 后） | 流内 error chunk 或断流 | 流内 `error` event |
| **人工拒单**，`stream:false` | B2 终态 decision | `409` + JSON error | `409` + JSON error |
| 挂起超时/中途不可用/失败，`stream:false` | B2 终态 decision | `504`/`503`/`500` + JSON error | `504`/`529`/`500` + JSON error |

**人工拒单必在 assignment 可见之后**：流式先写/flush 200 再推 TUI，所以只能流内拒绝；聚合先持久化 admitted + mode 再推 TUI，终态前 HTTP decision 仍未选择，所以可以原子提交 409 + 完整错误 body。两者都不把人工等待塞回阶段 A；阶段 A 只做机器可判断的在线/容量/限流检查。对外错误必须是该方言里真实模型 API 可解析的错误对象；准入、部分流失败与聚合终态失败分别测试。

## 9. 审计与数据保留：默认安全

先区分两种用途：**正确性重放数据不是审计数据**。活跃请求为了崩溃恢复、200/SSE 精确重放和 worker 对账，必须暂存 canonical 请求、response wire/step/applied 与相关工具结果；它不受 `--audit-payload` 开关控制。请求完成后默认再保留 24 小时（`--replay-payload-grace` 可配），启动时及此后每小时清理：删除 canonical、response body/event wire 与已安全终结的 tool-result 正文，只留下 caller/key、请求摘要、终态、时间戳以及 worker event ID/digest receipt 等无正文幂等元数据。receipt 持续保留，使迟到的 durable-outbox duplicate 仍能 ACK。相同 key + 相同摘要在正文过期后返回 `410 replay_payload_expired`，相同 key + 不同摘要仍返回 `409`，绝不会把旧 key 当作新请求；活跃请求及已 complete 但 final worker receipt 尚未持久化的可恢复请求不会裁剪。任务级 tool ledger 只有在任务已终态且该任务的请求都已成为 tombstone 时才清 result，不能为了 TTL 破坏 at-most-once 正确性。

审计在此之外**分两层，默认不同**：

- **元数据层**（默认开）：`{id, caller_id, workspace_key, task_id, dialect, key_id, pending_ms, gen_ms, error?, 时间}`——问责、限流、运营所需，不含源码/输出正文。**只存 api_key 的 ID/hash，绝不存原始 key**。
- **全量 payload 层**（**默认关闭**）：请求原文、工具结果、响应正文——涉及需求方源码。默认关；**显式开启后默认 `TTL = 7 天`**（可配），非默认长存。
- **训练用途、本机 Agent 数据出境**各自**独立 opt-in**：把审计当"人类专家轨迹数据集"训练资产，需求方须单独同意，不能靠一句服务条款打包。

两条时钟互不替代：audit payload 的 `TTL=7 天` 只约束显式开启的审计副本；correctness replay grace 默认 24 小时，只约束协议恢复/幂等所需的工作副本。

这里的“裁剪”是数据库层的**逻辑删除/置空**，不是取证级安全擦除。SQLite 启动时强制设置并验证 `secure_delete=ON`，以降低当前数据库旧 cell 中残留正文的概率；数据库备份、卷快照、文件系统副本和底层介质仍须由部署方按自己的销毁策略处理。

## 10. 能力：标准工具透传，adapter 只做增强

Basic 档不猜工具语义，也不清空未知 harness 的工具：caller 本次请求明确声明什么，TUI 就只允许人调用什么，随后由真实 Agent 的原生权限与执行链处理。exact Workspace/Remote tools 另外执行 profile 授权分类：mapped/已审 standard 默认可用，privileged 或无法分类的 native/custom/MCP 工具需显式 active-capability opt-in。TUI 可以在**不增加能力**的前提下，为已知且完整匹配的 schema 提供结构化编辑器；需要镜像、自动文件映射、CAS 或执行 ledger 时，才必须理解具体工具语义：

- **Tasks 只是 caller Agent 的计划编辑器**：Web 通过可替换 `PlanProfileResolver` 选择专用面板；官方基础实现要求 exact harness 版本和 behavioral schema 完整匹配。OpenCode 1.17.18 `todowrite`、Codex 0.145.0 `update_plan` 是全量列表，Claude 2.1.217 则按其真实 `TaskCreate`/`TaskUpdate`/`TaskList` 生命周期并从 result 恢复 task ID。三端都已由真实 CLI + Playwright 在正式面板跑完三态 continuation。schema 漂移会关闭专用面板，不会猜测；计划与请求队列分离，也不会生成 Human 自用 Todo 或文本假同步。
- **Command 只是声明工具的快捷编辑器**：Web 通过可替换 `CommandProfileResolver` 选择专用面板；官方基础实现只在 exact harness/version 与完整 behavioral schema 匹配时映射 Claude Code 2.1.217 `Bash`、OpenCode 1.17.18 `bash`、Codex 0.145.0 `exec_command`。三端成功与非零失败恢复均已由真实 CLI + Playwright 从正式面板跑通；schema 漂移时 fail closed，仅保留高级 declared-tool 入口。命令默认沿用 Agent CLI 的当前 workspace，不用可能更宽的路由根覆盖 cwd。面板只生成 tool call，由客户 Agent 执行，Human Web/worker 绝不执行命令。
- **高级原生工具入口**：结构化编辑器不适用时，人仍可按 `t` 输入 `<tool-name> <JSON object>`；一行一个调用，可在同一响应返回多个调用。工具不在本次声明中就拒绝，schema 形状也不授予请求之外的能力。
- **每个增强 harness 一个显式 adapter / capability profile**（版本化），声明 path style、result codec、文件工具的匹配语义、native tool 的授权分类，以及命令的 cwd/超时/审批和错误格式；TUI 的镜像、文件同步和专用终端入口才映射到这些语义。它不充当本次请求全部工具的 allowlist；`opencode@1.17.18` 只映射已捕获的 `read/write/edit/bash`，不虚构 search/delete/rename。其它 caller-declared tools 仍可走通用入口，但 privileged 和未分类工具要求显式 active-capability opt-in，不能因 caller schema 出现一个名字就默认获得 command/network/spawn 权限。
- read/搜索/删除/重命名和镜像首填都必须由真实能力闭合。OpenCode 1.17.18 的普通 lossy `read` 不能被当作 byte-exact 首填；`:pull path` 是单独的精确 bootstrap：生成已声明的原生 `bash` tool call，运行 `opencode debug file read --pure`，经 Agent 权限确认后以 base64 回传并按持久 intent hydrate 单个相对路径文件。空内容是合法文件，前导 `-` 路径用 `./` 消歧；整个回流请求受统一 `8 MiB` wire budget 约束，过大时明确失败，不宣称 `16 MiB` 文件支持。
- **shim 是可选增强**：它可额外暴露 `human_read_file`、`human_search`、`human_edit_file`、`human_write_file` 与可选命令工具，并通过 `/internal/v1/tools/execute` 提供 ledger 与 caller-side CAS——见 [05](05-m0-contract.md) §6。普通 Agent 自带的 read/search 不需要经过 shim。

## 11. 安全：路径围栏是词法纵深防御，执行边界在 caller 侧

**最锋利的事实**：所有档的工具都在需求方机器执行、我们不执行任何东西——Basic 的真正执行边界是需求方 Agent 的权限系统；Remote tools/Workspace 可再叠加 caller helper。接单人是“不受信任的 tool_call 来源”（与模型同构，但为思考型对手）。三责任：不加剧暴露、纵深防御、诚实划界。

- **路径语义由 profile 决定**：`human-shim@1` 把真实根 `R` 映射为 `/workspace`，可隐去 home/用户名；`opencode@1.17.18` 必须保留 caller 绝对 `filePath/workdir`，因此不作路径隐私承诺。自由文本内路径改写也不是真边界。基线/仓库只服务 git-for-context，传输仍走 tool_call（[03](03-tui.md) §5）。
- **越界防护分两层**（明确定性）：网关对路径型参数做 `path.Clean` 规整、拒 `..` 爬出 `V`、词法拒写 `.git/`（尤其 hooks=RCE）、大小写不敏感盘按小写比对、敏感路径（`.env`/`.ssh`/密钥）红标。这一层只是纵深防御；caller shim 才是真实文件系统强制边界，执行 `realpath`/symlink 围栏，并在解析后的真实相对路径上再次拒绝任一 `.git` 段。因此 workspace 内 `alias -> .git` 不能通过 write/edit/delete 或 rename 两端写入仓库元数据。任意本地进程交换 symlink 的跨进程竞态仍不被扩大成 `openat` 级承诺。
- **shell 无法围栏**（诚实）：`command` 是自由字符串，`cat ../../etc/shadow`、`$HOME/.aws` 无法可靠解析——正则扫危险模式 + TUI 高亮 + 依赖 harness 权限弹窗兜底。
- **active caller-tool 能力默认关闭**：Basic 以本次请求的明确 tool declaration 为授权边界；Remote tools/Workspace 的 command/network/sub-agent 和未分类工具还须按 workspace/task 显式 opt-in，并继续经过客户侧权限确认。当前协议复用 `X-Human-Allow-Exec` 承载这项 active-capability opt-in，名称比实际覆盖面窄，不能解读为只保护字面名为 `bash` 的工具。
- **披露与发送意图**：默认让需求方**知道背后是人**——"伪装"只表示协议兼容，不表示隐瞒身份。默认 Workspace 走 T-08 人工 preview/confirm；显式开启 auto-send 表示“保存干净改动即发送意图”。change-level 安全警告/冲突、Review 跳过项或 adapter 无法交付的部分批次仍阻断自动发送；单纯的无-CAS 警告会展示，但不会替代客户 Agent 的权限闸。使用本机 Agent 辅助还须单独取得数据出境同意。

## 12. 技术栈与存储抽象

- **CLI**：`spf13/cobra`——`human local` 单进程运行 Service/SQLite/workerkit/Web；`human gateway` 与 `human worker` 是独立远程兼容部署；`human shim` 是可选 caller 边界。
- **配置**：`spf13/viper`——file（toml/yaml）+ 环境变量 + flag 层叠加载，与 cobra 打通（`BindPFlag`）；配置文件即 [03](03-tui.md) §8 的 toml。
- **传输**：`net/http` + 标准 SSE；远程 worker transport 使用 WebSocket。`callerhttp.BuiltinRoutes()` 明确绑定 `/v1/chat/completions`、`/v1/responses`、`/v1/messages`，不靠请求启发式选择 codec。
- **公共库**：`llm.Service` 只拥有正确性 core 和交给它的 Store/Protector Resource；`callerhttp`、`workerws`、listener 与 TLS 均由宿主拥有。`AdmissionPolicy`、`WorkerRouter`、`ToolAuthorizer`、Store、Codec、ID/Seed/Clock、Observer 都是 ports。`local.Open` 是官方基础实现，不是不可替换的框架边界；它还通过 `WebStateStore` 和 `Service()` 暴露组合接缝。

**存储边界**：公共 core 只依赖 `llm.Store`，并由
`humantest.TestLLMStore` 验证第三方实现；官方基础 adapter 是 `llm/sqlite`。可替换表示
接口与原子性合同公开，不表示官方 SQLite 支持多实例共享。

- [`examples/custom-framework`](../examples/custom-framework/README.md) 提供不引用
  `internal/` 的自有 Store 实现与组合证据；第三方 PostgreSQL/远程 durable service 可以
  实现同一接口，但必须兑现 transaction/CAS、commit-unknown、byte ownership 与 scan
  语义，而不是只让方法签名编译。
- 官方只提供 SQLite adapter；它是单 owner 的桌面实现，不是共享文件集群。
- request/task、response event、tool ledger 与 worker receipt 由同一 Store 原子边界管理，
  不另设第二套状态机。
