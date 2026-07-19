# 01 · 目标与产品定义

## 1. 最终目标（North Star）

**让客户侧真实 Agent 像调用 LLM 一样调用一位人类专家。**

这是**技术形态**而不是业务分类：人占据 LLM / 模型协议的位置；客户侧 Agent / harness 通过 Anthropic Messages、OpenAI Chat Completions 或 OpenAI Responses 发来上下文，人持续给出下一步判断，客户 Agent 在自己的真实环境里执行工具，再把结果作为下一次 completion 回传。编码、排障、咨询、运维或其它任务都走同一机制，产品不按场景把人的能力裁成“只做某一类工作”。系统也不另建第二套 Agent 执行面。

**Live Workspace 是核心闭环。** 人在本地镜像中保存改动，watcher 做完整 review，再按版本化 harness profile 生成客户 Agent CLI 的原生 `edit/write`；`bash` 与 Tasks 也只调用客户 Agent 已声明的原生工具；result 回流后续接原任务并推进已确认 baseline。自动发送是显式 opt-in，change-level 安全 warning/block 或冲突会停下来等人；adapter 的无-CAS warning 会展示但不替代客户 Agent 的权限闸。客户工作树始终是唯一真相。

核心实现位于 `internal/completion/`。三方言 codec、caller shim、持久循环、Live Workspace 和 TUI 已有仓库内测试；OpenCode 1.17.18 的 OpenAI-compatible Chat 工具闭环与 exact Workspace 的 `空镜像 → :pull 精确字节 → native edit/result → bash/tasks → final → 同 session terminal 后下一 user turn 新 task` 均已用真实 CLI 跑通。Workspace 三方故障正式测试还覆盖原生 edit 在 caller/worker/gateway 重叠离线时进入持久 outbox、gateway/SQLite 与 worker 重启、并发同 key 重放、result continuation 及 save-ahead diff 保留。Codex CLI 0.144.4 黑盒确认了响应前 5 次重试和稳定 turn metadata；0.144.5 又以隔离真实进程跑通 Responses 串行 `exec_command → function_call_output → final` 两轮闭环，并实收 namespace functions 与 hosted `web_search` 声明。它证明 Basic/Chat 的 Responses 文本/函数路径，不等于 Codex Workspace、Tasks 或 Live Workspace profile。10 分钟与 2 小时持续流只作为已归档的心跳兼容证据。当前 M0 门是剩余真实 harness 网络/服务故障恢复；Claude 仍只有本机 codec/schema 测试。远景是专家网络与市场；需求方**单独 opt-in** 后，completion 往返可沉淀为人类专家轨迹数据。

## 2. 决策记录：为什么“人当模型”

客户侧本来就有真实 Agent。若 Human Agent 只替换它调用的模型，下列能力可直接复用：

| 能力 | 另建任务执行面的代价 | 复用客户侧 Agent |
|---|---|---|
| 文件就地变化 | 自建代码传输、应用与基线对齐 | harness 的 edit 在客户机器执行 |
| 环境命令（adb） | 自建命令协议与授权模型 | harness 的 exec 与权限确认 |
| 取消与重试 | 自建中断、恢复和回滚协议 | 复用 harness 行为；gateway 只管理跨回合任务状态 |
| 两侧一致性 | 维护两套工作现场并持续同步 | **缩小为逐 tool-call 确认**；唯一真相是客户侧工作树，失败显式化 |

另有三个工程理由：维护面更小；**Basic 档**只需 base_url + token，即可返回文本或调用本次请求声明的原生工具，每次 completion 独立（Remote tools / Workspace 档才增加稳定身份和跨回合状态，见 [02](02-gateway.md) §1）；最直接验证“人能否提供可用的动态模型体验”。

代价（如实）：人需在请求生命周期内持续响应；上下文与工具面由客户侧 harness 支配；assistant 消息不能直接回传图片；超过 `max_pending` 的旧请求必须明确过期，不能假装无损复活。某个 harness 的时限或工具缺口只限制该 profile 的能力，不反向定义 Human Agent 的业务边界。

## 3. 角色与体验之锚

- **需求方**：任何支持自定义模型端点的 harness 的用户（或无人值守系统）。体验之锚：**与用一个很慢但很聪明的模型无差别**——工具照常执行、界面照常流式、错误照常重试。
- **接单人**：TUI 前的人类专家，**在自己的 IDE 里干活**（Claude Code/Cursor/手写皆可）。体验之锚：**一个像聊天窗口的模型工作台**——直接回复当前 completion，也可在 Live Workspace 保存改动、编辑客户 Agent 的计划或让它执行命令；真实文件和命令仍由客户 Agent 处理。沟通就是对话（对方发起、人回复、可反问澄清），无主动发起（[03](03-tui.md) §1–2）。

三条刚性约束：

1. **人的响应以分钟计，任务也可能持续数小时** → `stream:true` 已提供 SSE 心跳、流式进度与可配置 `max_pending`；机器准入失败用真实 `503/529`，流式 200 后的人工拒单/超时只能用流内错误。`stream:false` 只在终态原子返回 JSON，没有应用层 heartbeat。OpenCode Chat 的真实 10m/2h 只作为历史证据；当前门禁是 caller/worker/gateway 断网、服务重启、三方重叠故障及 `max_pending` 边界，**不再增加长挂时长**（[04](04-milestones.md)）。
2. **人不像模型那么听话** → 文本、命令和默认文件交付发送前人眼核对；显式 auto-send 时，保存干净文件本身就是人的发送意图，安全 warning/block 或冲突仍强制停下。系统做翻译与暂存，不替人扩大权限。
3. **接单人是“不受信任的 tool_call 来源”**（与模型同构，但为思考型对手）→ Basic 档只能调用 caller 在本次请求中明确声明的工具，真正的执行闸在需求方 Agent 的权限系统。Remote tools/Workspace 的 exact profile 还把工具分为 mapped/已审 standard 与 privileged/unclassified；后者须显式 active-capability opt-in。网关另做词法纵深防御，可选 caller shim 提供 realpath/symlink/执行限制，并在解析符号链接后的真实路径上强制拒绝 `.git` 写（见 [02](02-gateway.md) §11）。

## 3.1 人不手搓协议 · 结构化常用操作 · 可使用本机 Agent

三条定义交互模型的决定（细节见 [03](03-tui.md) §1–2、[02](02-gateway.md) §11）：

- **常用操作结构化**：TUI 常驻 `CHAT / REPLY / TASKS / COMMAND`。Tasks 只编辑**客户 Agent 自己的计划**，与接单 Inbox 无关；Command 只生成 caller 已声明且已授权的 `bash` tool call，绝不在专家机器执行。其它已声明工具仍可按 `t` 用 `<tool-name> <JSON object>` 高级入口调用；未声明工具一律不能调用，exact 增强档中的 privileged/unclassified 工具还必须先取得 active-capability opt-in。
- **Basic 不需要 adapter**：原生工具由客户侧 Agent 自己执行并在下一次 completion 回传 result；未识别 harness 仍保留请求声明的工具，不再清空成纯聊天。adapter 是已知 harness 的版本化**语义 mapper + 授权分类**：为登记工具补稳定身份、路径/result codec 与镜像映射，不是拦截其它 caller-declared tools 的全局 allowlist；exact Workspace/Remote tools 的 mapped/已审 standard 工具默认可发，privileged 或未分类 custom/MCP 工具需显式 opt-in 后仍可走通用入口。强 ledger/CAS 只来自 shim/等价边界。
- **可使用本机 Agent**：系统通过 `fsnotify` 监视镜像并在保存后自动刷新完整 review，也保留 `R`/`Ctrl+P` 手动入口；接单人可让本机 Claude/Codex 协助，但必须对警告和冲突亲自裁决。人是**监督闸非透传**，且必须披露代码可能流向其模型商。

## 4. 产品场景（示例，不是业务边界）

| # | 场景 | 说明 |
|---|---|---|
| S1 | 远程调试/排障 | 设备与环境在需求方侧（adb、内网库、"只在你环境复现"），人实时看输出连续决策 |
| S2 | 编码任务 | Live Workspace + read → edit/write → bash（测试）循环，人和 harness 持续协作 |
| S3 | 咨询问答 | 纯文本回合，无工具调用 |
| S4 | 结对/评审 | 人阅读 harness 递来的代码上下文，给意见 |
| S5 | 多需求方排队 | 跨会话请求进队列，人逐个处理（同一会话内 harness 天然串行）——排队体验与模型一致 |
| S6 | 本机 Agent 辅助 | 接单人可用自己的 Agent 处理镜像目录，但必须监督核对后交付（§3.1） |
| S7 | 异常 | 准入时无人在线 → overloaded 语义重试；流式中拒单/超时 → 流内失败；聚合终态失败 → 真 HTTP status + JSON error；对方 Esc/断连不撤销已持久 admission；历史指纹断裂只新建 UI 分组（审计 payload 是否保留取决于 opt-in；活跃恢复副本独立管理） |

## 5. 功能点

### Human gateway（G；`human local` 内嵌，`human gateway` 独立）

| # | 功能点 | 目标里程碑 |
|---|---|---|
| G-01 | OpenAI Chat Completions 方言（含 SSE 流式、tools/tool_calls、finish_reason） | M1 |
| G-02 | Anthropic Messages 方言（含 SSE event 流、tool_use/tool_result blocks、stop_reason） | M1 |
| G-03 | 内部规范格式（canonical）与双向转换矩阵 | M1 |
| G-04 | SSE 心跳保活（Anthropic `ping` event / OpenAI SSE 注释行）+ 进度流式 + 可配置 `max_pending`；10m/2h 只保留历史证据，当前门禁是故障恢复 | M0 |
| G-05 | 会话识别（仅 UI 聚合）：历史前缀指纹把无 thread_id 的请求在 TUI 呈现为连续对话；正确性边界用 G-17 稳定标识，不用指纹 | M1 |
| G-06 | 请求队列：跨会话 FIFO（人可挑单）、队列状态推送 TUI | M1 |
| G-07 | api_key 鉴权（`human gateway token` 签发/吊销，key=需求方身份）+ 可嵌入 request authenticator（Cookie/JWT/mTLS/上游 principal） | M1 |
| G-08 | 数据留存默认安全：元数据审计默认开（不含正文，key 只存 hash）；审计 payload 默认关闭、显式开启后默认 TTL=7 天；正确性副本完成后默认 24h grace 再裁成幂等 tombstone；训练用途独立 opt-in | M1 |
| G-09 | 机器准入失败 → 标准 HTTP 错误；流式 200 后人工拒单/超时 → 流内错误；聚合请求终态原子提交 HTTP status + JSON body；速率限制 per key | M1 |
| G-10 | TUI WebSocket 通道（下发/回传/断线重连/seq 幂等） | M1 |
| G-11 | profile 显式声明路径语义：`human-shim@1` 使用 `R↔/workspace` 虚拟路径；`opencode@1.17.18` 使用 caller 的绝对根 `R` 与原生绝对路径。可选 clone/bundle 只供上下文，传输仍走 tool_call | M1 |
| G-12 | 网关路径**词法纵深防御**：路径字段规整 + 越界/`.git` 写拒转发、敏感路径/危险 shell 标记；caller shim 才是真实文件系统强制边界，使用 realpath/symlink 围栏，并在解析后真实路径上再次拒绝 `.git` 写（含指向 `.git` 的内部别名）（[02](02-gateway.md) §11） | M1 |
| G-13 | Basic 透传本次请求明确声明的原生 tools；可选 harness **adapter/capability profile**（版本化）映射已知文件/命令语义并提供稳定身份与镜像能力，但不作为全局工具 allowlist。exact 增强档对 mapped/已审 standard 与 privileged/unclassified 分级，后者必须显式 opt-in；强 ledger/CAS 只由 shim/等价边界提供，schema 启发式不授予额外能力 | M1 |
| G-14 | 接入能力分 Basic / Workspace / Remote tools：Workspace 是核心原生 Agent 闭环，Remote tools 是可选强化边界；不是按业务价值递增的阶段 | M1 |
| G-15 | 准入/响应模式边界：流式先持久化并 flush 200 再派发；聚合先持久化 admitted/mode，终态原子决定 status/body（修 200-vs-503 冲突） | M1 |
| G-16 | 跨 completion 持久任务状态机（admitted→…→final）；会话粘连原接单人；baseline 仅成功 tool result 后推进；部分失败保留未确认 diff | M1 |
| G-17 | 稳定标识 `caller_id/workspace_key/task_id` 定正确性边界（能力/镜像/baseline 绑它）；指纹仅 UI 聚合（[05](05-m0-contract.md) §1） | M1 |
| G-18 | Responses API stream/aggregate codec、严格控制字段、serial/namespace/hosted/reasoning 边界与 golden fixture 已实现；Codex 0.144.5 Basic 函数闭环已真实通过，其 Workspace/故障恢复仍由 M0 实测裁决 | M0 |
| G-19 | 公共持久边界已按 `agent.Store`、`llm.Store`、`workspace.Store` 三个领域合同切分；官方生产 adapter 当前为 SQLite，宿主可实现自有 Store，postgres/mysql/redis 官方实现待多实例需求再设计 | M1 |
| G-20 | 故障矩阵：caller 重试、worker 抖动/半开、gateway/SQLite 重启及三方重叠离线；同一逻辑事件零重复、零静默丢失、无队头毒丸 | M0 |
| G-21 | 公共 Go API：`gateway` 提供 handler/恢复/认证接缝，`worker` 提供 Bubble Tea model，`local` 单进程组合 loopback gateway + SQLite + TUI；CLI 为薄装配 | M1 |

### human TUI（T）

TUI 是聊天优先的单屏工作台：`CHAT / REPLY / TASKS / COMMAND` 常驻；请求队列只以轻量 Inbox 提示存在，研发仍可在自己的 IDE 完成。

| # | 功能点 | 目标里程碑 |
|---|---|---|
| T-01 | 轻量 Inbox：在 Tasks 区提示等待请求、接单/拒单与来源摘要；不再维护独立请求队列视图，也不与 Agent Tasks 混用 | M1 |
| T-02 | 接单 / 拒单 | M1 |
| T-03 | 上下文渲染：system 折叠、消息流 markdown、tool 结果高亮与折叠、大上下文懒加载、按 profile 显示虚拟或绝对路径 | M1 |
| T-05 | Reply：直接输入，`Enter` 发送 progress、`Shift+Enter`/`Ctrl+J` 换行、`Ctrl+R` handoff、`Ctrl+D` final。只有没有已接入请求或正等待 tool result 时才禁用回复 | M1 |
| T-06 | Command：仅在 caller 已声明兼容 `bash` 或 Remote adapter 授权 exec 时生成 tool call，由客户 Agent 执行，绝不本地执行；`t` 保留为其它声明工具的 JSON 高级入口 | M1 |
| T-07 | **Live Workspace**：caller/workspace 命名空间镜像、递归 `fsnotify` + debounce、完整 review、preview/confirm；可选 auto-send 只自动发送无警告/无冲突改动，再由 exact profile 生成原生 tool call | M1 |
| T-08 | 交付核对与预览：默认 `R`/`Ctrl+P` 逐项 preview/confirm；显式 auto-send 仅绕过 change-level allow 项的人工确认，warning/block 仍暂停 | M1 |
| T-17 | 漂移兜底（**最小闭环必备，前移**）：edit tool_call 失败（对方手改/formatter/脏基线）→ 重新 read 回填镜像 + 提示重做；多文件部分失败保留未确认 diff | M1 |
| T-09 | Tasks：从 caller 上下文恢复并编辑客户 Agent 的全量计划，再通过其本次已声明的 `todowrite` / `TodoWrite` / `update_plan` 同步；无兼容工具或 schema 漂移时只读禁用，不生成 Human 自用 Todo 或文本假同步 | M1 |
| T-10 | 图片渲染（对方消息中的截图，kitty/iTerm2 内联，降级占位） | M2 |
| T-11 | 对方断连/中断横幅提示 | M1 |
| T-15 | 预览安全高亮：越界路径红色阻断、敏感路径/危险 shell 黄色警示 | M1 |
| T-12 | 会话历史与审计浏览 | M2 |
| T-13 | worker-local SQLite 持久化 Reply/Command/Tasks/Advanced tool-call 草稿及最多 32 个未完成 continuation；重启恢复，坏记录隔离，写失败有界退避且保留内存副本 | M1 |
| T-14 | onboarding 向导 + 配置（gateway 地址、token、镜像根、IDE、通知） | M1 |

### 跨组件（X）

| # | 功能点 | 目标里程碑 |
|---|---|---|
| X-01 | 审计导出 JSONL（人类轨迹数据集友好格式） | M2 |
| X-02 | 内部 WS 协议版本号 | M1 |

## 6. 非目标

- 另建一套任务委托、代码传输或远程执行协议；客户侧 Agent 始终负责工具循环与真实环境执行
- 人回传图片给对方（assistant 消息协议限制；降级：文字描述或 gateway 托管 URL 进 backlog）
- 上下文管理（压缩、截断是 harness 的职责，我们只做管道）
- 多接单人路由、市场、计费
- PTY / 长驻交互进程；终端实时性——命令回合制（结果下一回合返回）
- 全仓预同步（镜像目录惰性按需填充即可）；本机 Agent 代劳的技术性阻断/数据标注（靠核对闸 + 声明治理，见 [02](02-gateway.md) §11）
- gateway 侧 LLM 起草辅助（我们代写草稿）——注意与“接单人自带本机 Agent 干活”（§3.1，默认支持）不同，后者是接单人自己的工具、系统只扫描镜像目录
- **主动发起/反向消息**：completion 协议无回调通道，人只能响应客户侧 Agent 的请求，主动信息搭在下次响应里
- **全量 payload 长存 / 训练默认开**：默认安全——元数据审计默认；审计 payload 默认关闭，显式开启后默认 TTL=7 天；协议正确性副本仅在活跃/恢复期及完成后默认 24h grace 内保存，随后裁成无正文幂等 tombstone；训练与本机 Agent 数据出境各自独立 opt-in（G-08、[02](02-gateway.md) §9）

## 7. 术语

| 术语 | 定义 |
|---|---|
| 方言（dialect） | 已实现 codec 的 Anthropic Messages、OpenAI Chat Completions、OpenAI Responses 协议格式；进哪种出哪种。外部 harness 能力按真实 gate 逐 profile 记账，不从 codec 自动外推 |
| 回合请求 | 一次 completion 请求（含全量历史与工具清单），人给出一次响应 |
| workspace_key | 路径根 R / 能力档 / baseline / 镜像目录名的载体（**正确性绑它**，[05](05-m0-contract.md) §1） |
| task_id | 一次工具循环 / lease / 幂等的载体（**正确性绑它**） |
| ui_conversation_group | 仅 TUI 显示分组，历史指纹启发式，可错、不影响正确性 |
| 挂起（pending） | 请求已到达、心跳保活中、等人响应的状态 |
| canonical | 方言无关的内部规范格式（messages/blocks/tools） |
| Live Workspace / 镜像目录 | 对方工作区在接单人机器上的草稿副本；保存触发 review，人工确认或显式 auto-send 后按 exact profile 转成客户 Agent 原生 tool call，result 回流再确认 baseline |
| 基线 commit | 镜像 checkout 的起点，与对方工作树对齐一次；仅用于上下文，非传输 |
| 单一写者 | 协作窗口内对方工作树主要由接单人 tool_call 驱动；但一致靠**逐 tool-call 确认**（成功 result 才推进 baseline），非自动对齐 |
