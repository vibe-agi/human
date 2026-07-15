# 01 · 目标与产品定义

## 1. 最终目标（North Star）

**让客户侧真实 Agent 像调用 LLM 一样调用一位人类专家。**

人扮演动态模型：客户侧 Agent / harness 通过 Anthropic Messages、OpenAI Chat Completions 或 OpenAI Responses 发来上下文；人持续给出下一步判断；客户侧 Agent 在自己的真实环境里执行 read/edit/exec，再把结果作为下一次 completion 回传。系统不另建一套 Agent 任务执行面。

核心实现位于 `internal/completion/`。三方言 codec、caller shim、持久循环、工作区镜像和最小 TUI 已有仓库内测试；具体外部 harness 的兼容性与 10 分钟/2 小时长挂仍待 M0 实测。远景是专家网络与市场；需求方**单独 opt-in** 后，completion 往返可沉淀为人类专家轨迹数据。

## 2. 决策记录：为什么“人当模型”

客户侧本来就有真实 Agent。若 Human Agent 只替换它调用的模型，下列能力可直接复用：

| 能力 | 另建任务执行面的代价 | 复用客户侧 Agent |
|---|---|---|
| 文件就地变化 | 自建代码传输、应用与基线对齐 | harness 的 edit 在客户机器执行 |
| 环境命令（adb） | 自建命令协议与授权模型 | harness 的 exec 与权限确认 |
| 取消与重试 | 自建中断、恢复和回滚协议 | 复用 harness 行为；gateway 只管理跨回合任务状态 |
| 两侧一致性 | 维护两套工作现场并持续同步 | **缩小为逐 tool-call 确认**；唯一真相是客户侧工作树，失败显式化 |

另有三个工程理由：维护面更小（但仍需跨回合任务状态，并非无状态）；**Chat 档**只需 base_url + token（Remote tools / Workspace 档需要更多，见 [02](02-gateway.md) §1）；最直接验证“人能否提供可用的动态模型体验”。

代价（如实）：人需在请求生命周期内持续响应；上下文与工具面由客户侧 harness 支配；assistant 消息不能直接回传图片。若目标 harness 无法稳定长挂，产品就只承诺短交互排障，不扩展为另一套执行协议。

## 3. 角色与体验之锚

- **需求方**：任何支持自定义模型端点的 harness 的用户（或无人值守系统）。体验之锚：**与用一个很慢但很聪明的模型无差别**——工具照常执行、界面照常流式、错误照常重试。
- **接单人**：TUI 前的人类专家，**在自己的 IDE 里干活**（Claude Code/cursor/手写皆可）。体验之锚：**一个沟通与交付的信使台**——读对方递来的需求、聊清楚，去自己 IDE 干活，回来核对改动、放行交付。中间几十个 tool-call 回合不暴露给人；人面对任务级节奏（聊需求→干活→交付），不逐回合扮演模型。沟通就是对话（对方发起、人回复、可反问澄清），无主动发起（[03](03-tui.md) §1–2）。

三条刚性约束：

1. **人的响应以分钟计，研发型任务可能需要长挂数小时** → 实现已提供 SSE 心跳、流式进度与可配置 `max_pending`。200 前的机器准入失败用真实 `503/529`；200 后的人工拒单/超时只能用流内错误或断流。**真实外部 harness 的 10m/2h 上限与部分响应重试行为尚未实测，仍是方向的生死门（[04](04-milestones.md)）。**
2. **人不像模型那么听话** → 一切离开本机的内容（文本、文件改动、命令）发送前人眼核对；系统做翻译与暂存，不代替确认。
3. **接单人是"不受信任的 tool_call 来源"**（与模型同构，但为思考型对手）→ 网关做**词法纵深防御**（路径规整/越界拒转）；但真正的执行闸在需求方 harness 权限系统 + caller shim（realpath/symlink/执行限制），不在网关。命令能力默认关闭、按 task 开（见 [02](02-gateway.md) §11）。

## 3.1 人不手搓协议 · 文件隐式 · 可使用本机 Agent

三条定义交互模型的决定（细节见 [03](03-tui.md) §1–2、[02](02-gateway.md) §11）：

- **人不构造 tool_call**：协议底层的"文本 + tool_calls"是管道，不暴露。人只做聊天、命令和交付核对；**文件是隐式的**——人在镜像工作目录里用惯用工具改，当前回 TUI 按 `R` 扫描 diff 并生成文件 tool_call，fsnotify 自动刷新仍是后续体验项。
- **工具栈不暴露**：系统靠**显式 harness adapter**（版本化）声明文件/命令工具的真实语义并映射（schema 启发式只提示、不自动启用写入/执行）；对方的自定义工具是其 agent 自己的事，接单人不碰。未识别 harness 降级纯聊天。
- **可使用本机 Agent**：系统只在按 `R` 时扫描镜像目录，不关心文件由哪种编辑器产生；接单人可让本机 Claude/Codex 协助，但必须亲自核对后才生成 tool call。人是**监督闸非透传**，且必须披露代码可能流向其模型商。

## 4. 产品场景

| # | 场景 | 说明 |
|---|---|---|
| S1 | 远程调试/排障 | 设备与环境在需求方侧（adb、内网库、"只在你环境复现"）——核心场景，人实时看输出连续决策 |
| S2 | 编码任务 | read → edit → bash（测试）循环，人指挥 harness 完成 |
| S3 | 咨询问答 | 纯文本回合，无工具调用 |
| S4 | 结对/评审 | 人阅读 harness 递来的代码上下文，给意见 |
| S5 | 多需求方排队 | 跨会话请求进队列，人逐个处理（同一会话内 harness 天然串行）——排队体验与模型一致 |
| S6 | 本机 Agent 辅助 | 接单人可用自己的 Agent 处理镜像目录，但必须监督核对后交付（§3.1） |
| S7 | 异常 | 准入时无人在线 → overloaded 语义重试；流式中拒单/超时 → 流内失败；对方 Esc/断连 → TUI 中断提示；历史指纹断裂只新建 UI 分组（payload 是否保留取决于 opt-in） |

## 5. 功能点

### humand（G）

| # | 功能点 | 目标里程碑 |
|---|---|---|
| G-01 | OpenAI Chat Completions 方言（含 SSE 流式、tools/tool_calls、finish_reason） | M1 |
| G-02 | Anthropic Messages 方言（含 SSE event 流、tool_use/tool_result blocks、stop_reason） | M1 |
| G-03 | 内部规范格式（canonical）与双向转换矩阵 | M1 |
| G-04 | SSE 心跳保活（Anthropic `ping` event / OpenAI SSE 注释行）+ 长挂（小时级）+ 进度流式（挂起期间人可先吐进度 delta）+ 可配置最大挂起时长 | M0 |
| G-05 | 会话识别（仅 UI 聚合）：历史前缀指纹把无 thread_id 的请求在 TUI 呈现为连续对话；正确性边界用 G-17 稳定标识，不用指纹 | M1 |
| G-06 | 请求队列：跨会话 FIFO（人可挑单）、队列状态推送 TUI | M1 |
| G-07 | api_key 鉴权（`humand token` 签发/吊销，key=需求方身份） | M1 |
| G-08 | 审计分层默认安全：元数据层默认开（不含正文，key 只存 hash）；全量 payload 默认关闭、显式开启后默认 TTL=7 天；训练用途独立 opt-in | M1 |
| G-09 | 200 前机器准入失败 → 标准 HTTP 错误；200 后人工拒单/超时 → 流内错误或断流；速率限制 per key | M1 |
| G-10 | TUI WebSocket 通道（下发/回传/断线重连/seq 幂等） | M1 |
| G-11 | 虚拟工作区对齐：需求方声明真实根 `R`，跨线路径双向 `R↔/workspace` 改写（隐去 home/用户名）；可选声明 `base_commit`+remote/bundle 供接单人完整镜像 checkout（仅上下文，传输仍走 tool_call） | M1 |
| G-12 | 路径**词法纵深防御**：路径字段规整 + 越界/`.git` 写拒转发、敏感路径/危险 shell 标记——非真边界，realpath/symlink/执行限制由 caller shim 兜（[02](02-gateway.md) §11） | M1 |
| G-13 | 显式 harness **adapter/capability profile**（版本化）声明文件读写删移/命令执行的真实语义；未识别默认降级纯聊天；schema 启发式**只提示不自动启用**写入/执行 | M1 |
| G-14 | 接入分三档（Chat / Remote tools / Workspace），能力与承诺随档递增 | M1 |
| G-15 | 准入/流式两阶段：200 前失败返真 HTTP 错误码，200 后只流内失败（修 200-vs-503 冲突） | M1 |
| G-16 | 跨 completion 持久任务状态机（admitted→…→final）；会话粘连原接单人；baseline 仅成功 tool result 后推进；部分失败保留未确认 diff | M1 |
| G-17 | 稳定标识 `caller_id/workspace_key/task_id` 定正确性边界（能力/镜像/baseline 绑它）；指纹仅 UI 聚合（[05](05-m0-contract.md) §1） | M1 |
| G-18 | Responses API 第三方言 codec/stream 与 golden fixture 已实现；具体 harness 的真实支持仍由 M0 实测裁决 | M0 |
| G-19 | 存储抽象：定义 `Store` 接口并只实现 SQLite driver；postgres/mysql/redis 待多实例需求再实现；CLI 用 `cobra`、配置用 `viper` | M1 |

### human TUI（T）

TUI 是沟通 + 交付的信使台：人只面对对话/交付核对/待办，研发在自己 IDE，不手搓 tool_call、不逐回合应答。

| # | 功能点 | 目标里程碑 |
|---|---|---|
| T-01 | 队列视图：等待中请求（来源 key、会话摘要、新会话标记、挂起时长）+ 系统通知 | M1 |
| T-02 | 接单 / 拒单 | M1 |
| T-03 | 上下文渲染：system 折叠、消息流 markdown、tool 结果高亮与折叠、大上下文懒加载、路径虚拟对齐显示 | M1 |
| T-05 | 对话框：文本回复、澄清反问、**流式进度**（长挂期间先吐进度，不必憋到干完） | M1 |
| T-06 | 环境命令框：输命令 → bash tool_call（对方机器执行，环境绑定型任务用；研发本地命令在自己终端） | M1 |
| T-07 | **工作区镜像与交付 review**：当前实现 caller/workspace 命名空间镜像、`R` 扫描增改删、preview/确认后生成 tool_call；三档自动播种、fsnotify watcher 与 `e`/`o` IDE/终端入口仍未实现 | M1 |
| T-08 | 交付核对与预览：`R` 逐项核对扫描到的改动 + 本次响应（说明+改动+命令）预览，所见即所发 | M1 |
| T-17 | 漂移兜底（**最小闭环必备，前移**）：edit tool_call 失败（对方手改/formatter/脏基线）→ 重新 read 回填镜像 + 提示重做；多文件部分失败保留未确认 diff | M1 |
| T-09 | 待办面板（双重身份：私有清单 + 可选输出进度给对方——对方有 todo 工具则作 tool_call，否则文本流） | M2 |
| T-10 | 图片渲染（对方消息中的截图，kitty/iTerm2 内联，降级占位） | M2 |
| T-11 | 对方断连/中断横幅提示 | M1 |
| T-15 | 预览安全高亮：越界路径红色阻断、敏感路径/危险 shell 黄色警示 | M1 |
| T-12 | 会话历史与审计浏览 | M2 |
| T-13 | 草稿保存与崩溃恢复（挂起请求仍在 gateway，镜像目录在本地，重连续答） | M2 |
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
- **全量 payload 长存 / 训练默认开**：默认安全——元数据审计默认；payload 默认关闭，显式开启后默认 TTL=7 天；训练与本机 Agent 数据出境各自独立 opt-in（G-08、[02](02-gateway.md) §9）

## 7. 术语

| 术语 | 定义 |
|---|---|
| 方言（dialect） | 已实现 codec 的 Anthropic Messages、OpenAI Chat Completions、OpenAI Responses 协议格式；进哪种出哪种。具体外部 harness 是否兼容仍由 M0 实测 |
| 回合请求 | 一次 completion 请求（含全量历史与工具清单），人给出一次响应 |
| workspace_key | 路径根 R / 能力档 / baseline / 镜像目录名的载体（**正确性绑它**，[05](05-m0-contract.md) §1） |
| task_id | 一次工具循环 / lease / 幂等的载体（**正确性绑它**） |
| ui_conversation_group | 仅 TUI 显示分组，历史指纹启发式，可错、不影响正确性 |
| 挂起（pending） | 请求已到达、心跳保活中、等人响应的状态 |
| canonical | 方言无关的内部规范格式（messages/blocks/tools） |
| 镜像目录 | 对方工作区在接单人机器上的本地副本；人/agent 在此干活，按 `R` 扫描并核对后转成 tool call 回传 |
| 基线 commit | 镜像 checkout 的起点，与对方工作树对齐一次；仅用于上下文，非传输 |
| 单一写者 | compat 下对方工作树主要由接单人 tool_call 驱动；但一致靠**逐 tool-call 确认**（成功 result 才推进 baseline），非自动对齐 |
