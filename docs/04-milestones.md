# 04 · 里程碑与路线

项目只有一条技术主线：客户侧真实 Agent 通过模型协议调用人类专家；人占据 LLM 位置，业务类型不由里程碑限定。里程碑统一为 **M0/M1/M2**。

Live Workspace 是主线的一部分：Human 保存 → exact profile 生成客户 Agent CLI 原生 edit/write/bash/tasks → result 回流 → continuation/reconcile。OpenCode 的 2h 持续流结果已归档，当前**不再追加更长挂门**；本机 M0 可靠性门看 caller、公共 service/SQLite、进程内 workerkit 与 Web 人侧，远程 gateway/worker 的多进程故障证据作为独立扩展边界记录。

本轮 RC 的可交付目标固定为 **Claude Code 2.1.217、OpenCode 1.17.18、Codex 0.145.0 三个 exact profile 下的单机 `human local`**。三端都已通过原生 Live Workspace；Codex 仅在 Responses 请求实际声明精确兼容的 `custom` freeform `apply_patch` grammar 时升级，缺少模型元数据或 schema 漂移时保持 Chat/RemoteTools。其它客户端版本、真实远程 IdP/证书轮换和多实例部署仍属扩展边界；不能把三方言 codec 或内部故障测试外推成任意版本、任意团队部署都已兼容。

## 当前落点：RC 单机路径已有证据，扩展门继续补

| 范围 | 已有证据 | 仍缺证据 |
|---|---|---|
| M0/M1 核心 | 三方言 stream/aggregate codec、持久循环与 adapter/shim；三端真实 CLI 具有共同 final/resume/Command 成功/失败恢复/拒单基线及正式 Tasks 三态；OpenCode 1.17.18 exact Workspace `空镜像 → :pull → edit/result → bash/tasks → final → 同 session terminal 后下一 user turn 新 task` 已真实通过，Claude 2.1.217 完成 mirror create→`Write`、modify→`Edit`，Codex 0.145.0 完成 create/modify→Responses custom `apply_patch`，三者均核对 caller 最终字节 | opt-in auto-send 的真实用户体验；Claude 接近 context window 的自动压缩；Codex Workspace 故障恢复；扩展产品任务 |
| 故障恢复 | 自动故障注入已覆盖 caller 同 key 5 次断流、exact caller-gone/安全接管、public expiry/迟到 NACK、worker 冷启动/5 次抖动/半开检测、gateway + SQLite 重启与三方重叠掉线；Workspace 正式测试再覆盖离线原生 edit、gateway/SQLite 与 worker outbox 重启、并发 replay、result continuation 与 save-ahead diff；真实 OpenCode 三断点各连续掉线 5 次并在第 6 次恢复；Claude/Codex 均通过完整 progress 帧后断 SSE 的真实 CLI 恢复门 | 真实远程 IdP、代理证书轮换、多实例存储与长期演练 |
| M2 产品可靠性 | 单元、集成、端到端、恢复/幂等/安全与 race 测试已覆盖核心路径 | 故障矩阵与运维演练收口、真实用户试点与 20 任务产品门 |

因此以下里程碑是**退出标准**，不能因为代码存在就视为产品门已通过。

## M0 · 可裁决的兼容性与产品实验（1–2 周，go/no-go 硬门）

OpenCode 1.17.18 的 Basic/Chat 与 exact Workspace 均已跑通。Basic 无稳定身份时每次 completion 独立；exact Workspace 以原生 `X-Session-Id` 和截止最新 user 消息的 canonical 历史生成 candidate task，但同一 caller/workspace/exact harness/session 的唯一非终态 task 会覆盖 candidate。clarification、followup、tool call 与 result continuation 因而保持同 task，只有 terminal 后下一顶层 user 才新建 task。request key 直接由 caller/workspace/harness session 与两种请求摘要派生，不含可变 task_id。Codex CLI 0.144.4 的响应前重试/turn metadata 黑盒与 0.145.0 的真实公共栈 Responses RemoteTools/Plan、partial SSE 恢复和 exact `apply_patch` Workspace 门均已通过；尚缺的是 Workspace 故障注入恢复，不是基本文件能力。下表是故障证据的裁决维度；OpenCode 三个真实网络断点已过，真实远程多进程恢复顺序仍待观察：

| 维度 | 验证 |
|---|---|
| 网络 | caller 在 200 前/首帧后/部分进度后断开；worker 冷启动离线、反复抖动、半开连接与 ACK 丢失 |
| 服务 | local service 在 admitted/200/event step/decision/receipt 窗口重启；远程扩展另验 gateway 与 worker/outbox 重启 |
| 重叠故障 | caller、service、人侧同时离线，按不同顺序恢复；远程拓扑另验 caller/worker/gateway；事件不丢、不重、不阻塞后续 session |
| 时间边界 | 故障在 `max_pending` 内恢复；超过则稳定 `expired`，迟到事件进 rejected inbox 而不复活旧请求 |
| 闭环 | 连续 `read → result → edit → result → exec → final` 跨多次 completion 打通 |
| 边界 | 多文件部分失败、重复 POST（幂等）、用户并发修改工作树 |
| 协议 | Chat Completions、Responses API、Anthropic Messages 的真实支持情况 |

**Go/no-go 阈值：**

- 每个可重试故障点至少连续失败 5 次再恢复，Human 内部同一逻辑 request/event 必须**零重复派发、零静默丢失、零毒丸阻塞**；OpenCode 原生工具无 shim ledger，其实际执行次数另行观测，不能由服务端去重代替。超过 `max_pending` 必须明确终止，不强行恢复。
- 自动客户端的同 key 恢复只证明服务端不变量；目标真实 harness 必须另记“是否重试、是否复用稳定身份”。不带显式 key 时仅两个严格版本化例外可派生：Codex Basic/Chat + Responses turn，以及 OpenCode 1.17.18 exact Workspace turn（[05](05-m0-contract.md) §4）；其它请求独立。
- 20 次连续工具闭环必须做到**零重复执行、零静默错误写入**；达不到就先修可靠性，不进入 M1。

**历史持续流证据（范围仅 OpenCode 1.17.18 + OpenAI-compatible Chat + `stream:true`）**：同一请求的 suspend-aware HTTP 有效时长为 `2h0m53.231615s`，期间 483 个心跳，final 后 HTTP 200、OpenCode `reason=stop`，worker outbox 与 rejected inbox 均为空。该结果保留为心跳/代理链的兼容记录，不是当前故障恢复门，也不再由更长 soak 追加证明。

**当前真实网络证据（范围仍仅 OpenCode 1.17.18）**：受控代理在 response headers 前、完整 stream-start 后、Human progress 后三个场景并行运行；每个场景连续切断 5 次，第 6 次以相同 request body、`X-Session-Id` 与 Human idempotency key 恢复，只有一个 assignment，最终 progress/final 不重复，单轮约 70 秒。Makefile 正式门默认 `REAL_NETWORK_DROPS=5`，release 用 `REAL_COUNT=3` 重复整套三场矩阵。它不包含真实 gateway/worker/caller 进程重启或恢复顺序；那部分目前只有内部三方故障矩阵。

**产出：** harness × 故障矩阵（故障点、断开次数、恢复顺序、超时边界、幂等结果、草稿/事件留存、已知限制），再与协议能力矩阵合并。用脱敏的真实请求和工具 schema 补充现有 golden fixtures；合成 fixture 不能替代外部兼容证据。

## M1 · 单一垂直切片（3–6 周）

只选**一个 harness、一个方言、单专家、单工作区**，把一条 Live Workspace 路径走通：

- auth 与准入/流式/聚合响应边界；
- 跨 completion 持久任务状态机；
- TUI（对话、进度、Tasks/Command 与 Live Workspace 保存/交付/结果回流）；
- Basic 只调用本次请求声明的原生 read/edit/exec；Live Workspace 需要 exact adapter，强 ledger/CAS 才需要 shim/等价边界；
- tool result 对账、按 profile 区分原生结果确认与 caller-side CAS，并覆盖漂移恢复；
- 取消、断线重连与重启恢复。

完整 clone/bundle/整仓 bootstrap、图片、训练数据导出和多专家不阻塞这个切片；**Live Workspace 本身不能被裁掉**。OpenCode 的 `:pull` 已提供逐文件 exact bootstrap，不等同整仓同步。三方言 codec 虽已实现，M1 仍只用一个真实 harness/方言完成产品闭环，避免把“代码存在”误当“兼容性成立”。

**退出标准：** 真人通过目标 Agent 完成一个有真实文件 edit/write、命令或 Tasks、结果回流和 continuation 的真实任务；任务可以是编码、排障、运维或其它类型，不把 adb 写成业务门。

## M2 · 可靠性与协议扩展（7–10 周）

- 把第二方言与 Responses API 接入真实 harness，补齐兼容矩阵；
- 有界公平队列与活跃任务粘连；
- 扩展故障注入、fuzz、race 与定期运维演练；
- 已落地数据保留与命令授权策略的故障矩阵、运维验证收口；
- 真实用户试点；
- 扩展 Live Workspace 的可信播种、完整 clone/bundle 与更多 exact harness profile；不把它们混同于已经成立的惰性 mirror/原生工具闭环。

**产品门（尚未执行，属于 M2 扩展而非本轮 RC 阻塞项）：** 20 个内部真实任务中，客户确认成功率 ≥ 80%、未授权命令 = 0、静默文件错误 = 0，并记录每专家小时解决任务数。不要把自动化的 20 次协议闭环与这 20 个真人任务混为一谈。

## 验收 demo（M1 末）

> 前置：终端 A 按 **OpenCode 1.17.18 exact Workspace** 配置 base_url/token；
> `X-Session-Id` 由 OpenCode 自己发送。Human 在 Web 选择对应 repo，双方只约定同一逻辑
> 项目，绝对路径无需一致。需要强 ledger/CAS 时再叠加 Remote tools/shim。

1. 终端 A 的真实 Agent 配置 `base_url=Human gateway, model=human-expert`，用户发起一个需要修改与验证的真实任务。
2. 终端 B 的 Human TUI 接单；人可先回复进度、同步 Agent Tasks 或请求命令。
3. 若 Human mirror 为空，人先在 Command 输入 `:pull path`，经客户 Agent 的 `bash` 权限确认，以 `opencode debug file read --pure` 返回的 exact base64 字节播种所需文件；空文件和前导 `-` 路径均可处理，但整个回流请求必须装进 `8 MiB` wire budget。随后编辑并保存，watcher fresh review，人工确认或显式 auto-send 后生成 Agent 原生 edit/write。
4. 客户 Agent 在真实工作区执行，下一 completion 回传 result；TUI 按稳定 task/tool-call ID 与持久 delivery intent 续接并 reconcile。等待 result 时保存的更新草稿不会被误确认，仍留待下一次交付；随后可运行 bash 或继续编辑。
5. 用户在原 Agent 界面持续看到工具活动和最终结果，并明确知情背后是人。

跑通这条路径，“客户侧真实 Agent 调用动态人类 LLM”的核心假设成立。

## Backlog

| 项 | 说明 |
|---|---|
| 完整 clone/bundle/整仓 bootstrap | 为 Live Workspace 提供可导航的可信初始上下文；OpenCode `:pull` 已解决逐文件精确播种，但不替代整仓能力 |
| LLM 起草辅助 | gateway 代写草稿、人审核；不同于接单人自带工具 |
| 人回传图片 | assistant 协议不直接支持图片，可探索托管 URL 或文字描述 |
| 更多方言 | Gemini、Bedrock、Ollama；canonical 已解耦，可增 adapter |
| 团队/路由/市场 | 多接单人、技能路由和计费；元数据审计已留基础 |
| PTY / 长驻进程 | 只有目标 harness 明确支持时才评估 |

## 仍需外部实测或人工判断

| # | 问题 | 默认 |
|---|---|---|
| Q1 | 伪模型名如何展示 | `human-expert`；多专家可用 `human-expert-<name>` |
| Q2 | 三种 codec 均已实现后，优先投入哪条 | M0 实测后按兼容性与战略价值决定，不从代码完成度推断 |
| Q3 | 目标 harness 能否稳定透传每次请求的身份 | OpenCode 1.17.18 exact Workspace 以 `X-Session-Id + 最新 user-turn canonical 前缀` 生成 candidate，并优先续用同 affinity 的唯一非终态 task；request key 绑定 harness session 与请求摘要，不含 task_id。Claude 2.1.217 使用 header/body 双 session UUID，Codex 0.145.0 使用冻结的 Responses turn metadata profile，二者都已有真实 CLI resume/partial 证据；未知版本仍降级或 fail closed，不能套用这些结论 |
| Q4 | `max_pending` 默认值 | 取实测硬上限的安全内值，按 harness 与接入档配置 |
| Q5 | 身份披露强度 | 默认让需求方知道背后是人；“伪装”只指协议兼容，不隐瞒身份 |
