# 04 · 里程碑与路线

项目只有一条产品主线：客户侧真实 Agent 通过模型协议调用人类专家。里程碑统一为 **M0/M1/M2**。

当前优先验证环境绑定型排障（adb、内网、现场故障），因为它最需要客户侧 Agent 在真实环境执行工具。长研发体验是否对外承诺，由 M0 的长挂结果裁决。

## 当前落点：代码已实现，外部门尚未通过

| 范围 | 已有证据 | 仍缺证据 |
|---|---|---|
| M0/M1 核心 | `internal/completion/` 三 codec、持久循环、adapter/shim/CAS、镜像/TUI；仓库内 20 次 `read→edit→exec→final` 测试与脱敏 golden fixtures | Workspace 的 snapshot/base_commit/完整镜像字段与主动 TUI read/search；冻结版本的真实三协议 harness 矩阵；10m/2h 长挂；真实 adb demo |
| M2 可靠性 | 单元、集成、端到端、恢复/幂等/安全与 race 测试已覆盖核心路径 | 8h soak、故障矩阵收口、真实用户试点与 20 任务产品门 |

因此以下里程碑是**退出标准**，不能因为代码存在就视为产品门已通过。

## M0 · 可裁决的兼容性与产品实验（1–2 周，go/no-go 硬门）

gateway、caller shim 与 adapter spike 已落地；现在必须接到真实 harness，冻结 harness 版本、OS 与代理链，取得可复现的 go/no-go 数据。仓库内测试不能替代网络代理、客户端总时长上限与部分响应重试行为。

| 维度 | 验证 |
|---|---|
| 长挂 | 短交互档 10min、长研发档 **2h**（头号项） |
| 流控 | 心跳、进度 delta、取消、离线、断流，以及“已吐进度后再失败”的客户端重试行为 |
| 闭环 | 连续 `read → result → edit → result → exec → final` 跨多次 completion 打通 |
| 边界 | 多文件部分失败、重复 POST（幂等）、用户并发修改工作树 |
| 协议 | Chat Completions、Responses API、Anthropic Messages 的真实支持情况 |

**Go/no-go 阈值：**

- 至少一个战略 harness 稳定通过 2h 长挂，才宣称支持长研发体验；否则产品只承诺短交互排障。
- 20 次连续工具闭环必须做到**零重复执行、零静默错误写入**；达不到就先修可靠性，不进入 M1。

**产出：** harness × 能力矩阵（超时硬上限、心跳/进度有效性、闭环可靠性、已知限制），并用脱敏的真实请求和工具 schema 补充现有 golden fixtures。合成 fixture 不能替代外部兼容证据。

## M1 · 单一垂直切片（3–6 周）

只选**一个 harness、一个方言、单专家、单工作区**，把一条路径走通：

- auth 与准入/流式两阶段；
- 跨 completion 持久任务状态机；
- 最小 TUI（对话、进度与交付核对）；
- 显式 read/edit/exec adapter，不靠 schema 猜语义；
- tool result 对账、caller-side CAS 与漂移恢复；
- 取消、断线重连与重启恢复。

完整 clone/bundle、图片、todo、训练数据导出和多专家不阻塞这个切片。三方言 codec 虽已实现，M1 仍只用一个真实 harness/方言完成产品闭环，避免把“代码存在”误当“兼容性成立”。

**退出标准：** 真人通过目标 Agent 完成一次真实 adb 排障。仓库内同构 E2E 已通过，但真实设备与目标 harness demo 尚未执行。

## M2 · 可靠性与协议扩展（7–10 周）

- 把第二方言与 Responses API 接入真实 harness，补齐兼容矩阵；
- 有界公平队列与活跃任务粘连；
- 故障注入、fuzz、race 与 8h soak；
- 默认安全的数据保留与命令授权策略；
- 真实用户试点；
- 仅在 M0 通过 2h 长挂后，补 Workspace 完整镜像研发体验。

**产品门（尚未执行）：** 20 个内部真实任务中，客户确认成功率 ≥ 80%、未授权命令 = 0、静默文件错误 = 0，并记录每专家小时解决任务数。不要把自动化的 20 次协议闭环与这 20 个真人任务混为一谈。

## 验收 demo（M1 末）

> 前置：终端 A 已按 **Remote tools 档**配置 adapter、调用级身份头和 caller shim；只改 base_url 只能进入 Chat 档。

1. 终端 A 的真实 Agent 配置 `base_url=humand, model=human-expert`，用户输入：“Android 设备插在这，登录按钮没反应，帮我查。”
2. 终端 B 的 TUI 收到请求，人接单并发出 `adb logcat -d | grep -i login` tool call。
3. 客户侧 Agent 在客户机器执行 adb，并在下一次 completion 把结果发回。
4. 人根据结果给出编辑；客户侧 Agent 在真实工作区执行 edit，再运行验证命令。
5. 用户在原 Agent 界面持续看到流式进度和最终结果，体验上像在使用一个响应较慢、但能持续判断现场的模型；同时明确知情背后是人。

跑通这条路径，“客户侧真实 Agent 调用动态人类 LLM”的核心假设成立。

## Backlog

| 项 | 说明 |
|---|---|
| 完整 clone/bundle 镜像 | Workspace 研发体验，仅在 M0 长挂通过后投入 |
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
| Q3 | 目标 harness 能否稳定透传每次请求的身份 | Remote tools/Workspace 要求 shim 注入 `workspace_key/task_id/idempotency_key`；不能注入则降级 Chat |
| Q4 | `max_pending` 默认值 | 取实测硬上限的安全内值，按 harness 与接入档配置 |
| Q5 | 身份披露强度 | 默认让需求方知道背后是人；“伪装”只指协议兼容，不隐瞒身份 |
