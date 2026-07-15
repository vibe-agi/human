# 04 · 里程碑与路线（一期 = P1）

命名：一期里程碑用 **P1-M0/M1/M2**，二期用 **P2-**（[phase2-async-mode.md](phase2-async-mode.md)），避免两期都叫 M0 混淆。

一期聚焦**环境绑定型排障**（adb/内网/现场故障，最契合 completion 协议、最具差异化）；长研发档（S2）是否一期承诺，由 P1-M0 的长挂结果裁决，不预先承诺。

## 当前落点：代码已实现，外部门尚未通过

| 范围 | 已有证据 | 仍缺证据 |
|---|---|---|
| P1-M0/P1-M1 核心 | `internal/completion/` 三 codec、持久循环、adapter/shim/CAS、镜像/TUI；仓库内 20 次 `read→edit→exec→final` 测试与脱敏 golden fixtures | Workspace 的 snapshot/base_commit/完整镜像字段与主动 TUI read/search；冻结版本的真实三协议 harness 矩阵；10m/2h 长挂；真实 adb demo |
| P1-M2 可靠性 | 单元、集成、端到端、恢复/幂等/安全与 race 测试已覆盖核心路径 | 8h soak、故障矩阵收口、真实用户试点与 20 任务产品门 |
| P2 异步核心 | `internal/delegation/`、官方 A2A、worker/worktree/rewind、human-mcp/apply、remote exec 已实现并有仓库内 E2E | 外部 MCP/A2A harness 试点、长期运行和产品体验门 |

因此下文里程碑继续作为**退出标准**，不能因代码存在就标记产品门通过。

## P1-M0 · 可裁决的兼容性 + 产品实验（1–2 周，go/no-go 硬门）

**剩余目的**：代码层的 gateway + caller shim/adapter spike 已落地；现在必须把它接到真实 harness，**冻结** harness 版本、OS、代理链，取得可复现的 go/no-go 数据。仓库内测试不能替代网络代理、客户端总时长上限与部分响应重试行为。

验证矩阵：

| 维度 | 验证 |
|---|---|
| 长挂 | 短交互档 10min、长研发档 **2h**（头号项） |
| 流控 | 心跳、进度 delta、取消、离线、**断流、"已吐进度后再失败"的客户端重试行为** |
| 闭环 | 连续 `read → result → edit → result → exec → final` 跨多次 completion 打通 |
| 边界 | 多文件部分失败、重复 POST（幂等）、用户并发修改工作树 |
| 协议 | **Chat Completions / Responses API / Anthropic** 三者的真实支持情况 |

**Go/no-go 阈值（明确）**：

- 至少一个战略 harness **稳定通过 2h 长挂** → 才宣称支持长研发档；否则**一期只做短排障，S2 长编码直接交二期**。
- 20 次连续工具闭环：**零重复执行、零静默错误写入**——达不到则闭环不可靠，先修再走。

**产出**：harness × 能力矩阵（超时硬上限、心跳/进度有效性、闭环可靠性、已知坑）+ 对现有代表性 **golden fixtures** 补充脱敏的真实 harness 请求与工具 schema。现有 fixture 已进入回归测试，但不是外部实测替代品。

## P1-M1 · 单一垂直切片（3–6 周）

只做**一个 harness、一个方言、单专家、单工作区**，把一条路走通走透：

- auth + **准入/流式两阶段**（[02](02-gateway.md) §4）；
- **跨 completion 持久任务状态机**（§5）；
- 最小 TUI（对话 + 交付核对）；
- **显式 read/edit/exec adapter**（不靠 schema 猜）；
- tool result 对账 + **漂移兜底 T-17**；
- 取消与重启恢复。

**切片验收不依赖**：完整 clone/bundle、图片、todo、训练数据导出、多专家。三方言 codec 虽已提前实现，P1-M1 仍只选一个真实 harness/方言跑垂直切片，避免把“代码存在”误当“多协议产品化完成”。

**退出标准**：真人经此切片完成 **S1（adb 短交互）** 全流程——一期验收 demo（见下）。仓库内 E2E 已通过同构 read/edit/exec 闭环；真实设备与目标 harness demo 尚未执行，因此本退出标准未宣告通过。

## P1-M2 · 可靠性 + 第二协议（7–10 周）

- 第二方言与 **Responses API** adapter 已提前实现；P1-M2 剩余门是把它们接入真实 harness、补兼容矩阵与可靠性数据；
- 有界公平队列 + **活跃会话粘连**；
- **故障注入、fuzz、race、8h soak**（已有核心故障/race 测试；8h soak 尚未执行）；
- 安全与数据保留策略（默认安全，[02](02-gateway.md) §9/§11）；
- 真实用户试点。
- 若 P1-M0 长挂过 2h：加 Workspace 档完整镜像研发（S2）。

**产品门（尚未执行）**：20 个内部真实任务验证——**caller 确认成功率 ≥ 80%、未授权命令 = 0、静默文件错误 = 0**，并记录"每专家小时解决任务数"作为经济性指标。不要把自动化的 20 次协议闭环与这 20 个真人任务混为一谈。

## 一期验收 demo（P1-M1 末）

> 前置：终端 A 已按 **Remote tools 档**配置 adapter、调用级身份头和 caller shim；仅改 `base_url` 只能进入 Chat 档。
> 终端 A：Claude Code 配置 `base_url=humand, model=human-expert`，用户输入"我 Android 设备插在这，登录按钮点了没反应，帮我查"。
> 终端 B（TUI）：请求入队+通知 → 人接单 → 读诉求 → 环境命令框输 `adb logcat -d | grep -i login`、发送 → 终端 A 的 Claude Code **在需求方机器上真的执行了 adb**、日志随下一回合回到 TUI → 人在**自己的编辑器**里改镜像目录的 `Login.kt`（或让本机 agent 改）→ `R` 核对到自动检测的改动 → 聊天补一句说明 → 预览发送 → 对方机器文件**真的被改**、跑通 → 完成。
> 终端 A 用户**体验上**与用一个"很懂 Android、但有点慢"的模型无异——但采购时已知情背后是人（[02](02-gateway.md) §11 披露），"伪装"仅指协议兼容、非隐瞒身份。

跑通这条（S1，短交互），"人无缝伪装成模型"的核心假设成立。S2（研发交付）待 P1-M0 长挂过关后于 P1-M2 加演。

## Backlog（二期或以后）

| 项 | 说明 |
|---|---|
| **人 = Agent 异步模式产品化** | A2A + MCP + worktree + patch/rewind/remote exec 核心已实现；后续是外部试点、体验完善与长期可靠性，[phase2-async-mode.md](phase2-async-mode.md) |
| 完整 clone/bundle 镜像 | Workspace 档研发，gated on P1-M0 长挂结果 |
| LLM 起草辅助 | gateway 代写草稿、人审核——注意区别于"接单人自带 agent"（一期默认） |
| 人回传图片 | assistant 协议不支持图片；用 gateway 托管 URL 或文字描述变通 |
| 更多方言 | Gemini、Bedrock、Ollama（canonical 已解耦，加 adapter） |
| 团队/路由/市场 | 多接单人、技能路由、计费；元数据审计已留数据 |
| 会话主动召回 | 人主动 ping 需求方（反向发起，超出 completion 语义，需 A2A） |
| PTY / 长驻进程 | 交互式终端（对方 harness 支持才有意义） |

## 仍需外部实测或人工判断

| # | 问题 | 默认 |
|---|---|---|
| Q1 | 伪模型名命名（影响 harness 模型选择器展示） | `human-expert`；多专家 `human-expert-<name>` |
| Q2 | 三种 codec 均已实现后，具体 harness 优先投入哪一条 | P1-M0 实测 Chat/Responses/Anthropic 后按兼容性与战略价值定，不从代码完成度推断 |
| Q3 | 目标 harness 能否稳定透传每次请求的身份 | 代码契约要求 Remote tools/Workspace 由 caller shim 注入 `workspace_key/task_id/idempotency_key`；仍须逐 harness 验证，不能注入则降级 Chat |
| Q4 | max_pending 默认值与各 harness 上限的关系 | 取实测硬上限的安全内值，per-harness/per-档 可配 |
| Q5 | 身份披露的强度（对终端用户 vs 对采购方） | 默认让需求方知道背后是人；"伪装"仅指协议兼容 |
