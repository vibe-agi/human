# 04 · 里程碑与路线（一期 = P1）

命名：一期里程碑用 **P1-M0/M1/M2**，二期用 **P2-**（[phase2-async-mode.md](phase2-async-mode.md)），避免两期都叫 M0 混淆。

一期聚焦**环境绑定型排障**（adb/内网/现场故障，最契合 completion 协议、最具差异化）；长研发档（S2）是否一期承诺，由 P1-M0 的长挂结果裁决，不预先承诺。

## P1-M0 · 可裁决的兼容性 + 产品实验（1–2 周，go/no-go 硬门）

**唯一目的**：用最粗糙的手写 gateway（读请求 → 命令行手敲响应 → 转方言吐回）+ 最小 caller shim/adapter spike（稳定身份注入、请求与 tool-call 去重账本、CAS）证明物理可行。**冻结** harness 版本、OS、代理链，让结论可复现。

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

**产出**：harness × 能力矩阵（超时硬上限、心跳/进度有效性、闭环可靠性、已知坑）+ **golden fixtures**（脱敏的真实请求 + 工具 schema，作 adapter 依据与回归金标准）。

## P1-M1 · 单一垂直切片（3–6 周）

只做**一个 harness、一个方言、单专家、单工作区**，把一条路走通走透：

- auth + **准入/流式两阶段**（[02](02-gateway.md) §4）；
- **跨 completion 持久任务状态机**（§5）；
- 最小 TUI（对话 + 交付核对）；
- **显式 read/edit/exec adapter**（不靠 schema 猜）；
- tool result 对账 + **漂移兜底 T-17**；
- 取消与重启恢复。

**先不做**：完整 clone/bundle、图片、todo、训练数据导出、多方言、多专家。

**退出标准**：真人经此切片完成 **S1（adb 短交互）** 全流程——一期验收 demo（见下）。

## P1-M2 · 可靠性 + 第二协议（7–10 周）

- 第二方言或 **Responses API** adapter；
- 有界公平队列 + **活跃会话粘连**；
- **故障注入、fuzz、race、8h soak**；
- 安全与数据保留策略（默认安全，[02](02-gateway.md) §9/§11）；
- 真实用户试点。
- 若 P1-M0 长挂过 2h：加 Workspace 档完整镜像研发（S2）。

**产品门**：20 个内部任务验证——**caller 确认成功率 ≥ 80%、未授权命令 = 0、静默文件错误 = 0**，并记录"每专家小时解决任务数"作为经济性指标。

## 一期验收 demo（P1-M1 末）

> 前置：终端 A 已按 **Remote tools 档**配置 adapter、调用级身份头和 caller shim；仅改 `base_url` 只能进入 Chat 档。
> 终端 A：Claude Code 配置 `base_url=humand, model=human-expert`，用户输入"我 Android 设备插在这，登录按钮点了没反应，帮我查"。
> 终端 B（TUI）：请求入队+通知 → 人接单 → 读诉求 → 环境命令框输 `adb logcat -d | grep -i login`、发送 → 终端 A 的 Claude Code **在需求方机器上真的执行了 adb**、日志随下一回合回到 TUI → 人在**自己的编辑器**里改镜像目录的 `Login.kt`（或让本机 agent 改）→ `R` 核对到自动检测的改动 → 聊天补一句说明 → 预览发送 → 对方机器文件**真的被改**、跑通 → 完成。
> 终端 A 用户**体验上**与用一个"很懂 Android、但有点慢"的模型无异——但采购时已知情背后是人（[02](02-gateway.md) §11 披露），"伪装"仅指协议兼容、非隐瞒身份。

跑通这条（S1，短交互），"人无缝伪装成模型"的核心假设成立。S2（研发交付）待 P1-M0 长挂过关后于 P1-M2 加演。

## Backlog（二期或以后）

| 项 | 说明 |
|---|---|
| **人 = Agent 异步模式** | A2A + MCP + worktree + patch 交付全套，[phase2-async-mode.md](phase2-async-mode.md) 已设计并经 TLA+ 有限状态检查 |
| 完整 clone/bundle 镜像 | Workspace 档研发，gated on P1-M0 长挂结果 |
| LLM 起草辅助 | gateway 代写草稿、人审核——注意区别于"接单人自带 agent"（一期默认） |
| 人回传图片 | assistant 协议不支持图片；用 gateway 托管 URL 或文字描述变通 |
| 更多方言 | Gemini、Bedrock、Ollama（canonical 已解耦，加 adapter） |
| 团队/路由/市场 | 多接单人、技能路由、计费；元数据审计已留数据 |
| 会话主动召回 | 人主动 ping 需求方（反向发起，超出 completion 语义，需 A2A） |
| PTY / 长驻进程 | 交互式终端（对方 harness 支持才有意义） |

## 开放问题（P1-M1 前拍板）

| # | 问题 | 默认 |
|---|---|---|
| Q1 | 伪模型名命名（影响 harness 模型选择器展示） | `human-expert`；多专家 `human-expert-<name>` |
| Q2 | Responses API vs Chat Completions 的投入次序（codex 预告移除 Chat Completions） | P1-M0 实测两者，按结果定；Anthropic Messages 必做 |
| Q3 | 稳定标识如何注入每次请求 | P1-M0 验证 harness 透传能力；Remote tools/Workspace 必须由 caller shim 注入 `workspace_key/task_id/idempotency_key`，不能注入则降级 Chat |
| Q4 | max_pending 默认值与各 harness 上限的关系 | 取实测硬上限的安全内值，per-harness/per-档 可配 |
| Q5 | 身份披露的强度（对终端用户 vs 对采购方） | 默认让需求方知道背后是人；"伪装"仅指协议兼容 |
