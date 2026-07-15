# 05 · P1-M0 可执行契约

[02](02-gateway.md) 讲"为什么"，本篇是"照着实现什么"——P1-M0/M1 的精确字段、状态、时序。收口 codex 复审的 4 个实现契约 + read/search + 身份拆分。02 相关节引用此处为准。

## 1. 身份：三个正交概念，不可混用

旧代码把"conversation"既当路径/能力载体又当显示分组，是 bug 源。彻底拆开：

| 概念 | 负责 | 稳定性 | 来源 |
|---|---|---|---|
| `workspace_key` | 路径根 R、能力档、baseline、镜像目录名 | 稳定（一个工作区一个） | Remote tools/Workspace 档由 caller 提供 |
| `task_id` | 一次工具循环、lease（粘接单人）、幂等 | 稳定（一次委托一个） | Remote tools/Workspace 由 caller shim 生成并在每次请求携带 |
| `ui_conversation_group` | 仅 TUI 显示分组 | 可错、可变 | 历史指纹启发式（G-05） |

在鉴权得到的 `caller_id` 命名空间内，**正确性只依赖前两个**；完整边界写作 `caller_id/workspace_key/task_id`。指纹只影响 TUI 卡片聚合，错了不影响状态/镜像/baseline。镜像目录按 `caller_id/workspace_key` 建（不是 `<conv>`）。

## 2. 接入三档要求的字段

| 档 | 必需字段 | 边界保证 |
|---|---|---|
| **Chat** | base_url + token | 无工具，无需身份 |
| **Remote tools** | `harness_id + harness_version` + `workspace_key` + `task_id` + `idempotency_key` + 根 `R` + **shim 或等价 harness 边界** | 主场景 S1 由此拿到正确性边界、执行去重与 realpath/symlink 防护 |
| **Workspace** | 上面 + `snapshot / base_commit / 完整镜像` | 本地 IDE 镜像研发 |

字段由 caller shim 随每次请求注入（具体 header/body 承载由 adapter 定义），gateway 不猜、不靠响应回传后期待通用 harness 自动记住。缺稳定字段或 `harness_id` → 不启用工具，降级 Chat；P1-M0 必须实测目标 harness 确实允许透传。

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
error_shape: 工具失败的回传结构（供对账区分成功/失败/部分）
```

未登记的 harness → Chat 档。schema 形状**只用于提示"疑似写入器"**，绝不自动启用。

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
2. **请求幂等**：Remote tools/Workspace 的 caller shim 为每个逻辑 completion 生成随机 `idempotency_key`，同一次重试保持不变、下一逻辑请求换新；gateway 不再用历史长度推导。阶段 A 在新请求准入检查前先查 `(caller_id, idempotency_key)`：同 key + 同 canonical 请求摘要复用原任务，并重放或续接同一持久响应事件日志；同 key + 不同摘要返 `409 idempotency conflict`。因此重复 POST 不受随后离线/满载影响，也不重复派发工具。
3. **执行幂等 + tool-call 对账**：每个 tool_call 有全局唯一 ID；caller shim 以 `(caller_id, task_id, tool_call_id)` 记持久执行账本，重复 ID 只重放原 result、绝不再次执行。回流 result 按 ID 匹配；成功的推进 baseline，失败/缺失的保留"未确认"待重试（多文件部分失败不整批回滚）。
4. **迟到 result / 部分 SSE 后重试**：result 晚到并入原 `awaiting_results`；客户端在部分 SSE 后重试 → 幂等复用。
5. **状态在写出 200 之前持久化**：先创建或幂等复用任务并落库 `admitted`，再写 200，杜绝"200 已发、任务未落库"的崩溃窗口；人按 `a` 接单后才进入 `leased`。
6. 终态含 `canceled`（caller）/`rejected`（人拒单，§5）/`expired`（超 max_pending）/`failed`（人声明无法完成）。

## 5. 拒单时序：人工拒单在阶段 B

先写 200 → 推 TUI → 人才看到、才能按 `r`。故**人工拒单必然在阶段 B（200 之后）**，不可能在阶段 A 返 503。

- **阶段 A（200 前，可返真 HTTP 错误）**：只做**机器可判断**的——鉴权、接单人在线、队列容量、限流。
- **阶段 B（200 后，只能流内失败/断流）**：人工拒单、超时、中途不可用 → 流内 error event / 断流；行为由 P1-M0 实测（客户端对部分响应的重试未必一致）。
- **不在 200 前阻塞等真人接单**——否则重新引入首字节超时。准入只看"有没有人在线且有容量"，不等"某个人接了这单"。

## 6. read/search 与 caller-side CAS

adapter 声明了 read/search，但本地 agent 的 read 只读**本地 scratch**，不会自动穿越到需求方。必须有明确穿越路径，三选一（P1-M0 实测择优）：

- **TUI read/search/file-open 入口**：人/agent 触发 → 作为 read/search tool_call 发往需求方 → 结果回填镜像；
- **本地 MCP 文件代理**：接单人的 agent 通过一个本地 MCP，其文件读取被代理成需求方 tool_call；
- **caller helper 自动 hydrate**（Workspace 档）：helper 主动把 R 下文件同步进镜像。

**caller-side CAS（编辑前置条件）**：edit 回传时带其所基于内容的指纹（行区间 hash / blob sha）；需求方侧（harness 或 helper）**先校验前置条件再落盘**——匹配才 apply，不匹配则显式失败（对账走第 4 节，重新 read 重做）。这是"逐 tool-call 确认"落到线上的机制，替代"单一写者自动对齐"的乐观假设。

## 7. 本契约对应的验收点（P1-M0）

P1-M0 用手写假 gateway 逐条验证：三档身份字段流通；循环状态机跑通 `read→result→edit→result→exec→final`；澄清回复可回到原 task；重复 POST 与重复 tool_call 均零重复执行；人工拒单在阶段 B 正确降级；read/search 穿越到需求方并回填；20 次连续闭环零重复、零静默错误写入。
