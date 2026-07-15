# 05 · M0 可执行契约

[02](02-gateway.md) 讲"为什么"，本篇是"照着实现什么"——M0/M1 的精确字段、状态、时序。本文定义身份、adapter、循环、拒单与 read/search 的实现边界，02 相关节引用此处为准。

> **当前落点**：循环状态机与 Remote tools 核心已在 `internal/completion/`、caller shim、SQLite store、gateway 与 worker 协议中实现；仓库内直接协议测试跑通 20 次闭环、重复 POST 与重复 tool-call 去重。Workspace 的 snapshot/base_commit/完整镜像字段校验尚未实现；真实外部 harness 透传、10m/2h 长挂与部分响应重试也仍未执行。

## 1. 身份：三个正交概念，不可混用

旧设计把"conversation"既当路径/能力载体又当显示分组，是 bug 源；当前实现已拆开：

| 概念 | 负责 | 稳定性 | 来源 |
|---|---|---|---|
| `workspace_key` | 路径根 R、能力档、baseline、镜像目录名 | 稳定（一个工作区一个） | Remote tools/Workspace 档由 caller 提供 |
| `task_id` | 一次工具循环、lease（粘接单人）、幂等 | 稳定（一次委托一个） | Remote tools/Workspace 由 caller 集成显式分配；当前 shim 启动时绑定，重启必须复用同一值 |
| `ui_conversation_group` | 仅 TUI 显示分组 | 可错、可变 | 历史指纹启发式（G-05） |

在鉴权得到的 `caller_id` 命名空间内，**正确性只依赖前两个**；完整边界写作 `caller_id/workspace_key/task_id`。指纹只影响 TUI 卡片聚合，错了不影响状态/镜像/baseline。镜像目录按 `caller_id/workspace_key` 建（不是 `<conv>`）。

## 2. 接入三档要求的字段

| 档 | 必需字段 | 边界保证 |
|---|---|---|
| **Chat** | base_url + token | 无工具，无需身份 |
| **Remote tools** | `harness_id + harness_version` + `workspace_key` + `task_id` + `idempotency_key` + 根 `R` + **shim 或等价 harness 边界** | 主场景 S1 由此拿到正确性边界、执行去重与 realpath/symlink 防护 |
| **Workspace** | 上面 + `snapshot / base_commit / 完整镜像` | 目标契约；当前尚无这些字段的完整校验，不能据此宣称 Workspace 档已实现 |

字段由 caller 集成显式提供，shim 校验后随每次请求注入（具体 header/body 承载由 adapter 定义），gateway 不猜、不靠响应回传后期待通用 harness 自动记住。通用 gateway 对缺稳定字段或未知 `harness_id` 的请求降级 Chat；当前项目自有 Remote-tools shim 更严格：启动时必须显式配置稳定 `caller_id/workspace_key/task_id`，过滤来路 `X-Human-*` 后只注入一份受信 `X-Human-Caller-Id`，humand 在读取请求体和 `BeginRequest` 前将它与 caller token 的 principal 比对。错配返 `403`，human-shim profile 缺声明返 `428`；每个逻辑 completion 还必须携带 `Idempotency-Key`，缺失或 task 不匹配同样在代理前返回 `428`。因此 token 轮换可以保留 ledger 命名空间，但配置不能借此切换 caller 身份。

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
2. **请求幂等**：caller 集成为每个逻辑 completion 分配随机 `idempotency_key`，同一次精确重试保持不变、下一逻辑请求换新；shim 只校验并透传显式 key，**禁止按 body hash 合并**。阶段 A 在新请求准入检查前先查 `(caller_id, idempotency_key)`：同 key + 同 canonical 请求摘要复用原任务，并重放或续接同一持久响应事件日志；同 key + 不同摘要返 `409 idempotency conflict`。HTTP 边界也有不可变的持久裁决：`0=尚未裁决`、`200=SSE`、`4xx/5xx=带原 content-type/body 的终结响应`；并发重试在裁决前等待，裁决后只能逐字节遵循原路径。持久 200 的 replay 在写出首字节前若暂时读库失败，会受 request/runtime context 约束地重试，不能退化为空 200。因此两个 body 相同但 key 不同的独立请求不会折叠，重复 POST 也不受随后离线/满载影响。
3. **执行幂等 + tool-call 对账**：每个 tool_call 有全局唯一 ID；当前 caller shim 是单 task 边界，从受信启动配置覆盖工具请求 body 中的 `caller_id/task_id`，再以 `(caller_id, task_id, tool_call_id)` 记 SQLite 持久执行账本。复用同一 caller/task 配置和 ledger 重启（即使轮换 caller token）仍只重放原 result、绝不再次执行；改变 body 身份不能绕过账本。回流 result 按 ID 匹配；成功的推进 baseline，失败/缺失的保留"未确认"待重试（多文件部分失败不整批回滚）。
4. **迟到 result / 部分 SSE 后重试**：result 晚到并入原 `awaiting_results`；客户端在部分 SSE 后重试 → 幂等复用。
5. **状态和响应边界在写出前持久化**：先创建或幂等复用任务并落库 `admitted`，持久化 stream start，再提交 `200/SSE` 裁决；随后写出并同步 flush `200 + start`，最后才 Enqueue 让 TUI 可见。故 200 前失败可原样重放 HTTP 状态/body；flush 后 worker 在窗口消失只能持久化流内 `unavailable` 终态。重启会恢复“已提交 200、尚未 Enqueue”的原 assignment，不会改走 500 或留下半截流。人按 `a` 接单后才进入 `leased`。
6. **worker 事件分阶段可恢复**：每个事件以稳定 `event_id + digest` 唯一，按 `step → state effects → applied → response complete → receipt/ACK` 收口；`step/applied` 精确重放复用原行，冲突 fail closed。codec 的 stream/event 时间种子与 wire 一并持久化，重启按 seed 重建并逐字节核验，避免 Responses/OpenAI 的时间或序号漂移。任一阶段出现可重试存储错误后，当前 consumer 串行续跑该事件，不让 heartbeat、expiry 或下一事件越过；已 complete 但缺 receipt 的请求也会在启动恢复中补齐。
7. 终态含 `canceled`（caller）/`rejected`（人拒单，§5）/`expired`（超 max_pending）/`failed`（人声明无法完成）。

## 5. 拒单时序：人工拒单在阶段 B

先写 200 → 推 TUI → 人才看到、才能按 `r`。故**人工拒单必然在阶段 B（200 之后）**，不可能在阶段 A 返 503。

- **阶段 A（200 前，可返真 HTTP 错误）**：只做**机器可判断**的——鉴权、接单人在线、队列容量、限流。
- **阶段 B（200 后，只能流内失败/断流）**：人工拒单、超时、中途不可用 → 流内 error event / 断流；行为由 M0 实测（客户端对部分响应的重试未必一致）。
- **不在 200 前阻塞等真人接单**——否则重新引入首字节超时。准入只看"有没有人在线且有容量"，不等"某个人接了这单"。

## 6. read/search 与 caller-side CAS

Remote tools 的文件穿越只有一条实现路径：**caller shim 或等价 harness adapter 暴露并执行受信工具，客户侧 Agent 负责标准 tool-call/result 循环。**

1. 客户侧集成以受限 token 读取 `GET /internal/v1/tools/schema`，获得 `human_read_file`、`human_search`、`human_write_file`、`human_edit_file`、`human_delete_file`、`human_rename_file`，以及显式开启时才出现的 `human_exec`。
2. 人的 completion 响应发出相应 tool call；客户侧 Agent 调用 `POST /internal/v1/tools/execute`。shim 不信任 body 内的 caller/task 身份，而是用受信启动配置覆盖，再在客户工作区执行。
3. tool result 由客户侧 Agent 放入下一次 completion，请求回到同一 `task_id` 与原 lease；gateway 对账后继续循环。

**caller-side CAS（编辑前置条件）**：read 返回内容指纹；write/edit/delete/rename 必须带 `expected_sha256`。shim 在真实文件系统上先校验指纹、realpath 与 symlink 边界，再落盘；不匹配就显式失败，随后重新 read 并重做。执行账本再以 tool-call ID 保证精确重试只重放原结果。这是“逐 tool-call 确认”的线上机制，不依赖镜像自动一致。

## 7. 本契约对应的验收点（M0）

**仓库内已验证**：项目自有 gateway + caller shim 通过直接 HTTP/tool 协议连续跑通 20 次 `read→result→edit→result→exec→final`；显式 key 的重复 POST 与重复 tool-call 复用持久结果；两个同 body 独立 key 不折叠，工具账本可跨 shim 重启重放。

**M0 仍待外部验证**：冻结版本的目标 harness 是否透传三档身份字段并正确执行 shim 工具循环；Chat/Responses/Anthropic 的 10m/2h 长挂；已吐进度后流内失败的客户端重试；真实 read/search schema/结果格式；20 次外部闭环的零重复、零静默错误记录。只有这些完成，才可给具体 harness 下兼容结论。
