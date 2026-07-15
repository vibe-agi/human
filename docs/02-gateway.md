# 02 · Gateway 设计（humand）

humand 对外扮演 Anthropic / OpenAI 的模型 API，对内把请求交给人、把人的响应转回对应方言。它**不管理对话内容**（上下文每次由 harness 全量带来），但**必须管理跨 HTTP 回合的持久任务状态**——因为一次工作是多次 completion 请求的串（§5–6），"这个 tool_call 发出去了、结果还没回来"这类状态跨请求存活。故 humand = 双方言转换 + 准入/队列 + 心跳 + **持久任务状态机** + 审计。

> 本篇吸收 codex review 的 P0 修正：接入分档、跨回合状态机、准入/流式两阶段错误、显式 adapter、稳定标识、默认安全。

## 1. 接入分三档（"一行配置"只属于 Chat 档）

"改一行 base_url"只对最浅的档成立；越往深，需求方要提供越多、装越多。明确三档，避免用一句话承诺全部：

| 档 | 需求方要做 | 能力 |
|---|---|---|
| **Chat** | base_url + token | 纯对话（问答/评审/给方案）。零工具、零文件。 |
| **Remote tools** | 上面 + `harness_id/version` + `workspace_key` + `task_id` + `idempotency_key` + 根 `R` + harness adapter + **shim 或等价边界** | read/edit/exec 在需求方机器执行——环境绑定型排障（adb）主场景；**S1 由此拿到正确性边界与执行去重**（字段见 [05](05-p1-m0-contract.md) §2） |
| **Workspace** | 上面 + 装 **caller helper**，额外传 `snapshot / base_commit / 完整镜像` | 本地 IDE 镜像研发（[03](03-tui.md) §5）；realpath/symlink/执行围栏由 helper 兜（§11） |

一期把 Chat + Remote tools 做扎实（对齐"环境绑定型排障"的聚焦，见 [04](04-milestones.md)）；Workspace 档在 P1-M0 验证长挂通过后才承诺。

## 2. 双方言端点

| 方言 | 端点 | 流式 | 工具 |
|---|---|---|---|
| OpenAI | `POST /v1/chat/completions` | `stream:true` → `data:` SSE，`[DONE]` 收尾 | `tools`/`tool_calls`/`finish_reason` |
| Anthropic | `POST /v1/messages` | SSE 具名 event 流 | `tool_use`/`tool_result`/`stop_reason` |

\+ `GET /v1/models`（列 `human-expert-*` 伪模型名）。鉴权走各方言标准头。

**Responses API 风险（codex 提示）**：OpenAI 的 codex CLI 正迁往 **Responses API**，其自定义 provider 涉及 base_url + wire API + 鉴权三者，且已预告移除 Chat Completions。故：**不把"一期已覆盖 codex"当确定承诺**；把 Responses API 列为 P1-M0 必测方言、并在 canonical 上作为第三 adapter 尽早支持。当前确定支持的是 Anthropic Messages + OpenAI Chat Completions，覆盖哪些具体 harness 由 P1-M0 实测矩阵定。

## 3. canonical 与转换矩阵

逻辑在方言无关的内部格式上进行，每方言一个 adapter：

```
/v1/chat/completions ─┐                         ┌─ OpenAI streamer
/v1/messages ─────────┼─► canonical ─► [人] ─►──┼─ Anthropic streamer
（Responses API）─────┘   request/response      └─（Responses streamer）
```

| 概念 | OpenAI | Anthropic | canonical |
|---|---|---|---|
| system | `role=system` | 顶层 `system` | `system: string` |
| 多模态 | `image_url` | `image(source)` | `blocks[]`(text/image) |
| 工具定义 | `function{parameters}` | `input_schema` | `tools[]`(统一 JSON schema) |
| 工具调用 | `tool_calls{name,arguments(串)}` | `tool_use{name,input(对象)}` | `tool_uses[]`(对象) |
| 工具结果 | `role=tool` | `tool_result` block | `blocks[tool_result]` |
| 结束 | `finish_reason` | `stop_reason` | `stop: end\|tool_use\|max` |

**canonical schema 版本化**：每次演进有版本号；转换器是纯函数。**P1-M0 保存脱敏的真实请求 + 工具 schema 作 golden fixtures**（回归金标准，也是 adapter 的依据，§10）。

## 4. 请求：准入 / 流式两阶段（修 200-vs-503 冲突）

**一旦写出 200 header 就不能再改 HTTP 状态码**。故严格分两阶段：

```
── 阶段 A · 准入（200 之前，可返标准 HTTP 错误）────────────
  鉴权/解析 → 按 (caller_id, idempotency_key) 查记录并校验请求摘要
    · 同 key + 同摘要：复用原任务与响应事件日志（即使当前离线/满载）
    · 同 key + 不同摘要：409 idempotency conflict
    · 未命中：限流 → 接单人在线状态 → 队列容量 → 持久化 admitted（§5）
  其他失败：直接 401/429/503/400（真 HTTP 状态，harness 原生重试接管）
── 阶段 B · 流式（已写 200，只能流内失败或断流）──────────
  写 200 + 开 SSE + 起心跳（§6）
  推送 TUI；人接单后进入 leased
  人读需求 → 去 IDE 干活（可长挂数小时）
    · 期间可吐进度 delta（对方见"模型逐步输出"）
    · 澄清 = 一次不带交付的文字响应（对方回答 → 新请求）
  响应 = 说明文字 +（交付改动→文件 tool_calls / 环境命令）
    → 目标方言 streamer 分块吐出；finish/stop 收尾
  阶段 B 内失败（超 max_pending / 人离线）→ 流内 error event 或断流（§8）
  ▼
审计落库；连接关闭
（人无法主动发起——completion 无反向通道；主动信息搭下次响应）
```

**"已吐进度后再失败"必须单独测**（[04](04-milestones.md)）：客户端未必自动重试部分响应。**流式是伪装的关键**：人的响应一次性产生，但须 delta 分块吐出，否则 harness 流式 UI 与"首 token 超时"失配。

## 5. 跨 completion 的持久任务状态机

assistant 返回 tool_call 后本次 SSE 即结束，执行结果只能在**下一次** completion 请求回流。故 humand 必须持久化跨回合状态：

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

**真实工具流是循环**（read→result→edit→result→exec→final），reconciled 回到原 leased 继续——精确的字段、幂等、对账规则见 [05](05-p1-m0-contract.md) §4。要点：

1. **会话粘连原接单人**：`awaiting_results` 的下一次请求直接回原接单人 lease，不重进 FIFO——否则换人接就丢现场。
2. **只有成功 tool result 才推进镜像 baseline**：对齐发生在**确认**之后（修正"单一写者自动对齐"的乐观）。
3. **多文件部分失败保留未确认 diff**，针对性重试，不整批回滚也不静默丢弃。
4. **请求幂等**：先查 `(caller_id, idempotency_key)`；同 key 同请求摘要复用原任务并重放/续接持久响应事件日志，同 key 不同摘要返 409。
5. **执行幂等**：caller shim 按 `(caller_id, task_id, tool_call_id)` 记持久账本；重复 ID 只重放原 result，绝不再次执行。
6. **状态在写出 200 之前先落 `admitted`**，人接单后才进 `leased`，杜绝"200 已发、任务未落库"的崩溃窗口。
7. **漂移恢复（T-17）属最小闭环**：edit 失败 → 重新 read 回填 → 重做，早期里程碑就要有。

## 6. 心跳与长挂（P1-M0 头号硬门）

harness/中间层有 idle timeout（常 30–120s），人要几分钟到几小时。保活靠"流已开始且仍在产出"：Anthropic `event: ping` 空事件；OpenAI `:` 注释行。挂起期按间隔（默认 15s）发送。

- **进度 delta 兼作强心跳**（比空 ping 更像模型在产出）；
- idle 之外还有**总时长硬上限**（某些 harness/代理有）。`max_pending` 按 harness 硬上限分档（短交互档分钟级 / 研发档小时级）；逼近仍无响应 → 阶段 B 错误语义收尾（§8）；
- **"一个请求能挂多久"是整个方向的唯一硬门**，P1-M0 必验小时级（[04](04-milestones.md)）；撑不住的 harness 只归"短交互档"。

## 7. 会话与工作区标识：正确性用稳定 key，指纹只做 UI

- **正确性绑稳定标识**：鉴权所得 `caller_id` 命名空间 + `workspace_key`（路径/能力/baseline）+ `task_id`（循环/lease/幂等），由接入契约提供——**误合并会串工作区，不只是显示问题**。三概念的正交拆分见 [05](05-p1-m0-contract.md) §1。
- **历史前缀指纹只用于 `ui_conversation_group`（UI 聚合）**：把无 id 的请求在 TUI 呈现为连续对话是锦上添花；指纹断裂 → 降级新卡片，人可手动合并，**不影响正确性**。

## 8. 错误语义：准入错误是真 HTTP，流式错误是流内

| 情形 | 阶段 | OpenAI | Anthropic |
|---|---|---|---|
| 无人在线/队列满/限流（机器可判断） | A（200 前） | `503`+`Retry-After` | `529 overloaded_error` |
| 格式无法转换 | A | `400` | `400` |
| 同一 idempotency key 对应不同请求摘要 | A | `409` | `409` |
| key 无效/超限 | A | `401`/`429` | `401`/`429` |
| **人工拒单**/挂起超时/中途不可用 | B（200 后） | 流内 error chunk 或断流 | 流内 `error` event |

**人工拒单必在阶段 B**：先写 200 → 推 TUI → 人才看到、才能拒——不可能在 A 返 503（时序见 [05](05-p1-m0-contract.md) §5）。阶段 A 只做机器可判断的（在线/容量/限流），**不在 200 前阻塞等某个人接单**，否则重新引入首字节超时。对外错误必须是该方言里**真实模型会返回、且客户端有重试预案**的错误；A 与 B 的失败路径**分别测试**（B 的部分响应重试行为是未知数）。

## 9. 审计与数据保留：默认安全

审计**分两层，默认不同**：

- **元数据层**（默认开）：`{id, caller_id, workspace_key, task_id, dialect, key_id, pending_ms, gen_ms, error?, 时间}`——问责、限流、运营所需，不含源码/输出正文。**只存 api_key 的 ID/hash，绝不存原始 key**。
- **全量 payload 层**（**默认关闭**）：请求原文、工具结果、响应正文——涉及需求方源码。默认关；**显式开启后默认 `TTL = 7 天`**（可配），非默认长存。
- **训练用途、转包**各自**独立 opt-in**：把审计当"人类专家轨迹数据集"训练资产，需求方须单独同意，不能靠一句服务条款打包。

## 10. 能力：显式 adapter，不靠 schema 猜

不能只凭"有 path/content/command"就断定写入/执行语义——不同 harness 在**编辑匹配、并行执行、审批、cwd、删除/重命名、错误格式**上可能完全不同。故：

- **每个已支持 harness 一个显式 adapter / capability profile**（版本化），声明：文件读/写/删/移工具及其匹配语义、命令执行工具及 cwd/超时/审批语义、错误格式。TUI 的文件同步与终端映射到 profile 声明的真实工具。
- **未识别工具默认降级为纯聊天**（Chat 档）；启发式（schema 形状）**只用于提示"可能是文件写入器"，绝不自动启用**写入或命令。
- read/搜索/删除/重命名、惰性镜像首填都在 profile 里定义闭环（G-13 只识别两个能力是不够的）。
- **read/search 的穿越机制必须显式**（否则本地 agent 的 read 只读本地 scratch、不到需求方）：TUI read/search 入口 / 本地 MCP 文件代理 / caller helper 自动 hydrate，三选一，P1-M0 实测择优；配 **caller-side CAS**（edit 带前置条件指纹，需求方侧校验后落盘）——见 [05](05-p1-m0-contract.md) §6。
- 对方的其余自定义工具**不转发给人**，接单人无从调用——面更小更安全。

## 11. 安全：路径围栏是词法纵深防御，执行边界在 caller 侧

**最锋利的事实**：Remote tools/Workspace 档下工具在需求方机器执行、我们不执行任何东西——真正的执行边界是需求方 harness 的权限系统 + caller helper，不在网关。接单人是"不受信任的 tool_call 来源"（与模型同构，但为思考型对手）。三责任：不加剧暴露、纵深防御、诚实划界。

- **路径虚拟对齐**：需求方声明真实根 `R`，跨线路径双向 `R↔V(/workspace)` 改写，隐去 home/用户名；自由文本内路径尽力改写、不保证。基线/仓库暴露（Workspace 档）只服务 git-for-context，传输仍走 tool_call（[03](03-tui.md) §5）。
- **越界防护 = 词法纵深防御**（明确定性）：网关对路径型参数做 `path.Clean` 规整、拒 `..` 爬出 `V`、拒写 `.git/`（尤其 hooks=RCE）、大小写不敏感盘按小写比对、敏感路径（`.env`/`.ssh`/密钥）红标。**但这是词法层纵深防御，不是真边界**——真正的 `realpath`/symlink 解析、执行限制**必须由 caller 侧 shim（helper）完成**，因为只有它在真实文件系统上。
- **shell 无法围栏**（诚实）：`command` 是自由字符串，`cat ../../etc/shadow`、`$HOME/.aws` 无法可靠解析——正则扫危险模式 + TUI 高亮 + 依赖 harness 权限弹窗兜底。
- **命令能力默认关闭**：按 workspace/task 显式开启（与二期 remote exec 一致）。
- **披露**：默认让需求方**知道背后是人**——"伪装"只表示协议兼容，不表示隐瞒身份。转包（本机 agent 代劳）默认走 T-08 核对闸 + opt-in + 数据出境披露。

## 12. 技术栈与存储抽象

- **CLI**：`spf13/cobra`——一期两件套 `humand`（`serve` / `token`〔签发·吊销〕/ `migrate`）与 `human`；`human-mcp` 是二期件（[phase2](phase2-async-mode.md)）。
- **配置**：`spf13/viper`——file（toml/yaml）+ 环境变量 + flag 层叠加载，与 cobra 打通（`BindPFlag`）；配置文件即 [03](03-tui.md) §8 的 toml。
- **传输**：net/http + 标准 SSE；WebSocket（`nhooyr/websocket`）连 TUI。核心 = 方言 adapter（含 golden fixtures 回归）+ 持久任务状态机 + 队列 + 心跳循环。

**存储抽象（driver 可插）**：humand 的持久数据（跨回合任务状态、队列、审计元数据/payload、稳定标识映射）走一层 `Store` 接口，按 `database/sql` 式 **driver 注册**，配置切换：

```
storage.driver = "sqlite"                # 默认：单机零配置（modernc.org/sqlite，免 cgo）
               | "postgres" | "mysql"    # 生产多实例、结构化审计查询
               | "redis"                 # 低延迟任务状态 / 队列 / 会话粘连
```

- 接口**按数据形态切分**，各 driver 实现所需子集：`TaskStore`（状态机，需事务/CAS）、`QueueStore`（FIFO + 会话粘连，redis 天然）、`AuditStore`（元数据结构化查询→sql；payload 大对象 + TTL→可落对象存储或 redis TTL）。
- **一期范围（避免扩大 P1-M1）：只定义 `Store` 接口 + 实现 SQLite driver**（单机零依赖起步够用）；postgres/mysql/redis 接口已就绪，**待多实例需求出现再实现**——上表的非 sqlite 行是接口能力声明，不是 P1-M1 交付。
- 二期 humand 的 task/artifact 存储复用同一抽象（[phase2](phase2-async-mode.md) 选用的 a2a-go `taskstore.Store` 本就是接口，天然对接）。
