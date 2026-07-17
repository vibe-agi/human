# 03 · TUI 规格（human）

> **实现边界**：`human` 的 completion TUI 基于官方 [Bubble Tea v2](https://github.com/charmbracelet/bubbletea)（`charm.land/bubbletea/v2`），并用 [Bubbles](https://github.com/charmbracelet/bubbles)（`charm.land/bubbles/v2`）与 [Lip Gloss](https://github.com/charmbracelet/lipgloss)（`charm.land/lipgloss/v2`）实现聊天输入、滚动和自适应布局；单屏常驻 `CHAT / TASKS / COMMAND / REPLY`，另有轻量 Inbox、连续 progress、计划编辑、声明命令与 `[t]` 高级工具。Live Workspace 已实现递归 `fsnotify`、全量 review、手动 preview/confirm 与 opt-in auto-send；worker-local state DB 持久化 Reply/Command/Tasks 草稿及最多 32 个 continuation。OpenCode 1.17.18 exact Workspace 已用真实 CLI 跑通空镜像 `:pull` 精确播种 → 原生 edit/result → bash/tasks → final → 同 session terminal 后下一 user turn 新 task；Codex 0.144.5 已实测 Responses Basic 命令工具循环，但其 TUI Tasks/Workspace 尚未实测；Claude 仍只有本机契约/仓库测试。图片内联与 IDE 快捷入口尚未实现。

## 1. 定位：沟通与交付的信使台

TUI **不是原始模型协议控制台**。客户侧 Agent 负责 tool-call/result 循环；TUI 把其中与人有关的内容整理成四个常驻区域：

- **CHAT**：连续展示客户、Human 与工具结果；
- **REPLY**：像聊天框一样直接输入；`Enter` 发送 progress 段并保持人工回合，`Ctrl+J` 换行，`Ctrl+R` clarification/handoff 给 Agent，`Ctrl+D` final 结束；
- **TASKS**：只展示并编辑**客户 Agent 自己的计划列表**，与接单 Inbox 分离；
- **COMMAND**：生成客户 Agent 已声明命令工具的调用，绝不在 Human 本机执行。

SSE event 与方言字段不直接暴露给人。Tasks/Command 只把 caller **已经声明**且 schema 完整匹配的工具变成专用编辑器，不增加任何能力；其它工具可在 Tasks 焦点按 `t` 输入 `<tool-name> <JSON object>`，一行一个调用，`Ctrl+S` 发送。Basic 以本次 declaration 为边界；exact Workspace/Remote tools 中 mapped/已审 standard 可直接发，privileged 或未分类 custom/MCP 工具还要求任务显式 active-capability opt-in（当前复用 `X-Human-Allow-Exec`）。系统负责生成方言对应的一个或多个 wire tool call，相关 result（包括工具失败正文）会在下一次 completion 中以可读形态出现。

请求、工具目录或 schema 超出当前终端时不会静默丢弃：默认显示最新上下文，固定保留四个操作区和状态行，`PgUp/PgDn` 可浏览完整 system/messages、完整工具说明与当前工具的完整 schema。长草稿只折叠显示早期行，发送内容仍保留完整值。粘贴入 Reply/Command 的 CRLF 或单独 CR 先归一为 LF，不会把换行用的 `\r` 污染成实际发送给 Agent 的 `␍` 字符。所有 caller 文本在渲染前会把 ANSI/C0/C1、双向文本控制等终端控制字符转成惰性的可见记号，不能借请求内容清屏、隐藏或伪造操作区。

**所有工具与文件交付都沿客户 Agent 的显式 tool-call 边界**：Basic 直接调用其声明的 read/edit/exec；Live Workspace 在保存后自动 fresh review，默认仍由人 preview/confirm。显式开启 `workspace.auto_send` 后，保存表示发送意图，只有 change-level `allow` 的改动自动生成 exact profile tool call；安全 warning/block 或基线冲突会停住。OpenCode 原生 profile 没有 remote SHA/CAS，界面会展示这一 adapter warning，但最终执行仍由客户 Agent 的权限系统裁决。

三原则：**读懂需求**（对话渲染要清晰）、**在自己地盘干活**（TUI 不抢 IDE 的活）、**交付前核对**（所见即所发）。

## 2. 沟通形态：对话，不能主动发起

需求方那头是 harness，它以为在跟一个模型对话——**只能它发起，人只能响应**（completion 协议无反向通道，codex/claude 均不支持模型主动找用户）。所以：

- **就是对话**：对方在它的 harness 里说需求 → 作为请求进来 → 人在 TUI 读到、回复。人工回合开始后可反复按 `Enter` 发送 progress 段，像聊天一样连续补充信息而不关闭响应；需要对方 Agent 继续行动或回答时按 `Ctrl+R` 发 clarification/handoff，完成时按 `Ctrl+D` 发 final。人的回复可以是：**交付**（说明文字 + 经确认的工作目录改动）、**纯澄清反问**（"确认下：登录页还是注册页？"），或中间说明。
- **进度可流式，不必憋到干完**：`Enter` 发送的各段在同一 completion 内依次流给 Agent，发送成功后输入框保持可用；OpenCode 1.17.18 已真实收到 progress + 多 tool call + final，并留有 617.1 秒/41 心跳与有效 `2h0m53.2s`/483 心跳的持续流证据。这不外推到其他 harness/profile，也不代替当前的断网与服务恢复门。
- **想说的话只能搭在响应里**：没有 pending 请求时人无法主动找对方。completion 协议没有模型反向发起通道，所以提醒、建议等信息只能搭在当前或下一次响应里带出去。
- **本机工具自由**：接单人可用自己的 IDE 或 Agent 处理镜像目录；TUI 只观察保存后的文件系统结果。默认人工确认；若显式开启 auto-send，保存干净改动本身就是发送意图。人仍对交付负责，且涉及第三方模型时必须披露数据出境（[02](02-gateway.md) §11）。

## 3. 信息架构

只有一个工作屏，不再让人在多个页面之间来回切换：

```
┌─ CHAT · request … · your turn ────────────────────────────────┐
│ CLIENT  修复登录 500，并持续更新计划                            │
│ TOOL    read … → NullPointerException                         │
│ YOU     我先补判空，再跑测试                                    │
├─ TASKS · Agent plan ──────────────────────────────────────────┤
│ ✓ 复现                                                        │
│ ◐ 修复登录判空                                                 │
│ □ 跑测试                                                       │
│ INBOX 1/2 · a accept · r reject                                │
├─ COMMAND · bash · Enter send ─────────────────────────────────┤
│ go test ./...                                                  │
├─ REPLY · Enter progress · Ctrl+R handoff · Ctrl+D final ──────┤
│ 直接输入回复…                                                  │
└─ Tab/Shift+Tab focus · Ctrl+J newline · Status: your turn ────┘
```

- **Inbox 只是轻量提示**：没有已接请求时，在 Tasks 区显示选中的等待请求；`a` 接单、`r` 拒单，`Enter` 不会误接。它不是 Tasks 的数据来源。
- **持续人工回合**：接入当前请求后自动聚焦 Reply；`Enter` 只流出一个 progress 段，发送后仍停留在人工回合，可继续输入。`Ctrl+R` 发 clarification/handoff，把控制权交回 Agent；`Ctrl+D` 发 final，明确结束对话响应。
- **handoff / 工具后的视觉续接**：`Ctrl+R` clarification/handoff 以及 task、command、高级工具或文件 tool call 都会结束当前 completion。工具 continuation 必须带回全部预期 `tool_call_id`；Remote tools / Workspace 的 handoff 还须匹配稳定 caller/workspace/task。TUI 可同时停放最多 32 个 continuation，返回顺序不必与发出顺序相同；默认 state DB 会跨 TUI 重启恢复它们和未同步草稿。身份/结果不匹配的请求安全进入 Inbox。Chat 可按精确 tool result ID 续接，但没有稳定 task 的纯 handoff 不自动合并。视觉连续不改变 completion 协议边界。
- **焦点而非模式键**：`Tab / Shift+Tab` 在 Reply、Command、Tasks 间切换。输入框中的 `a/q/t/x` 都是普通文字，只有 `Ctrl+C` 是全局退出。

### 3.1 Tasks 是 caller Agent 的计划

Tasks 从当前请求历史里的 tool call/result 按 `tool_call_id` 恢复确认状态；本地编辑先形成草稿，`Ctrl+S` 再通过 caller 本轮声明的计划工具发送一个**全量列表**，由此同步到客户侧 Agent 的 Tasks 视图。当前识别三种精确形态：

| caller | 工具 | 全量列表 |
|---|---|---|
| OpenCode | `todowrite` | `todos[{content,status,priority}]` |
| Claude | `TodoWrite` | `todos[{content,status,activeForm}]` |
| Codex | `update_plan` | `plan[{step,status}]`（可带 `explanation`） |

只有 OpenCode 1.17.18 已用真实 fixture 和工具闭环核对；Claude/Codex 只是按本机可见契约做了严格 matcher、编码器和仓库测试，尚未在真实 Claude/Codex harness e2e。工具缺失、声明多个兼容工具、schema 漂移、结果冲突或前一更新仍未收到 result 时，Tasks 会只读禁用，而不是猜字段或生成 Human 自用 Todo。结果冲突是从**当前请求历史**逐次重算的 fail-closed 派生状态，并非进程内锁存位：只要历史仍保留任一成功但 call/result 不一致的任务记录，本回合就继续只读；历史压缩不再携带该记录，或请求不再声明任务工具时，状态自然解除。目前没有在应用内忽略某条分歧的手动操作。

### 3.2 Command 是 caller 工具的快捷输入

Command 仅在 caller 本轮明确声明兼容 `bash`，或 Remote adapter 对当前任务授权 exec 时启用。`Enter` 只生成 tool call，命令始终由客户 Agent 在它自己的权限系统和机器上执行；Human TUI **绝不本地执行**。精确 `opencode@1.17.18` Workspace 还识别 `:pull path/to/file`：它生成 OpenCode 原生 `bash` 调用 `opencode debug file read --pure`，经 caller Agent 的正常权限闸返回 base64 精确字节并播种 mirror。空文件可 hydrate；前导 `-` 路径会以 `./` 消歧。命中危险词法会先警告，需再次 `Enter` 确认；schema 不兼容时禁用，并提示改用 `[t]` 高级工具输入。

## 4. 上下文渲染（读懂需求的关键）

- **system 提示**默认折叠（往往很长且每回合重复），可展开；
- **消息流** markdown 渲染；人自己的进度/交付/反问也在流里，形成连续对话感；
- **工具结果**（对方 harness 在其循环里产生的、需要人看的部分，如报错、日志）语法高亮 + 折叠 + 懒加载；
- **会话续接**：只高亮本请求新增的尾部，人不必重读历史；
- **路径显示服从 profile**：mirror 内部始终使用相对路径；`human-shim@1` 的 wire 坐标是 `/workspace`，`opencode@1.17.18` 则显示/发送 caller 的绝对 `filePath/workdir`，不承诺隐藏 home/用户名；
- 图片内联（对方消息里的截图，kitty/iTerm2，降级占位）。

## 5. Live Workspace（核心文件协作通道）

**镜像目录**：对方工作区在接单人机器上的本地草稿（`~/mirror/<caller_id>/<workspace_key>`，按正确性命名空间而非会话建目录，[05](05-m0-contract.md) §1）。人用自己的 IDE/Agent 在这里工作；递归 watcher 忽略 `.git`、debounce 后触发一次全量 review，因此编辑器的 rename-save/coalesced event 不会直接被当成增量真相。`R`/`Ctrl+P` 是人工复核与 watcher 失效时的兜底。

**铁律：唯一真相是对方的工作树，镜像只是草稿板、不是第二份真相。** 镜像 = "接单人读到/知道的东西"物化成文件（等价于 Claude 上下文里的文件视图，只是落了盘），从真相播种、随时可丢、有疑问就重新同步。交付的是"施加在对方树上的一条条编辑"，**绝不是"我这份镜像"**——严禁把整个镜像当第二份真相回传。两边靠**逐 tool-call 对账**保持一致（不是"一致性消失"，是缩小到每次 edit 的确认）：baseline 只在收到**成功 tool result** 后推进；对方手改 / formatter / 多文件部分失败都表现为 edit 响亮失败 → 重新 read 回填重做（fail-explicit）。**唯一例外是 `write` 整文件覆盖**（无 `old_string` 保护，会静默盖掉对方并发改动）——故已有文件优先 `edit`，`write` 只用于新文件或明确整体替换。

### 5.1 git 只用于"上下文"，不用于"传输"

- **git-for-context**：给接单人一份可导航、基线正确的完整代码库（人和其 agent 能 grep、读结构）。**这是接单人侧需要 git 的唯一理由。**
- **git-for-transport**：**不存在**。改动回传永远走 tool_call 通道（edit/write 由对方 harness 执行），绝不走 git push/pull——否则同一改动被 git 与 tool_call 双重应用，必然打架。TUI 的 `:pull path` 只是通过原生 tool call 精确读取一个文件，与 `git pull` 无关。

git 只在**建立时用一次**给接单人正确起点；此后两边 git 各过各的，只有文件增量单向（镜像→对方）经 tool_call 流动。

### 5.2 回传认内容不认分支：两边不同分支不会导致 apply 冲突

- **传输是 edit/write tool_call，匹配文件内容、不认分支**：`edit(path, old_string, new_string)` 成功的唯一条件是对方**当前**文件里找得到 `old_string`——分支名、commit hash 一概不影响。接单人就像"坐在对方键盘前的模型"，改动落在对方**当前 checkout 的树**上；接单人本地 git 只用于整理草稿，不参与传输。故不存在"两边分支不同 → 编辑冲突"。
- **唯一真实风险是内容基线不同（非分支不同）**：镜像播种内容 ≠ 对方此刻内容（如 clone 了 C0 但对方工作树有未提交改动）→ 对应 seam 的 edit 失败——**响亮报错、只伤一处、自愈**（重新 read 回填 → 重做），详见 5.4。正解不是"两边同分支"，是"镜像从对方真实当前内容播种"（对方从干净已 push 基线开工最稳）。

### 5.3 镜像播种方式（按任务）

| 档 | 接单人侧 | 适用 | 基线 |
|---|---|---|---|
| 惰性 scratch（无 git） | 只落被读到的文件 | adb 调试、排障、小改 | 无需 |
| 完整 clone（共享 remote） | `git clone` + checkout 基线 commit | 研发类 | 对方 HEAD 已 push |
| 完整 bundle（无共享 remote） | 网关中转 repo 打包 | 研发但无 repo 权限 | 打包 commit |

播种必须是 byte-exact。OpenCode 1.17.18 的普通 `read` 返回带行号、不能判定尾部换行且无 remote hash，因此不会用它自动 hydrate mirror；精确 profile 已支持在 Command 输入 `:pull path/to/file`，持久记录 hydration intent 后让客户 Agent 运行 `opencode debug file read --pure`，严格解析 `{content: base64, encoding: "base64"}` 并 hydrate 单个相对路径文件。`content:""` 合法表示空文件，前导 `-` 文件名会改成 `./-...` positional。它要求请求声明 `bash`、开启 exec 且通过客户 Agent 权限确认；这是逐文件 bootstrap，不是完整 clone/bundle 或主动双向同步。base64 tool result 连同历史、schema 和 envelope 必须整体装进统一 `8 MiB` wire budget，过大时 fail-closed；这里没有 `16 MiB` 文件承诺。

### 5.4 保存、交付、漂移与兜底

- **默认交付**：保存后 watcher 自动刷新 review，状态栏提示 `Ctrl+P` preview；人确认后，系统按 exact profile 编码 caller 原生 tool call，由客户 Agent 执行。
- **opt-in auto-send**：`workspace.auto_send=true` / `--workspace-auto-send` 把保存视为发送意图。只有 change-level 安全级别为 `allow` 的 fresh review 自动 preview 并发送；敏感路径、冲突、删除/安全 warning 会停下来。adapter 生成的“原生工具无 CAS”warning 会显示但不阻止这项显式 opt-in；客户 Agent 权限确认仍是最后执行闸。
- **结果回流**：发送前先把 reviewed mutation、exact tool-call ID/digest 与已发送内容作为 delivery intent 持久化；只有匹配该 ID、call digest 与 profile result contract 的成功结果才把 caller baseline 推进到**已发送版本**。若人在等待 result 时又保存了更新草稿，新 diff 会相对新 baseline 继续留在 Review；只有没有 save-ahead 时 Review 才归零。真实 OpenCode 1.17.18 gate 已收到 `Edit applied successfully.` 并完成这一对账；这证明准确确认“发出的版本”，不等于 remote SHA/CAS 证明。
- **漂移源**：对方人手动改文件、对方侧 formatter/hook 改格式、基线脏——都表现为 **edit tool_call 失败**（old_string 匹配不上），自带信号；兜底是重新 `read` 对方该文件真实内容回填镜像、人重做那一处。最佳实践：对方从**干净、已 push** 的基线开工。
- **能力降级**：对方 harness 无文件写入工具（只读研究型 agent）→ 交付通道不可用，退回纯对话，状态栏 `文件✗`。

## 6. 键位（默认）

| 键 | 作用域 | 行为 |
|---|---|---|
| `Tab` / `Shift+Tab` | 全局 | 在 Reply、Command、Tasks 间循环焦点 |
| `PgUp` / `PgDn` | 全局 | 浏览 Chat 上下文；Tasks 焦点也可用 `[` / `]` |
| `Ctrl+C` | 全局 | 退出 TUI |
| `a`、`r` | Inbox（Tasks 焦点、无 active） | 接单 / 拒单选中请求；`Enter` 不接单，`↑/↓` 或 `j/k` 选择 |
| 直接输入、`Enter` | Reply | 输入任意普通文字；发送一个 progress 段并保持人工回合，可连续回复 |
| `Ctrl+J` | Reply / Command | 换行；终端支持增强键盘时也可 `Shift+Enter` |
| `Ctrl+R` | Reply | 发送 clarification/handoff，结束本次 completion 并把控制权交给 Agent |
| `Ctrl+D` | Reply | 发送 final，明确结束当前对话响应 |
| `↑/↓`、`Enter/e`、`n` | Tasks | 选择、编辑、新建 caller Agent 计划项 |
| `Space`、`p`、`d` | Tasks | 切换状态、切换优先级、删除计划项；优先级只会编码到支持它的工具 |
| `Ctrl+S` | Tasks | 把本地草稿作为一个全量 task-list tool call 同步给 caller Agent |
| 直接输入、`Enter` | Command | 生成 caller 已声明的命令 tool call；精确 OpenCode 可用 `:pull path` 精确播种单文件；危险词法需第二次 `Enter` |
| `t` | Tasks | 打开高级声明工具输入：一行一个 `<tool-name> <JSON object>` |
| `Enter` / `Ctrl+S` | 高级工具输入 | 换行 / 一次发送一个或多个 tool call |
| `v` | Tasks | 展开/折叠完整 system、消息、工具目录和 schema |
| `R` / `Ctrl+P` | Tasks | 镜像交付 review / preview；预览后 `Enter` confirm，经 exact caller tool 推送；仅 shim/等价边界提供强 CAS |

## 7. 关键流程

**S1 环境绑定型（adb 调试，回合短、人肉在场）**：从轻量 Inbox 按 `a` 接单 → 读 CHAT → 在 REPLY 用 `Enter` 连续发送进度 → 在 COMMAND 输入已声明命令并 `Enter` → 客户 Agent 执行、result 下一 completion 回来 → 读日志继续决策 → 需要 Agent 继续时按 `Ctrl+R` handoff，完成时按 `Ctrl+D` final。非命令工具用 `[t]` 高级入口。人是这台设备的远程大脑。

**S2 Live Workspace（核心流程，Agent 驱动、人监督）**：按 `a` 接单 → 若镜像为空则在 Command 用 `:pull path` 精确播种所需文件 → 在 REPLY 连续说明 → 编辑 TASKS 并 `Ctrl+S` 调客户计划工具 → 在镜像用 IDE 保存 → watcher fresh review → 默认 preview/confirm，或显式 auto-send 干净改动 → exact profile 生成原生 edit/write → 客户 Agent 执行并回传 result → 按 delivery intent reconcile baseline、保留任何 save-ahead diff → bash 验证 → handoff/final。OpenCode 1.17.18 的 `:pull/edit/result/bash/tasks/final` 以及同 session terminal 后下一 user turn 新 task 已真实通过；完整 TUI 人工操作与真实 harness 网络抖动仍按故障矩阵继续验收。

**发送预览安全高亮**（[02](02-gateway.md) §11）：预览对**越界路径**（网关拒转，红色阻断）、**敏感路径**（`.env`/`.ssh`/密钥，黄色）、**危险 shell**（`rm -rf`/`curl|sh`/`sudo`，黄色）显式高亮。

**拒单/离线**：准入时无人在线 → 200 前返回 overloaded；已推送后按 `r` 拒单或等到 `max_pending` → 阶段 B 流内 error / 断流。两条路径已有仓库内测试；自动 caller 已验证同 key 在 5 次断流后可续传。Codex CLI 0.144.4 黑盒另已证明 500/响应前断 TCP 会重试 5 次且 turn metadata 稳定，但已吐部分 SSE 后是否重试仍待 M0 外部实测。

**caller 断线不是取消**：HTTP/SSE socket 消失不会撤销已经持久化的 admission；同 key 重试可在 `max_pending` 内续接。当前没有独立 caller-cancel 协议，TUI 不应把单纯断网显示为“用户已取消”。

**恢复**：gateway 会保留挂起请求，worker outbox 负责已提交事件的幂等重放，镜像目录也在本地；worker 在 gateway 尚未启动、运行中反复断线或 WebSocket 半开时会退避重连，401/403 则明确终止。同一 worker 进程的重连复用稳定 instance ID；另一个 TUI 使用相同 token 时不会顶替 incumbent，而是显示明确等待状态并有界退避，旧连接释放后自动接管。caller/worker/gateway 三方同时掉线后的 exactly-once 恢复已有仓库内故障注入；Workspace 专项还验证了离线原生 edit、gateway/SQLite 与 worker outbox 重启、并发同 key caller 重放、result continuation 和 save-ahead diff 保留。Codex 已补 Responses Basic 的真实工具闭环，但收到部分 SSE 后的 CLI 重试及 Workspace 故障恢复仍需外部验证。恢复必须发生在 `max_pending` 剩余窗口内；超时后原请求已 `expired`，迟到回复不会使它复活。

普通发送失败会恢复 Reply/Tasks/Command 草稿。默认 `worker.state_db` 还会按 caller/workspace/task/session/tier 持久化这些草稿、rejected draft 与最多 32 个 unfinished continuation；TUI 重启时逐条校验，坏记录隔离，不阻止其它 scope 恢复。状态库写失败会有界退避并保留内存副本，而不是假装已落盘。

若 gateway 因会话已过期而拒绝已进 outbox 的事件，worker 会在同一个 SQLite 事务里把原事件及脱敏 scope 移入有序 rejected inbox、同时应用累计 ACK；独立 dispatcher 可跨重启重放，直到 TUI 首次成功安装草稿并确认，之后只留 digest tombstone。Remote tools / Workspace 可按稳定 scope 跨 completion 恢复；Chat 草稿不因可伪造的 task_id 带入别的请求。镜像交付预览、正在编辑的高级 JSON composer 等**未列入 state DB 的瞬时 UI 状态**仍需重新建立，不能宣称整个屏幕任意时刻无损恢复；达到 32-scope 上限也会淘汰最旧项并明确提示。

## 8. 配置

经 `viper` 加载：配置文件（toml）+ 环境变量 + flag 层叠；CLI 由 `cobra` 提供子命令。

```toml
[gateway]
url = "wss://human.example.com/internal/v1/worker/ws"

[workspace]
mirror_root = "~/mirror"
auto_send = false

[worker]
outbox = "~/.human/worker-outbox.db"
state_db = "~/.human/worker-state.db"
```

远程模式用 `human worker` 启动，worker token 优先通过 `HUMAN_GATEWAY_TOKEN` 注入，不写入配置文件或命令行参数；`worker.state_db=""` 可显式关闭 UI 恢复。本机模式用 `human local`，它把两枚凭据写入 mode `0600` 的项目本地文件，SQLite 仍只保存 hash；`human local credentials --token-only` 只在用户显式调用时输出 caller token。身份显示名、IDE 命令、通知和图片协议仍是产品规格，不应提前写入可运行样例。
