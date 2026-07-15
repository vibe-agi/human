# 二期 · 异步交付模式（人 = Agent）

> 一期是**同步模式**（人 = 模型，见 [01-goals.md](01-goals.md)）。本篇是第二形态——人作为独立 **Agent** 异步接单，在 git worktree 里深度工作、以 diff 交付。设计输入包括桌面推演、TLA+ 有限状态模型检查（[../formal/](../formal/)，五项 TLC 实验如预期）与开源调研。当前 `internal/delegation/` 状态权威、官方 A2A transport、worker/worktree/rewind、累计 patch、`human-mcp` apply 与 remote exec 已有 Go 实现和仓库内端到端测试；未建模通道、外部 A2A/MCP harness、长期运行与产品体验仍须实测。

## 0. 为什么是二期，不是一期

一期讨论中反复出现同一模式：文件就地变化、实时同步、远程命令、Esc 中断、Esc Esc 回滚、排队——**每一项在"人=模型"形态下都由对方 harness 免费获得**，而本套设计要为它们各建一套机制，工程量大一个数量级。本套的独特价值在**异步深度工作**：接单后自主安排数小时、diff 交付、baseline commit 作验收凭证。这是二期的事；一期的同步模式以小得多的成本覆盖了交互/环境绑定型场景。两形态最终并存，架构不互相堵死。

## 1. 组件与三条硬规则

| 组件 | 形态 | 职责 |
|---|---|---|
| `humand` | A2A 服务端 | 基于官方 SDK 暴露 Agent Card、Bearer 边界、JSON-RPC 与 HTTP+JSON 的 task/message/list/cancel 核心，并投影独立 SQLite authority。**不懂 git**；当前未宣称 push notification。 |
| `human` | 接单人入口 | 当前以 `human delegation` CLI 完成 watch/tasks/accept/deliver/rewind/complete 与 worktree 流程；完整 todo/图片工作台仍是产品规格。 |
| `human-mcp` | MCP server（stdio，跑需求方本地） | 派单、状态、回复/取消、取累计 patch 并校验/apply；远程命令工具仅显式启用时注册。 |

三条硬规则（保证协议升级/多项目扩展不用大动）：**① humand 不懂 git**（只做协议/状态机/队列/存储，patch 对它只是字节）；**② 所有 git 操作都在拥有仓库的机器上**（接单侧 worktree/commit/diff 在 TUI，需求方侧 apply 在 human-mcp）；**③ TUI 不对外说 A2A**（对外协议收敛在 humand）。

调用方接入靠 **MCP**：`human_delegate` / `human_status` / `human_result(apply=true)` / `human_reply` / `human_cancel` / `human_tasks` 已实现；启用 remote exec 后再注册 `human_exec_pending/approve/deny`。human-mcp 跑在需求方本地，因此由它在 caller 权限下把 patch apply 到需求方工作区——"文件就地变化"的实现点。具体第三方 MCP harness 仍待外部试点。

## 2. 核心模型：task = PR

| 概念 | git 实体 | 类比 |
|---|---|---|
| Task（一次委托） | 分支 `human/<task-id>` + 专属 worktree | PR 分支 |
| Turn（一轮） | ≥1 个 commit | 一次 push |
| 交付 | `git diff --full-index --binary base...HEAD`（三点） | PR 的 Files changed |
| 完成归档 | 预检后固定 final commit、保留 branch/keep ref、归档并移除 worktree | PR 关闭并保留审计锚点 |

worktree 粒度是 **task 不是轮次**（多轮共享同一工作现场，每轮产出以 commit 沉淀）。当前默认是 no-merge 的 patch 交付；Complete 只执行一次终态预检、归档与 worktree 清理，不替 caller 合并分支。

**状态机**（A2A 标准状态直接映射）：`submitted`（排队未接）→ `working`（接单，建 worktree）→ `input-required`（结束一轮，等对方）→ `working`（对方追问）… → `completed`/`canceled`/`rejected`/`failed`。

**worker 命令原子性（已实现）**：每条命令以 `event_id + worker_id + task_id + command_digest` 对账；领域状态变更与编码后的结果回执在同一 SQLite 事务内提交。稳定的领域拒绝会回滚命令副作用，但仍持久化同一拒绝回执；基础设施错误则整体回滚。因此断线/崩溃后的精确重试只重放原回执，不会再执行一次 Accept/Deliver/Exec/Complete/Rewind。

**消息分级**（working 中按 `metadata.intent`）：`message`（steering，安静排队）/ `interrupt`（≈ Esc，强提示要求人立刻关注）/ `rewind`（≈ Esc Esc，携带 `to_turn`，见 §5）。

**完成判定权**：`completed` 是 A2A 终态、之后不能再收消息，而"活干完没干好"由 caller 判断——故接单人默认停 `input-required` 等表态。新 task 的 `referenceTaskIds` 当前会经协议透传；worker 侧根据引用从旧分支顶点复活工作现场尚未实现。

## 3. 交付：累计 patch + 五级 apply

**artifact = 累计 patch**（`base...HEAD` 三点 diff），**replace 语义**：每轮全量替换，caller 手里永远是最新累计 patch，可独立 apply，无需叠加历史。实现生成 `--full-index --binary` patch；metadata 带 `{base_commit, turn, files:[{path, blob_sha, mode}]}`，其中 `mode` 覆盖普通文件、可执行位与删除。caller 同时校验 blob hash 与 mode，mode 也进入 artifact CAS，避免“内容相同但权限漂移”静默成功。

**apply 链（human-mcp，五级，由调研定型）**：

0. **前置 dirty-commit**（Aider 式，可关）：把「patch 触及 ∩ 当前脏」的文件先 pathspec 限定 commit——结构性消除 apply 冲突主因，且需求方手工改动永远有 committed 副本可找回；
1. **revert + apply 累计**：`apply -R` 旧累计 → `apply --3way` 新累计。安全门（Aider `/undo`）：已 push 到 origin 的绝不 revert，走第 2 级；
2. **直接 apply 增量**：revert 失败多因 caller 已 commit 中间交付——改用本轮增量 patch 直接 apply；
3. **mergiraf 结构化合并**（PATH 探测到才启用）：`-c merge.conflictStyle=diff3` 重试，对 index stage 1/2/3 三方全文逐文件 `mergiraf merge`（语法感知，46 语言）——成功则采纳 + 强制标注、留冲突版审计副本；
4. **结构化冲突报告**（jj 思想）：全败 → 不动工作区，产出逐文件逐 hunk `{base, ours, theirs}` + 侧标签——冲突升级为可延后、可指派回下一轮的一等状态。

任一级成功后逐文件比对实际 blob hash **和 Git mode** 与 `metadata.files`（**X-04 运行时校验**）：一致才算成功。实现还校验 artifact/task/workspace 绑定、累计与增量 CAS、路径全集，并在失败时回滚；这提供明确的运行时 oracle，但不把未知第三方行为包装成形式化证明。

## 4. baseline 对齐与 git 工作流

**当前基线获取**：显式配置 `shared_remote` 时，接单侧 fetch `base_commit` 后创建 worktree；未配置时，要求该 base commit 已存在于本地对象库。此前设计的“无共同 remote 时交换 tree fingerprint 并自动降级到 patch-only”尚未实现，不能作为当前互通保证。

**worktree 生命周期**：接单 → 按显式配置 fetch base_commit → `worktree add … -b human/<id>` → 可选 setup argv（默认不执行任意 hook）→ 人在自己 IDE 干活 → 结束一轮流水线（异常扫描秘钥/大文件 → 保留人工的 ≥1 commits 或生成安全提交 → full-index/binary diff → 发送）→ 对方追问下一轮 → Complete 收尾。**三点 diff（`base...HEAD`）** 保证任务期间 base 前进、或人主动 merge 上游，都不把上游改动混进交付 patch。

**仓库体检与引导（产品规格，尚未实现自动体检）**——后续 TUI/CLI 应在首次使用时尽早检查 baseline：

| 现场 | 引导（全需人确认） |
|---|---|
| zip/祖传代码（无 .git） | `git init` + 按栈生成 .gitignore + **初始 commit = 甲方原始交付状态（验收凭证）** |
| init 过但零 commit | 补初始 commit（worktree 需至少一个 commit） |
| 工作区脏 | 展示改动，人选 commit 成 baseline / 自行整理 |
| detached / shallow / submodule | detached 允许（base=当前）；shallow 直接支持；submodule 警告（自动化进 backlog） |

影子仓库（`GIT_DIR` 分离、目录零 .git 侵入）降级为 backlog，仅服务"合同禁止目录出现 .git"的极端场景。

**引用管理**（`refs/human/`，源自 Cline v4 与 jj 实战教训）：`wip/<id>/<n>`（轮内 WIP 快照，进行中工作的保险）、`backup/<id>/turn-<M>`（rewind 备份）、`keep/<id>/*`（**防 GC**——被 rewind 滚掉的 commit 不在任何分支上，无它会被 `git gc` 回收）。四规则：**① 破坏性操作前强制快照**（含 untracked 的零副作用临时 index 法：`GIT_INDEX_FILE=tmp git add -A && write-tree && commit-tree`，不用裸 `stash create`——会丢 untracked）；**② 恢复纪律**（reset/checkout 前先 `cat-file -e` 验证目标存在；**绝不 `git clean -fd`**）；**③ 快照失败降级警告、永不阻塞交付**；**④ commit 卫生**（自动 commit 一律 `--no-verify` + `gpgSign=false`；消息编码 `human: task-<id> turn-<n>`；author=接单人本人、系统身份走 committer/trailer）。

**task 完成收尾（已实现，防双路回流）**：默认 patch 交付模式**不 merge 不 push**。`Complete` 先做无副作用预检：worktree 必须存在且干净、HEAD 必须等于最新交付锚点、本地 turn 必须与 authority 一致；预检失败不会把 authority 写成终态。通过后 authority 才进入 `completed`，随后 final commit 固定到 `refs/human/keep/<task>/completed`、写 archive record、移除 worktree并保留 `human/<task>` branch。若 authority 已提交而归档清理失败，精确重试只完成幂等归档，不重复终态转换。

## 5. rewind：精确时间旅行（Esc Esc 对应物）

turn ↔ commit 内容寻址 ⇒ `rewind(N)` 后的配置与"第 N 轮刚交付"的真实配置**逐字节相等**——Claude Code 需影子仓库快照才能做到的，我们因 turn=真 commit 天然拥有。人确认后 TUI **三步执行**：① 当前 worktree `add -A + commit`（进行中工作落库）→ ② 分支顶点打显式 backup ref → ③ `reset --hard <turn-N-commit>` + 对话截断。悔滚 = reset 回 backup ref。

**是 humand 上的一次事务，非两端各自回滚**：请求到达 → `rewind-pending`（等人确认）→ 人确认 → humand 原子生效（截断锚点、被回滚轮次的消息/artifact 标 `superseded`——审计完整保留、绝不物理删除）→ 广播生效事件；人拒绝 → 广播拒绝（附理由）。两端只认生效事件，`tasks/get` 全量对账——**humand 是唯一权威源**。对 caller 侧根本不存在"回滚"这个特殊操作：交付一直是 replace 语义，回滚只是"最新累计 patch 变成 turn N 那份"，human-mcp 照常 revert+apply。

**对模型 Esc 是强制、对人是请求**：人保有否决权；git 回滚不了世界副作用（已跑的 migration/部署/外部调用），这正是人应当拒绝并说明的时刻。

## 6. remote exec 子协议（人操作需求方机器，如 adb）

**动机**：patch 对应 Read/Edit，本子协议对应 Bash——adb 调试（设备在需求方侧）、内网库、"只在你环境复现"。实现把它作为 `working` 内部的持久请求-响应，不改变 task 状态：人发 `command_request{request_id, command, cwd?, timeout?, reason}`（reason 必填）→ human-mcp 授权 → 有界执行（超时、输出截断）→ `command_result{exit_code, stdout, stderr}` 按 request_id 路由回，**不进 steering 队列**。无 PTY；请求/结果进入 delegation 事件与 caller 侧 exactly-once ledger。

**当前授权边界是双重显式开关 + 逐条决定**：服务端必须以 `humand serve --remote-exec` 开启，才接收 worker command request 并在 Agent Card 声明能力；caller 本地还必须以 `human-mcp --allow-remote-exec` 开启，才注册 `human_exec_pending/approve/deny` 和执行器。两端任一开关关闭都不能执行；开启后命令仍先 pending，须对具体 request 调用 approve（或 deny），执行账本按稳定 ID 防响应丢失后的重复执行。模式白名单/`exec_policy` 自动预授权仍是后续产品策略，不应写成当前默认能力。

## 7. 形式化模型（可执行版见 [../formal/](../formal/)）

三条受检性质：**C1 一致性**（在 apply/hash 校验 oracle 正确的前提下，已交付版本不会变成幽灵版本）、**C2 stale-pull 恢复**（fetch 后权威变化时仍可重新拉取并收敛）、**C3 进展**（公平性下最终到终态）。

**三个结论**：**① replace 语义的无记忆正确性**——正确性条件只引用最新 artifact、不引用历史；当前模型检查的是原子 pull 后遇到 stale authority 仍能收敛，消息丢/重/乱序需另建 channel/seq/ack 模型，不能由本模型代证。**② fail-explicit 有前提**——模型只证 applied/inflight 版本曾被交付；"本应冲突却静默报成功"是否可达取决于运行时 apply/hash oracle（X-04），不在模型可见状态内。**③ safety 与人无关、liveness 才与人有关**——同一可达状态空间内的 safety 检查不依赖人类公平性；去掉人类公平性后 `EventuallyTerminal` 出现 stuttering 反例。

**TLC 结果**（2.19，`formal/`，`./run-checks.sh` 可复现）：主配置全过 **3,486 状态**穷举；放大到 4 轮/2 回滚/2 本地修改 **54,478 状态**仍全过；去人类公平性的单-property 配置中 `EventuallyTerminal` 如预期出现 stuttering 反例；两个注 bug 的 mutant 均被不变量当场抓获。终态含 `failed`（`Fail` 转移）。**这是有限状态下的协议核心模型检查，不是完整证明**。**边界**：验协议非实现（故需 X-04 运行时校验兜底）；`applied` 只代表最近验证的 artifact 版本，不证完整工作树相等；公平 `CallerFetch` 抽象掉了 `human_result(apply=true)` 的显式意图；模型只含原子 pull 的 stale read，不含消息 channel、seq/ack、crash/restart 或持久化恢复；humand↔TUI 的 WS 丢/重/乱序也未建模。

## 8. 调研：12 项设计变更（R1–R12）

4 路开源调研（均 clone 研读源码，mergiraf/git 行为本机实测）：

| # | 变更 | 来源 |
|---|---|---|
| R1 | apply 前置最小范围 dirty-commit | Aider |
| R2 | apply 链加 mergiraf 结构化合并级（本机实测通过） | mergiraf |
| R3 | 冲突升级为结构化对象（hunk + 侧标签） | jj（git index stages = 穷人版 Merge） |
| R4 | revert 安全门：已 push 的绝不 revert | Aider `/undo` |
| R5 | 接单侧轮内 WIP 快照（含 untracked 临时 index 法） | Cline v4 + jj |
| R6 | `refs/human/keep/*` 防 GC（修正真实漏洞） | jj `refs/jj/` |
| R7 | 恢复前 `cat-file -e` 校验、绝不 `clean -fd`、失败不阻塞 | Cline v4 |
| R8 | commit 卫生：`--no-verify`+`gpgSign=false`、编码 task/turn、author 归属 | Cline+Aider |
| R9 | patch 用 `git diff --full-index`（否则 `--3way` 失败，实测） | mergiraf 线 |
| R10 | 审计事件挂 view 快照（op-log 思想，为全局 undo 预留） | jj op log |
| R11 | SDK 定案：a2a-go v2.3.1 + MCP go-sdk v1.6.1 直接用 | SDK 线 |
| R12 | merge 助手可配 mergiraf 为 merge driver | mergiraf |

**明确不采纳**：Cline v3 shadow git（嵌套 .git 重命名 hack 与"人在现场"不容）、jj 本体/N 树冲突（vanilla git 兼容硬约束）、隐式全局 merge driver、rerere/union merge、trpc-a2a-go。

**SDK 实现事实**：humand 使用 **a2a-go v2.3.1**，以项目自己的 SQLite `delegation.Store` 为唯一权威，通过 SDK `RequestHandler` 投影官方 Agent Card、JSON-RPC 与 HTTP+JSON；Bearer 中间件隔离 caller。human-mcp 使用 **MCP go-sdk v1.6.1**（stdio + 泛型 AddTool + progress notification），通过 Agent Card 解析官方 A2A client。当前 A2A card 不声明 streaming/push notification，客户端以 task get/list 对账。

## 9. 里程碑与非目标

**实现落点**：P2-M0/M1 的 monorepo、SQLite authority 与官方 A2A，P2-M3 的 worktree/累计 patch/恢复，P2-M4 的 human-mcp apply，以及 P2-M5 中的 rewind 与 remote exec 核心均已落地并有仓库内测试。完整异步 TUI、图片/附件、todo/通知、外部 harness 试点与长期可靠性仍未完成；因此不按代码清单宣称整个 P2-M5 产品门通过。

**非目标**：多项目路由、多接单人调度/市场计费、OpenAI-compat 网关（那是一期）、影子仓库、并行接单、企业级多租户。

**验收 demo**：仓库内 E2E 已跑通 MCP delegate → worker 接单/worktree 多轮交付 → `human_result(apply=true)` 令 caller 文件就地变化，以及回复/rewind/remote exec 的核心协议。仍需用一个真实外部 MCP harness 和真人工作流重复该 demo，补 todo/通知等体验后，才能作为产品验收。

## 10. 一期可复用

两种模式已在同一 monorepo 并存：`completion` 负责被实时调用的模型兼容面，`delegation` 负责异步 A2A 权威；共享 CLI/鉴权/SQLite 等基础设施，但保持状态机与 worker 协议分离。后续可复用 UI 组件，不能把两个领域包重新折叠成 `phase1/phase2` 大包。
