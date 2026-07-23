# 11 · 人侧公共栈：workerkit 与 Web

本页描述当前实现。`human local` 的人侧已经是 `workerkit + web`，不再经过旧
gateway、worker WebSocket 或本地 durable outbox。

## 1. 分层

```text
第三方 UI / 官方 Web
          │  Snapshot + 命令 + Notifications
          ▼
workerkit.Worker
  对话状态机 · Inbox · 草稿 · tool calls · alerts
  ├── Wire port
  ├── StateStore / AlertStore ports
  ├── Mirror port
  └── observe.Observer port
          │
          ▼
llm.WorkerConnection / llm/workerws.Client
          │
          ▼
llm.Service
```

依赖只向下。`workerkit` 不 import Web、CLI 或 `internal/*`；Web 不保存第二份
业务状态；`llm.Service` 不知道浏览器、文件镜像或具体网络协议。

## 2. workerkit：可替换的人侧领域层

`workerkit.Open(ctx, Config)` 返回一个 headless `Worker`。UI 只需要：

- `Snapshot()`：Inbox、Conversation、Review 和未 dismiss 的 Alert 的深拷贝；
- `Notifications()`：coalescing 的“重新读取 Snapshot”提示；
- `Accept/Reject/Reply/Clarify/Final/SubmitToolCalls` 等串行命令；
- Mirror 的 pull、review、confirm、discard 与 result settlement 命令。

主要 ports：

| Port | 官方基础实现 | 宿主可以替换为 |
|---|---|---|
| `Wire` | `WrapConnection`（进程内）、`llm/workerws.Client` adapter | 自有队列、消息总线、测试桩 |
| `StateStore` | memory、`workerkit/sqlite` | 自有数据库或服务 |
| `AlertStore` | memory、`workerkit/sqlite` | 任意 durable alert store；这是可选扩展，不扩大最小 StateStore |
| `Mirror` | `workerkit/fsmirror` | 远程 IDE、虚拟 FS、禁用镜像 |
| `Observer` | nil/no-op、`observe.NewSlog` | metrics、trace、日志 adapter |

Store 接口是行为合同，不是“必须使用 SQLite”的别名。官方 SQLite 只是基础实现；
自定义实现保有自己的生命周期，除非宿主显式把 release ownership 交给上层。

### 2.1 两个用户、两个文件系统

Agent User 与 Human User 是两个 principal，也默认位于两个互不挂载的文件系统：

- Agent User 的 cwd 只属于 Agent/harness，不进入 HumanLLM Task 或 wire；
- `fsmirror.Config.Root` 是 Human User 自己选择并拥有的基础目录；
- `WorkspaceScope{Caller, WorkspaceKey}` 只是两者之间的不透明路由身份，不是路径映射。

`fsmirror` 只读取 Human 选择的目录。原生工具 builder 发送项目相对路径；Agent harness
在自己的 cwd 与本机路径语义下解析，因此 Human 不需要知道 Agent 是 POSIX 还是 Windows。
每个 filesystem mirror 必须绑定完整 scope，每个会话还绑定独立 `HumanWorkspace.ID`；
跨 caller/workspace 或跨会话投递都返回 `ErrMirrorScopeMismatch`。

## 3. durable 与 advisory 边界

- assignment/event 的正确性真相在 `llm.Service` Store；远程连接时由
  `llm/workerws` Journal 保护发送事件。
- 已接受 Conversation、草稿、parked tool calls、Alert 在 StateStore；本地默认
  使用 `workerkit-state.db`。
- Inbox 不重复持久化。未确认 assignment 由 transport 重投；同时写进
  StateStore 会制造两个真相。
- caller 断开是 advisory notice，但带 exact `caller/task/request` 身份；它只能
  影响匹配的请求，不能把同一 Task 的新 continuation 标成已断开。
- `request_expired` 对应的 core 决定已经 durable。workerkit 会从 Inbox 移除精确
  assignment，或把精确当前 Conversation 收成 terminal；迟到的旧 NACK 不会改写
  新 request/lease。
- Alert 的保存和 dismiss 在支持 `AlertStore` 时都是 durable；重启后恢复。

## 4. 官方 Web

`web.Server` 是 workerkit 的 HTTP 投影：

- `GET /api/state` 返回版本化的 Human 投影（当前 `schema_version: 2`），不是内部
  `Conversation` 的无差别序列化；
- `GET /api/events` 通过 SSE 推送状态更新并发送 keepalive；
- `POST /api/accept`、`/api/reply`、`/api/final`、tool/review 等端点只调用
  workerkit 命令，不复制状态机；
- active Conversation 只有收到 exact `caller_gone` 后才允许 abandon；
  awaiting-caller/results 仍可显式结束；
- 页面直接支持 Inbox、对话、progress/final、结构化 tool call、文件 review、
  caller-gone/expiry alert 与 durable dismiss。

Web 投影只暴露不透明 `workspace_scope`、Human 侧会话目录和必要的对话状态，不发送
harness session 或内部 `Assignment`。Agent 的 cwd/绝对路径根本不进入 Task、Store 或
worker wire。文件 builder 只生成项目相对路径，真实工具由 Agent 在自己的 cwd 与权限
系统中执行。

关闭浏览器不会停止 Worker。daemon 拥有 Store、Mirror 和连接；浏览器随时可以重新
打开并从 Snapshot 恢复。多个浏览器可读同一状态，所有写命令仍由 workerkit 的单一
命令锁串行化。

## 5. 本地组合与安全

`local.Open` 是这套 ports 的 batteries-included 实现：

1. `llm/sqlite` + 三个 built-in codecs 创建 `llm.Service`；
2. `callerhttp` 暴露 Chat Completions、Responses、Anthropic Messages；
3. 一个进程内 `llm.WorkerConnection` 经 `workerkit.WrapConnection` 进入人侧；
4. `workerkit/sqlite`、`fsmirror` 与 `web.Server` 组成浏览器产品。

模型端和 Web 端都只允许 loopback。模型端使用单 caller token，同时接受
`Authorization: Bearer` 和 Anthropic `X-Api-Key`；二者重复或冲突时 fail closed。
Web 每次启动生成独立 session token，只出现在打印的登录 URL；caller token 不进入
浏览器。若部署到远程网络，TLS、SSO、代理信任和 header 剥离由宿主负责，不能直接把
loopback session token 当成公网认证方案。

本地 human side 不需要 worker token，因为它与 Service 同进程；真实工具也从不在
Human 进程执行。人只返回模型 tool call，调用方 Agent 在自己的工作区和权限系统里
执行，再把 result 放进下一次模型请求。

`human local --workspace` 只配置 Human 侧基础目录。每个 exact harness session 默认映射
到稳定的 `session-<hash>` 子目录；原始 session id 不直接用作文件名。Human 可在 Web
中把某个会话切换到已有 repo，选择会持久化，重启后恢复。切换目录会以新目录当前内容
重新种 baseline，因此不会把已有 repo 全部当作 create；之后的修改才进入该会话的
Review。不同会话的 change 带独立 workspace id，跨会话交付在 workerkit 与 fsmirror
两层 fail closed。外接盘或 restore 后目录暂时不存在时，daemon 仍可启动并把目录标成
unavailable，Human 可从 Web 选择替代 repo。

## 6. 生命周期与可观测性

`local.Close` 先取消 handler/expiry/retention，再关闭 caller transport、Web、Worker、
Mirror、StateStore，最后关闭拥有 Store 的 Service。`workerkit.Worker.Shutdown` 会停止
notice pump，不留下等待一个仍存活连接的 goroutine。

`observe` 当前覆盖 admission、worker connect/disconnect、event settlement、expiry、
retention、assignment、human command、NACK 和 alert。Observer 在 Store callback 之外
调用；panic 被隔离，不能改变业务结果。Prometheus/OTel 仍是宿主可自行添加的薄 adapter，
不是 core 正确性依赖。

## 7. 已验证边界

- 真实 OpenCode 1.17.18 两轮 resume 已经通过 `human local` 公共组合；
- 浏览器 API 与 Playwright 门覆盖 accept、progress/final、tool loop、review 和断连；
- workerkit fake-wire/SQLite 测试覆盖重投、NACK、重启恢复、alert 持久化和 abandon；
- OpenAI Chat、Responses、Anthropic Messages 的 aggregate/stream 都有默认产品门：
  真实请求进入 `human local`，再只经 Web API 接单并完成；
- Codex 0.145.0 已经在公共 local 栈真实执行 `exec_command`、回传
  `function_call_output` 并收到 Human final；在请求声明 exact Responses custom freeform
  `apply_patch` 时还完成 mirror create/modify 和 caller 最终字节核对。Claude Code
  2.1.217 也已通过 Messages + Web 的 `Bash` 成功/失败 result 回流与恢复 final。
  具体 harness 的 Workspace 能力仍按实际 profile/schema 单独声明，不能因协议或普通工具
  闭环就外推成所有高级功能都已验证。
