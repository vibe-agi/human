# 11 · 人侧栈设计:workerkit 与 Human Web

本页是设计文档,不是现状描述。除 §0 与 L0 外,本页所有能力均**未实现**;
实现顺序与验收门见 §7。

## 0. 问题(现状确认)

人这一端目前是单一产品,不是可复用的库:

- 官方 TUI 经 `worker.Open → internal/workerclient` 使用 `internal/workerproto`
  的 WebSocket 方言,只能连接 legacy 产品 gateway;它不认识公共 `llm/workerws`
  协议。嵌入 `human.NewLLM` 的宿主因此**没有任何官方人侧客户端可用**。
- TUI 内部的注入点(`WithMirrorManager`、`WithStateStore`)都在 `internal/`,
  第三方无法注入自己的实现;Live Workspace 的全部正确性逻辑
  (save-ahead 交付链、拒单 finalizer、continuation 停放)同样锁在
  `internal/mirror` 与 `internal/tui` 里。
- durable worker 状态(outbox、mirror、草稿)与终端屏幕耦合在同一进程:
  专家必须常驻终端,关掉终端 UI 与 worker 一起消失。
- 公共侧已有的是 `llm/workerws.Client`:WS 连接、instance identity、重连
  退避、durable Journal/outbox、assignment/event/ACK/NACK。缺的是它之上的
  领域层和 UI。

## 1. 目标与非目标

目标:

1. 人侧成为公共、分层、可替换的栈:第三方可以 (a) 直接用官方 UI,
   (b) 只换 UI、复用领域层,(c) 只用 wire client 全自建。
2. **Web 是第一公民 UI**:worker daemon 与浏览器分离,专家不常驻终端,
   移动浏览器可用;daemon 拥有全部 durable 状态,UI 只是投影。
3. 该栈只依赖公共 `llm` 内核与 `llm/workerws`,因此它同时是双栈收敛
   ([06 §架构决策](06-product-todos.md#架构决策方向性不进入本轮验收))
   worker 半边的落地路径,不是额外的第三套实现。

非目标:

- 不做移动原生 app;Web 响应式即可。
- 本设计不含 HumanAgent 的人侧装配;workerkit 的分层完成后另行复用。
- **不维护两个官方 UI。** TUI 的终态是删除,不是迁移:它从现在起冻结
  (只修 bug,不加功能),在 web 通过同一真实 OpenCode 产品门之前继续作为
  RC 的已验收产品面;过门之日,TUI、`internal/tui`、`internal/workerclient`
  与旧 worker WS 方言整体删除。不存在"迁移 TUI 到 workerkit"的中间工程。

## 2. 分层架构

```text
┌──────────────────────────────────────────────────┐
│ L3  第三方 UI / 集成(Slack、工单系统、自研桌面) │ ← 消费 workerkit 公共 API
├──────────────────────────────────────────────────┤
│ L2  官方 UI adapter                              │
│     human web(唯一官方 UI;TUI 冻结待删)       │
├──────────────────────────────────────────────────┤
│ L1  workerkit(新公共包,headless 领域层)       │
│     接单/回复/final 状态机 · 草稿 · continuation │
│     停放 · tool-call 构造 · Mirror/StateStore/   │
│     Notifier/Observer ports                      │
├──────────────────────────────────────────────────┤
│ L0  llm/workerws.Client(已有)                  │
│     WS 连接 · instance identity · 重连退避 ·     │
│     durable Journal/outbox · ACK/NACK            │
└──────────────────────────────────────────────────┘
```

依赖只能向下;L1 不 import 任何 UI 或 `internal/*`,L2/L3 不直接触碰 L0。

## 3. workerkit(L1)

形态:事件驱动的领域对象,不渲染。`workerkit.Open(ctx, Config) (*Worker, error)`。

对 UI 的 API 是三件套:

- **命令**:`Accept/Reject/Reply(progress)/Clarify/Final/SubmitToolCalls/`
  `PullFile/ConfirmDelivery/DiscardChange` 与 Tasks 计划工具的 CRUD。
  每个命令绑定稳定 scope(task/session),并发调用串行化。
- **订阅**:`Events() <-chan Event` —— `AssignmentArrived`、`ReviewUpdated`、
  `DeliverySettled`、`RejectionArrived`、`ConnectionChanged` 等 typed 事件。
- **快照**:`Snapshot() State` —— inbox、活动任务、对话记录、pending review、
  各命令当前可用性。UI 崩溃/重开只需重放快照 + 订阅。

Config ports(全部可替换,官方实现只是 adapter):

| port | 职责 | 官方 adapter | 第三方可换成 |
|---|---|---|---|
| Wire | assignment/event 传输 | `llm/workerws.Client` | in-process 测试桩、自有协议 |
| StateStore | 草稿、parked continuation、review 基线 | sqlite | PostgreSQL、自有服务 |
| Mirror | Live Workspace 文件镜像与 review | fs + fsnotify | 虚拟 FS、远程 IDE |
| Notifier | 新单/review/结果通知 | no-op / 桌面 | Web Push、webhook、IM |
| Observer | 结构化运行事件 | slog | Prometheus 等(见 §6) |

正确性语义整体从 `internal/tui`/`internal/mirror` **搬迁**而非重写:
save-ahead 交付链(exact pending row → delivery intent → intent-recorded
phase → durable outbox)、崩溃恢复冻结原字节、拒单 finalizer 与 tombstone、
最多 32 个 parked continuation、成功 result 只推进已发送版本——现有故障注入
测试随语义一起迁到 workerkit 层,作为其 conformance 的一部分。

边界不变:workerkit 不执行客户命令、不挂载客户工作树;客户 Agent 仍是唯一
执行现场。

## 4. human web(L2)

### 进程模型 —— 与 TUI 的本质区别

```text
human worker --web 127.0.0.1:19081        # 或 human local --web
┌── worker daemon(常驻)──────────────────┐
│ workerkit:outbox · mirror · 草稿 · 状态 │
│ 内嵌 HTTP:静态 SPA + SSE 推送 + 命令 POST│
└──────────────────────────────────────────┘
        ↑ 浏览器 / 手机浏览器(随开随关,零持久状态)
```

durable 状态全部在 daemon;关浏览器不丢任务、不断连接。多个 UI 会话可同时
读同一 daemon,写命令统一串行进 workerkit(单专家单 worker 身份不变)。

### 安全

- 本机默认只听 loopback,启动时生成一次性 UI session token(打印为可点击
  URL);浏览器只持有 UI session,**worker bearer token 永不进浏览器**。
- 远程/团队部署由运维放置反向代理 TLS 与自己的 SSO;web 层暴露与
  `callerhttp.Authenticator` 同形态的认证 port,示例中的受信 header 只是
  扩展点演示,不是生产认证(与 [07](07-embedding.md) 同一措辞与边界)。

### 信息架构

对应 TUI 四区,为非终端用户重排:

- **收件箱**:待接单队列,含等待时长;接单/拒单一键操作,浏览器
  Notification(可选 Web Push)在页面不在前台时提醒。
- **任务工作台**:左侧对话流(progress 草稿、clarification、final,
  Ctrl+Enter 发送);右侧三个可折叠面板——Workspace Review(逐 change 的
  diff 与安全级别,confirm/discard)、Tasks(计划工具)、Command
  (caller 声明兼容时启用,含 `:pull`)。
- 危险命令的二次确认、部分可交付批次必须人工确认等既有安全交互原样保留。

### 技术选型

- 后端:daemon 内嵌 `net/http`,SSE 推状态、短 POST 发命令;`go:embed`
  静态资源,单二进制交付,无 Node 运行时依赖。
- 前端:轻量方案(htmx/Preact 量级),严禁外部 CDN;diff 渲染是唯一重组件。
- web 层**零业务状态**:一切来自 workerkit 的 Snapshot/Events,不产生第二套
  状态语义。

## 5. 第三方注入点总表(设计完成后)

| 第三方想做什么 | 用哪一层 |
|---|---|
| 直接用现成人侧产品 | `human web` |
| 自己的 UI(Slack bot、工单、桌面) | workerkit 命令/订阅/快照 API |
| 换草稿与状态存储 | `workerkit.StateStore` + conformance |
| 换镜像实现(远程 IDE、虚拟工作区) | `workerkit.Mirror` |
| 含状态机全自建 | `llm/workerws.Client` |
| 换传输(非 WebSocket) | `llm.WorkerTransport` |

## 6. Observability(同批补洞)

- 新公共 `observe` 包:`Observer` 接口 + typed 事件(admission 结果、
  assignment 生命周期、delivery settle、重连、outbox 深度、review 周期)。
- `llm.Config` 与 `workerkit.Config` 各加 `Observer` 字段,nil = no-op;
  官方提供 slog adapter,Prometheus/OTel adapter 留给宿主(事件已 typed,
  实现是薄层)。
- 契约:Observer 调用发生在 Store callback 之外,不得阻塞正确性路径;
  慢/坏 Observer 只丢事件,不失败业务操作。

## 7. 里程碑与验收门

| 里程碑 | 内容 | 状态(2026-07-19) |
|---|---|---|
| M0 TUI 冻结 | TUI 与 `internal/tui`/`internal/workerclient` 只修 bug,不加功能 | **完成**:三个包已带冻结声明 |
| M1 workerkit-core | 接单/回复/final/工具循环 + StateStore conformance + fake wire 故障注入 | **完成**:公共 `workerkit` + memory/sqlite StateStore + `humantest.TestWorkerStateStore`;fake-wire 覆盖重投去重、发送失败重试、拒单顺序、NACK 先持久后确认、32 上限 fail-closed、重启恢复;并以进程内 adapter 驱动真实 `llm.Service` 通过 chat 与 workspace 工具续接闭环 |
| M2 human web MVP | web 包 + SSE + 会话 token + 嵌入式单文件 UI(en/zh,默认 en) | **完成**:真实 OpenCode 1.17.18 Basic 门通过——CLI 连嵌入内核,人侧全程 web HTTP API,final 逐字回流;另有 Playwright 驱动真实 Chrome 操作页面、GLM 类 LLM 模拟人类专家的浏览器门 |
| M3 Mirror port 化 | `workerkit.Mirror` port + 官方 `fsmirror` adapter + web Review 面板 | **完成**:baseline 仅在成功 result 回流时推进、失败保持 pending、可选 BaselineFile 跨重启;工具映射 builder 留在宿主侧待 Harness SPI |
| M4 web 门对齐 → 删除 TUI | web 版真实 OpenCode 门逐项对齐 TUI 门;随后删除 TUI 栈 | **门侧已达成用户流程对齐**:Workspace 档真实门通过——accept、流式回复、`:pull` 字节级 hydration(envelope 解码)、镜像 save→review→confirm、原生 `write` 交付、result 续接、bash+todowrite 批、final、aux chat 隔离,工作树逐字节正确。**删除仍按闸待执行**,见下 |

**产品迁移已落地**:`internal/workerbridge` 把 legacy worker WS 方言桥接为公共
`workerkit.Wire`(确定性桥接 ID 去重重投、ConfirmAssignment ↦ 幂等 accepted
事件、SendEvent 绑定 durable outbox、NACK inbox ↦ Rejections/ConfirmRejection);
`human local --web <loopback>` 用它装配 workerkit + web 替代 TUI:独立 loopback
listener、每次启动一次性 session token、独立 `workerkit-state.db`、Close 逆序
拆除。真实 OpenCode 门已通过:CLI 连真实 local 产品(legacy gateway + 签发凭据 +
真实 WS + durable outbox + 桥),人侧全程 web HTTP API,final 逐字回流。

M4 删除闸的剩余项(不满足前不删):

1. web 门尚未覆盖:同 session 第二 user turn(`--session`)、local web 装配下的
   Workspace 档真实门(mirror/fsmirror 尚未接入 `--web` 装配,exact profile 的
   :pull/preview/confirm 在产品路径未重演)、`REAL_COUNT=3` 连续重复、TUI 门的
   save-ahead journal 崩溃细节(workerkit 层有等价故障注入,未在真实门重演)。
2. `human worker --web`(远程拆分模式)尚未提供 web 装配;桥当前只由 local 组合。
3. 删除本身是大规模机械 churn(worker/local/humancmd 重装配 + 数万行测试随删),
   须与 08 运维文档、backup/restore 的 worker outbox/state 语义同步修订。

Observer(§6)仍未落地,与产品迁移同批实现。

风险与诚实边界:

- M3 之前 web 不宣称 Live Workspace;M2 只是文本/工具闭环。
- M4 之前 TUI 是唯一已验收产品面,`human local` 默认入口不变;web 的
  "已等价"必须以 web 版真实 OpenCode 门为证据,不以组件测试替代。
- web 版真实门预期比 PTY 键值门更易自动化(HTTP 命令替代原始键值),这是
  删除 TUI 的维护收益之一,但覆盖范围必须先逐项对齐旧门再谈删除。
- web 远程部署的认证/TLS 边界遵循 [08 运维](08-operations.md);本设计不
  引入新的默认公网暴露。
