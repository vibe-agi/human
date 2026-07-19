# 10 · Human Framework 扩展合同

Human 同时提供两个领域内核和一套可组合框架：

- **HumanLLM**：实时、增量、response-oriented；
- **HumanAgent**：持久、任务型、Task/Message/Artifact-oriented；
- **Human Framework**：保留两个内核的正确性语义，让宿主替换 transport、storage、
  protector、identity、policy 与 workspace adapter。

本页描述已经存在的公共 SPI。只有位于公共 package、能由仓外 Go package 实现并有
测试证据的接口才算当前扩展点；`internal/` 类型和未来目标不算承诺。

## 1. 内核、port、adapter 与 composition root

依赖只能向内：

```text
OpenAI / Anthropic / Responses / A2A / custom transport
                         │
             HumanLLM / HumanAgent core
                         │
       typed ports + explicit correctness contracts
                         │
 SQLite / PostgreSQL / KMS / filesystem / custom adapters
                         │
       human.NewLLM / human.NewAgent / host composition
```

内核唯一拥有状态机语义：合法转移、digest、exact replay、revision CAS、lease fence、
Artifact 身份、apply receipt 与终态。adapter 可以选择协议、物理 schema 和部署方式，
但不能重新解释这些语义。

port 定义在消费它的公共 package 中，只使用领域类型，不暴露 SQL row、HTTP request、
WebSocket frame、A2A DTO 或云厂商 KMS DTO。官方 adapter 与第三方 adapter 使用同一个
接口、错误分类和可用的 conformance suite。

## 2. 已实现的公共 ports

| port | 当前公共合同 | 官方 adapter | 可替换为 |
|---|---|---|---|
| Agent Store | `agent.Store`：严格可串行化 transaction、revision/fence、command receipt、稳定 snapshot | `agent/sqlite` | PostgreSQL、FoundationDB、自有服务 |
| LLM Store | `llm.Store`：deployment bind、admission、wire replay、worker receipt 与 tool ledger 同一原子域 | `llm/sqlite` | PostgreSQL、自有 durable service |
| LLM Caller Transport | `llm.CallerTransport` / `CallerEndpoint`：认证 principal、cursor replay、断线不取消 durable work | `llm/callerhttp` | gRPC、消息队列、Unix socket、私有协议 |
| LLM Worker Transport | `llm.WorkerTransport` / `WorkerEndpoint`：assignment、event、ACK/NACK、背压 | `llm/workerws` + durable Journal | gRPC stream、队列、内网 RPC |
| Agent Worker Transport | `agent.WorkerTransport` / `WorkerEndpoint`：lease assignment 与领域 event 分离 | `agent/workerws` + durable Journal | gRPC stream、队列、内网 RPC |
| Agent Caller Surface | `agent.Agent` 的领域命令；caller DTO 不进入领域 | `a2a` HTTP+JSON handler | 自有 HTTP/RPC/队列 adapter |
| Workspace Store | `workspace.Store`、`CASApplier`、exact-base intent、revision/digest、indeterminate 收口 | `workspace/sqlite` + host applier | PostgreSQL、Git、远程 IDE、对象存储、虚拟工作区 |
| Protector | `protect.Protector`：带 Binding/AAD 的 envelope seal/open、key id/version | `protect/aead` AES-256-GCM | KMS、HSM、Vault、宿主密钥环 |
| Codec / Policy | `llm.Codec`、`AdmissionPolicy`、`WorkerRouter`、`ToolAuthorizer` | 三个 built-in codec、显式 `llm.AdmitAll`、单 worker 直路由 | 私有模型协议、RBAC/ABAC、专家路由 |
| Clock / ID / Seed | 并发安全、context-aware、codec 重放所需的持久 seed | system clock、随机 opaque ID | 逻辑钟、宿主 ID/KMS 服务 |

Codec 是纯转换 port：decode 必须严格，encode 只能由已持久化 seed 与有序 event 决定，
并声明 fingerprint 与上限。内核不通过字段相似度猜协议或工具语义。认证属于具体
transport；核心只接收 adapter 从可信凭据构造出的 principal。当前没有宣称通用
Observer SPI，日志与 metrics 仍是后续可抽取项。

策略 port 有三条已定死的安全语义：

1. **AdmissionPolicy 是必填部署选择。** `NewService` 拒绝 nil 策略；全放行必须显式
   写 `llm.AdmitAll()`，使"只靠 transport 认证"成为可 grep、可审计的决定，而不是
   静默默认。
2. **认证属性只是 advisory 通道。** caller adapter 可以把已认证 principal 的 claims
   （如 JWT claims、mTLS SAN）经 `AdmissionRequest.CallerAttributes` 传给
   `AdmissionPolicy` 与 `WorkerRouter` 做 ABAC 和专家路由。属性不进入 correctness
   identity：request digest、幂等比较、持久记录与 worker assignment 一概忽略它们，
   异属性的精确重试仍逐字节 replay。`ToolAuthorizer` 故意收不到属性——工具授权发生
   在 worker event 时刻、可能跨进程重启，只允许依赖 durable 状态；需要 claims 的
   授权应在 admission 时刻降级 tier 或拒绝。
3. **缺 Router 是配置错误,不是容量问题。** nil Router 只服务单 worker 部署：第二个
   worker 连接后 admission 以 `ErrWorkerRouterRequired`（HTTP 500
   `worker_router_required`）fail-closed，不伪装成可重试的 `worker_unavailable`。

## 3. Store 不是 CRUD

`agent.Store`、`llm.Store` 和 `workspace.Store` 才是核心依赖；`agent/sqlite`、
`llm/sqlite`、`workspace/sqlite` 只是官方实现。宿主可以完全不引入 SQLite，直接注入
自己的 Store。核心不得根据路径、DSN 或具体 driver 做分支，也不得从 `Store` 向下类型
断言取数据库句柄。三个 Store 是不同领域的一致性合同，不是一个含糊的通用 CRUD；同一
物理 backend 可以同时实现多个接口，但不能因此合并它们的公开状态机。

第三方 Store 的最低合同是：

1. 一次领域 commit 中的状态、event/message、receipt、lease、Artifact/Submission
   全成或全败；
2. expected revision、lease owner/fence 与 Workspace head 在同一 commit 内复验；
3. 同 id + 同 digest 精确 replay；同 id + 异 digest 有限冲突；
4. committed 结果返回前的断线或取消不得撤销 commit；
5. 查询 snapshot、固定排序与 cursor 之间不能丢记录；
6. corruption、unsupported schema 与 ambiguous commit 必须分类并 fail-closed；
7. `Store` 不含 `Close`；生命周期由 `framework.Resource[Store]` 显式表达；
8. LLM Store 的首次 `Bind(DeploymentID)` 原子且永久：精确重复幂等，异 deployment
   必须冲突，不能让两个正确性域误共享同一逻辑库。

`Bind` 只绑定永久 namespace，不是活实例租约或 HA fencing。当前合同要求每个 Store
correctness namespace 同时只有一个 HumanLLM Service 或 HumanAgent core 在驱动；自定义
共享 Store 若要做 active-active，必须在宿主层增加带 generation/fence 的协调，不能把
“transaction 可串行化”误当成“不会把同一任务投给两个 Human”。多实例协调 port 需要
单独建模后再进入公共合同。

Agent 与 LLM Store 都采用 typed transaction port，不公开 `database/sql`，也不把状态机
下推给 driver；Workspace Store 则直接约束“durable intent → 外部 CAS → terminal receipt”
的 at-most-once apply 边界。LLM 的 request、response event、worker receipt 与 tool ledger
故意属于同一个 `llm.Store` consistency domain；拆成独立提交的 backend 会破坏 ACK 与
精确重放。

Store 的 `View`/`Update` callback 只在调用期间有效，adapter 不能重试 callback，也不能
让 mutable bytes/maps 跨事务形成别名。`ErrStoreCommitUnknown` 表示“可能已提交”，核心
必须按 durable identity 对账，不能直接重跑可见响应或副作用。

## 4. Transport 合同

Caller Transport 负责认证、wire 上限、显式路由和领域错误映射；它不决定 Task/Response
状态。caller 连接结束只取消当前等待，不取消已经 durable admission 的工作。精确 retry
使用同一 idempotency key、digest 与 cursor 继续读取。

官方 HTTP/WebSocket adapter 用 `framework.Fault` 区分认证裁决：只有
`CodeUnauthenticated` / `CodeForbidden` 且 `RetryNever` 才分别映射为 401/403；provider
不可用与未分类基础设施错误一律返回无内部细节的 503，使远端 worker 保留凭据并继续退避
重连。自定义 adapter 必须保留这一“已证明永久失败”和“暂时无法裁决”的差异。

Worker Transport 必须区分：

- **delivery id**：一次可 ACK/NACK 的传输投递；
- **event/command id**：跨重连稳定的领域幂等身份；
- **worker id + lease**：提交时授权；
- **connection/session id**：只属于活连接，不进入 correctness key。

远端 worker 必须先把 assignment/event 写入 durable Journal/outbox，再跨 ACK 边界；只有
ACK/NACK 才删除 outbox。确定性拒绝以 NACK 终结该条，不能成为永久队头毒丸。断线、
重复、ACK 丢失、半开和 caller/worker/service 三方同时离线都是正常协议输入。

同理，`ToolAuthorizer` 只有返回 `CodeForbidden + RetryNever` 才会产生终态 NACK；
`CodeUnavailable`、取消和未分类错误不生成 receipt，也不删除 worker outbox，原 delivery
在策略服务恢复后按相同 identity 精确重试。实现必须可重入，不能在未落 receipt 前消费
一次性授权。

HTTP 与 WebSocket adapter 都不拥有 listener。`Start` 只借用 endpoint，返回的 Runtime
拥有 session 与后台循环；宿主先停止 transport，再关闭 Human core。

## 5. Protector / KMS 合同

Protector 处理静态 payload protection，不替代认证、token hash 或访问控制：

```go
type Protector interface {
    Describe(context.Context) (protect.Description, error)
    Seal(context.Context, protect.Binding, []byte) (protect.Envelope, error)
    Open(context.Context, protect.Binding, protect.Envelope) ([]byte, error)
}
```

`Binding` 绑定 component、purpose、authority、namespace、record type/id、field 与 schema；
adapter 必须将它作为 AAD 或等价的不可篡改上下文。`Envelope` 包含 provider/format、
key id/version、nonce 与 ciphertext，不能依赖进程全局“当前 key”解历史记录。

digest 在 seal 前对 canonical plaintext 计算；同一 plaintext 不要求同 ciphertext。
authentication failure、unknown key 与 temporary provider unavailable 是不同稳定错误。
Protector 的网络/KMS I/O 必须发生在 Store callback 外。受保护部署默认拒绝 plaintext
record；允许读取旧明文只能由显式 migration policy 开启。

## 6. 生命周期与资源所有权

所有构造器都用 `framework.Borrow` / `framework.Own` 明确依赖所有权，不靠是否实现
`io.Closer` 猜测：

| 资源 | borrowed | owned |
|---|---|---|
| Store / Protector | 宿主保持到 Runtime `Done` 后再关闭 | Human 在 shutdown 或构造失败时逆序 release |
| Codec / policy / router / auth | 宿主保持到对应 Runtime `Done`，并满足并发合同 | 当前作为 borrowed policy value 注入 |
| listener / router / TLS | 永远由宿主拥有 | 只有 CLI composition 可显式拥有 |
| transport session / loop | Runtime 拥有并等待 | Runtime 拥有并等待 |
| operation context | 只控制本次操作/等待 | 不控制 Runtime 生命周期 |

标准顺序是：停止 caller admission → 停止并等待 caller/worker transport → 拒绝核心新操作
→ 等待 in-flight commit → release owned Protector → release owned Store。

`human.NewLLM` 与 `human.NewAgent` 不创建进程全局单例。`NewLLM` 要求显式提供
`llm.Store` 与 `DeploymentID`；`DefaultLLMConfig` 只提供新鲜 codec registrations，不
创建数据库或 listener。`NewAgent` 同样要求显式 `framework.Resource[agent.Store]`；
`DefaultAgentConfig` 不创建数据库或 listener。需要 SQLite 时由宿主显式调用
`agent/sqlite.Open`，需要其它 backend 时直接注入自己的实现。

## 7. Capability、错误与版本

- 构造时验证 contract major、feature 与 adapter description；
- unsupported 是有限 typed error，不得 silent fallback；
- conflict、unauthorized、not found、temporary unavailable、corrupt、indeterminate 分开；
- codec 的 version/fingerprint 钉住所有影响 decode 或输出 bytes 的实现与配置；
- clean break 必须在同一提交同步官方 adapter、Memory model、conformance 与 schema，
  不保留双读、双写或旧 fallback。

## 8. Conformance 与故障证据

`humantest` 已公开 Agent Store、LLM Store、Workspace Store 以及两个 worker Journal 的
conformance kit；两个 Journal 另有可由第三方直接调用的
`TestAgentWorkerJournalRecovery` / `TestLLMWorkerJournalRecovery`。针对 Store 合同中
最危险的 `ErrStoreCommitUnknown` 路径，`humantest.CommitUnknownLLMStore` 提供可组合的
歧义提交注入（commit 已落但应答丢失 / commit 前连接丢失两种模式），
`humantest.TestLLMServiceCommitUnknownReconciliation` 则用真实 HumanLLM core 驱动
factory 提供的 Store，验证 admission 与 worker event 两个提交点都能按 durable identity
对账：可疑但已提交的 admission 收敛为精确 replay 且只有一个 assignment；真丢失的
commit 失败后同 key 精确重试成功；可疑 worker event 收敛为幂等 ACK。官方
`llm/sqlite` 与示例 custom Store 都运行该套件。官方 SQLite adapters
与可用的 Memory model 运行相同的领域套件，覆盖
callback exactly-once、rollback、strict serialization、byte ownership、limits、CAS、binding、
receipt、release/reopen recovery 与 retention。Journal recovery kit 会跨 reopen 验证完整
payload/digest、冲突 tombstone 和 sequence 单调性，但不会把 `Release` 当作 crash。第三方 driver
应在自己的包中用 fresh-store factory 运行
`humantest.TestAgentStore`、`humantest.TestLLMStore` 或 `humantest.TestWorkspaceStore`，再
补充由测试宿主控制的进程退出/kill、迁移、损坏与基础设施故障测试。

[`examples/custom-framework`](../examples/custom-framework/README.md) 同时演示仓外形状的
独立单文件 Store、自有认证、非 HTTP CallerTransport 和自定义 Protector decorator，且只
使用公共 package。该 Store 自行实现全部 `StoreView` / `StoreTx`，用版本化校验快照做原子
提交，直接运行公开 conformance、损坏测试与无 `Release` 的子进程退出恢复测试；同包另保留
可选 Store middleware 说明 owned/borrowed 组合。它是教学 adapter，不是生产数据库。
Postgres、MySQL 或远程 Store 在同一个 port 实现事务原语即可，HumanLLM core 无需知道底层产品；
Protector 的相同位置可替换为 KMS/HSM。

Codec 与策略 hook 已有 external-package contract fixtures；独立的通用 codec/policy
conformance kit 仍是后续项。LLM 的 Memory/SQLite 共用矩阵覆盖 caller wait cancel、worker
断线、ACK 丢失、精确重放、poison follower 与三方同时离线；Agent 共用矩阵覆盖 assignment
重投、event receipt 重放、Store 重启与 poison follower。Memory Store 的重启分支使用
`Abandon`（不调用 `Release`）作为确定性语义模型；SQLite 在共用矩阵里只做诚实的 release/reopen。
两套 worker Journal 以相同 factory/opener 套件验证 release/reopen，并另以 Memory `Abandon`
验证进程内语义。真正的进程边界由 Agent/LLM SQLite Store、两套 SQLite Journal 和示例 custom
Store 的子进程在成功提交后直接 `os.Exit`、父进程同路径重开来证明。它们仍不冒充断电、磁盘
损坏或远程数据库分区测试，第三方 driver 必须补自己的故障注入。

## 9. 当前落地状态

以下框架收口已经完成：

1. 两个 surface 的公共生命周期与 key 空间分离；
2. Agent / LLM typed Store 与官方 SQLite、Memory conformance；
3. Agent / LLM 独立 worker transport、durable Journal/outbox；
4. HumanLLM caller HTTP、Codec、路由、准入和工具授权 ports；
5. 两个 surface 共用的 Protector/KMS port 与官方 AEAD；
6. `workspace.Store` 与独立 `workspace/sqlite` adapter；
7. 完整自定义 HumanLLM composition 示例（独立 Store、Auth、CallerTransport、Protector）；
8. SQLite/Memory Store 与 worker Journal 的 handle 重启、语义 abandon、真实 SQLite 子进程退出、断线、精确重放、poison 隔离及三方同时离线故障矩阵。

尚未完成的产品能力包括两个 surface 共用同一条 Workspace revision chain、通用 Observer
port、面向多实例协调的 Store adapter，以及独立 codec/policy conformance kit。这些边界
不会阻止宿主今天实现自己的 Store、Transport、Auth 或 Protector。
