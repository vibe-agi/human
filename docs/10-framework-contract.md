# 10 · Human Framework 扩展合同

Human 同时提供两个产品形态和一套可组合框架：

- **HumanLLM**：实时、增量、response-oriented；
- **HumanAgent**：持久、任务型、Task/Message/Artifact-oriented；
- **Human Framework**：保留上述正确性内核，允许宿主替换 transport、storage、
  protector、identity、policy、workspace 与 observability。

本页是公开 SPI 的设计与迁移合同。未迁移的接口不得因为位于 `internal/`
就被描述为可插拔；已经列出的目标接口也不得在实现前写成已支持。

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

内核唯一拥有状态机语义：合法转移、command digest、exact replay、revision
CAS、lease fence、Artifact 身份、apply receipt 与终态不可重开。adapter 可以选择
数据结构、协议和部署方式，但不能重新解释这些语义。

port 必须定义在消费它的公共 package 中，使用领域类型，不暴露 SQL row、HTTP
request、WebSocket frame、A2A DTO 或某个云厂商的 KMS DTO。官方 adapter 与第三方
adapter 使用同一个接口、错误分类和 conformance suite。

## 2. 稳定 ports

| port | 内核要求 | 官方 adapter | 宿主可替换 |
|---|---|---|---|
| Agent Store | 原子事务、expected revision/fence、command receipt、稳定分页 snapshot | SQLite | PostgreSQL、FoundationDB、自有服务 |
| LLM Store | admission-before-200、wire exact replay、event/receipt/tool ledger 原子边界 | SQLite | PostgreSQL、自有 durable service |
| Caller Transport | principal → 领域命令；DTO 不进入内核 | OpenAI/Anthropic/Responses/A2A HTTP | gRPC、消息队列、私有协议 |
| Worker Transport | assignment/event/ACK/NACK、断线恢复、背压与 durable outbox | WebSocket | gRPC stream、队列、内网 RPC |
| Workspace | exact-base CAS、revision/digest、indeterminate 收口 | SQLite journal + host applier | Git、远程 IDE、对象存储、虚拟工作区 |
| Protector | 带 purpose/AAD 的 envelope seal/open、key id/version | plaintext/no-op 开发实现与显式 key adapter | KMS、HSM、Vault、宿主密钥环 |
| Identity/Policy | 认证 principal、Authority 派生、worker/tool/apply 授权 | token/auth callbacks | JWT、session、mTLS、RBAC/ABAC |
| Observer | typed lifecycle event，不参与正确性决策 | `slog`/metrics adapter | OTel、审计总线、自有监控 |
| Clock/ID source | 测试确定性；生产值必须单调/稳定到合同要求 | system clock + UUID | 逻辑钟、宿主 ID 服务 |

Codec 是 Caller Transport 的纯转换子接口：decode 必须严格、encode 必须确定，
并声明其 capability。内核不通过字段相似度猜协议或工具语义。

## 3. Store 不是 CRUD

第三方 Store 的最低合同如下：

1. 一次领域 commit 中的状态、message/event、command receipt、lease grant、
   Artifact/Submission 必须全成或全败；
2. expected revision、lease owner/fence 与 Workspace confirmed head 必须在同一
   commit 内复验，不能在事务前读一次后盲写；
3. 同 command id + 同 digest 精确 replay；同 id + 异 digest 有限冲突；
4. committed 结果返回前发生的断线或取消不得撤销 commit；
5. 查询 snapshot 与后续 cursor 之间不能丢事件；分页顺序和 tie-breaker 固定；
6. corruption、ambiguous commit 和不支持的 schema 必须 fail-closed；
7. Store 业务 port 不含 `Close`；拥有它的 `framework.Resource` 在释放前必须先由
   runtime 拒绝新操作并等待已进入的操作，不能关闭仍在使用的连接；
8. claim 必须保证一个 Task/一个 fence 只授予一个 worker，竞争失败方不得得到
   看似成功的 assignment。

因此 Agent Store 会采用 typed transaction port，而不是公开 `database/sql`，也不把
整套状态机下推给 driver。LLM Store 会按 completion、receipt、audit、credential
等正确性边界拆分小接口，但组合时必须验证所需 capability 与原子性。

所有官方和第三方实现必须通过公开 `humantest` conformance kit。仅仅编译通过一个
Go interface 不构成兼容证据。

## 4. Transport 合同

Caller Transport 负责认证、限流前的 wire 上限、严格 decode、capability negotiation
和领域错误映射；它不决定 Task/Completion 状态。传输连接结束不等于领域任务结束。

Worker Transport 必须把以下概念分开：

- **delivery id**：一次可 ACK/NACK 的传输投递；
- **command id / event id**：跨重连稳定的领域幂等身份；
- **worker id + lease fence**：提交时授权；
- **connection/session id**：仅用于活连接，不进入正确性身份。

outbox 必须先持久后发送；ACK 才能删除；确定性拒绝以 NACK 终结该条，不能把它变成
永久队头毒丸。断线、重复、乱序、半开和三方同时离线都属于 transport conformance，
不能靠长时间健康连接代替。

## 5. Protector / KMS 合同

Protector 处理需要静态加密的 payload，不替代认证、hash 或访问控制。最小语义是：

```go
type Protector interface {
    Seal(context.Context, ProtectionContext, []byte) (Envelope, error)
    Open(context.Context, ProtectionContext, Envelope) ([]byte, error)
}
```

`ProtectionContext` 至少绑定 component、record kind、authority/workspace 与稳定 record
identity；adapter 必须把它作为 AAD 或等价的不可篡改上下文。`Envelope` 明确算法、
key id、key version、nonce 与 ciphertext，禁止依赖进程全局“当前 key”才能解旧数据。
同一 plaintext 不要求产生同一 ciphertext；幂等 digest 在 seal 前对 canonical plaintext
计算。`Open` 的认证失败、未知 key 与临时 KMS 不可用必须是不同的稳定错误类别。

内建 token 继续只保存不可逆 hash；需要重用的 caller/worker secret 由宿主 secret
store 管理，不得误用 Protector 把可验证 token 变成数据库可解密明文。

## 6. 生命周期与资源所有权

所有构造器必须声明依赖是 **borrowed** 还是 **owned**，不靠“实现了 `io.Closer`”猜测：

| 资源 | 注入实例 | 由 factory 打开 |
|---|---|---|
| Store / Protector client / Observer | borrowed；宿主在 Human 停止后关闭 | owned；Human 逆序关闭 |
| HTTP listener / router / TLS | 永远由宿主拥有 | CLI composition 可以显式拥有 |
| transport session / background loop | runtime 拥有并等待 | runtime 拥有并等待 |
| request / command context | 只控制本次调用 | 不得隐式关闭 runtime |

标准关闭顺序：停止 admission → 停止/等待 transport → 拒绝新领域操作 → 等待 in-flight
commit/terminal receipt → 关闭 owned adapter。任何 callback 都不能同步关闭正在调用它的
runtime；否则必须返回明确错误而不是自锁。

`human.NewLLM` / `human.NewAgent` 是 composition root，不隐藏进程全局单例。简单配置可
选择官方 SQLite adapter；高级配置注入已打开的 borrowed ports。一个配置不能同时
提供默认路径和自定义 Store，避免出现两个持久真相。

## 7. Capability、错误与版本

- 构造时验证 capability；运行中不做“有这个方法大概就支持”的猜测；
- unsupported 是有限、typed error，不得 silent fallback；
- conflict、unauthorized、not found、temporary unavailable、corrupt、indeterminate 分开；
- SPI 使用显式 contract version/capability set；未知必需 capability 构造失败；
- clean break 期间允许调整接口，但一次提交内官方 adapter、fake 和 conformance kit 必须
  同步，不能保留双读、双写或旧 fallback。

## 8. Conformance 与完成证据

每个 port 的公开测试套件至少验证：

- exact replay / divergent replay；
- 并发 CAS、revision 与 lease fence；
- transaction rollback 与 commit 后 caller cancel；
- snapshot-to-stream 无缺口；
- close 与 in-flight operation；
- corruption / unsupported capability fail-closed；
- transport drop、重复、乱序、半开、NACK 队头释放；
- 仅 fake 能触发的 transient/ambiguous fault 与真实官方 adapter 的重启恢复。

最终示例必须同时替换 storage、authentication 和 transport，而不是只注入三个回调后
仍偷偷依赖 SQLite/HTTP/WebSocket。示例实现与官方 adapter 跑同一套测试。

## 9. 迁移顺序

1. 冻结本页合同和公共生命周期词汇；
2. 把 HumanAgent SQL 从领域方法移入 typed Store adapter；
3. 发布 SQLite Agent adapter 与 `humantest.AgentStore`；
4. 抽离 Agent worker transport/outbox 并实现远程 worker；
5. 拆 HumanLLM Store、Worker Transport 和 Codec；
6. 在 plaintext canonical bytes 与 Store 之间接 Protector；
7. 增加完整自定义 composition 示例；
8. 对官方与 fake adapter 运行同一故障矩阵。

第 2–3 步现已完成：HumanAgent 领域路径只依赖 `agent.Store`，`agent/sqlite` 是官方
adapter，官方 SQLite 与测试内存实现运行同一个 `humantest.TestAgentStore`；调用方可向
`agent.Config.Store` 注入 borrowed 或 owned resource。Agent Worker Transport 当前只完成
port，远程 adapter/outbox 仍在第 4 步实现中。在第 5 步完成前，`human.NewLLM` 仍是
SQLite + 内建 codec/worker WebSocket composition。文档必须保持这条现实边界。
