# formal/ · TLA+ 模型检查（二期 A2A 交付协议）

[docs/phase2-async-mode.md](../docs/phase2-async-mode.md) §7 理论的可执行版本，验证对象是**二期异步交付模式**的协议核心。一期也持久化跨 completion 的任务状态，但没有这里建模的 A2A、累计 artifact 与 rewind 协议面。`HumanAgent.tla` 建模：humand 单写者权威、Bob 的人类转移（接单/交付/完成/否决/失败）、caller 的机器转移（fetch/apply 抽象）与人类/敌手转移（追问/取消/请求回滚/**背着系统乱改工作区**）。

文件树抽象为交付版本号（commit 不可变 ⇒ 版本号单调不复用）；干净工作区上 revert+apply 必成功（公理 A1/A2）；脏工作区上 apply 结果非确定：校验通过或**显式**冲突（fail-explicit）。

**这是有限状态下的协议核心模型检查，不是完整证明。** 它检查的是协议逻辑（锚点/审计/回滚/收敛/竞态）在小配置下穷举无反例；不覆盖实现（见 §边界）。

## 运行

```sh
# 需 Java 11+、Python 3 与 TLC 2.19 的 tla2tools.jar
export TLA2TOOLS=/path/to/tla2tools.jar   # 或放一份到 formal/tla2tools.jar
./run-checks.sh                            # 跑 5 项并断言版本/退出码/性质/状态数
```

> **关于 `-deadlock`（易错点）**：TLC 的 `-deadlock` 旗标是**关闭**死锁检查，不是开启。死锁检查**默认开启**——因此我们**不**传该旗标。合法的静止终态用显式 `Terminating` 自环建模，故不会被误报为死锁。（早期版本误传了 `-deadlock`，等于没验死锁；已修正。）

## 验证的性质

| 类别 | 名称 | 含义 |
|---|---|---|
| 不变量 | `TypeOK` | 类型良构 |
| 不变量 | `LatestValid` | 锚点永远指向真实且未被回滚的交付 |
| 不变量 | `AppliedReal` | caller 的 applied/inflight 版本都曾由权威端交付；不证明 apply oracle 正确 |
| 不变量 | `SupersededOK` | 回滚标记只指向真实存在的审计记录 |
| 不变量 | `RewindPendingOK` | 待确认的回滚目标必在有效链上且严格更旧 |
| 动作性质 | `TurnsAppendOnly` | 审计只追加：历史永不被改写或缩短 |
| 动作性质 | `SupersededMonotone` | 回滚只做标记，永不取消标记/删除 |
| liveness | `EventuallyTerminal` | 任务最终到达终态（**依赖人类公平性**） |
| liveness | `EventuallyConsistent` | 在公平自动 pull 抽象下，caller 记录的 applied artifact 版本最终收敛到权威锚点 |
| 隐含 | 无死锁 | 默认开启；合法终态由 `Terminating` 自环显式建模、不误报 |

终态含 `completed / canceled / rejected / failed`（`Fail` 转移建模"人声明无法完成"）。

## 结果记录（2026-07-15，TLC 2.19，`./run-checks.sh` 可复现）

| 实验 | 配置 | 结果 |
|---|---|---|
| 主验证 | `HumanAgent.cfg`（3 轮/1 回滚/1 本地修改；机器+人类公平性） | **全过**，3,486 个不同状态穷举 |
| 规模复验 | `HumanAgentLarge.cfg`（4 轮/2 回滚/2 本地修改） | **全过**，54,478 个不同状态穷举 |
| 定理 3 实验 | `HumanAgentNoHumanFair.cfg`（仅机器公平性，人可永不行动；仅检查该 liveness） | 主配置中的 safety 全过；`EventuallyTerminal` 如预期失败（反例：一切一致后停在非终态 stuttering）——机械化印证"safety 与人无关，liveness 才依赖人" |
| 有牙齿测试 C | mutant：`RequestRewind` 不校验目标合法性 | `RewindPendingOK` **违例被抓** |
| 有牙齿测试 D | mutant：`ConfirmRewind` 物理删除审计记录 | `SupersededOK` **违例被抓** |

## 边界（诚实声明）

- **验协议非实现**：TLC 通过 ≠ 代码无 bug。`AppliedReal` 只证 applied 版本曾被交付，**不检测"本应冲突却静默报成功"**（实测注入该 mutant，现有性质仍全过）；这类内容维度 bug 属 oracle（apply/hash 校验，X-04）职责，模型层不可见。
- **抽象掉文件内容**：hash 校验证文本一致，不证语义适配（caller 脏工作区致的语义错位只暴露不消除）。
- **不证明完整工作树相等**：`applied` 表示最近验证过的 artifact 版本；`dirty` 本地改动本身不算版本分歧，故 `EventuallyConsistent` 不等价于两棵文件树逐字节相同。
- **消息通道**：建模的是**拉模型**的 stale read（`CallerFetch` 后权威可前进/回滚，已覆盖"读到旧值"）；humand↔TUI 的 WS 通道丢失/重复/乱序未在模型内建，其 seq/ack 幂等属实现层——这是一处明确未建模的假设。
- **pull 意图被抽象**：`CallerFetch` 被建模为公平自动动作，不含 `human_result(apply=true)` 的显式意图或授权；收敛结论以 caller 持续请求/允许 apply 为前提，也不验证终态后的产品调用策略。
- **liveness 依赖人的公平性**：人永不行动则任务永不完成——这是 human agent 的定义属性，非缺陷。
- **有限状态**：仅穷举了小配置；未做参数化归纳证明。git SHA 碰撞概率忽略。
