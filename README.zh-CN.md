# Human

> 全世界最慢的 LLM。推理能力惊人,延迟感人,还要交社保。

[English](README.md)

## 这是什么

所有 AI 产品都在说"AI 替代人类"。我们反着来:**这个项目用人类替代了 AI。**
就是你。

Human 是一个 OpenAI/Anthropic 兼容的模型服务,只不过"模型"是一个开着浏览器
的人。你的编程 Agent(OpenCode,或任何会说 Chat Completions / Messages /
Responses 的东西)照常调用 `POST /v1/chat/completions` —— 请求落进一个人类的
收件箱。这个人读题、思考(参数量:约 860 亿神经元;训练数据:一个童年)、
打字回答,必要时回发**原生 tool call** 让 Agent 在它自己的工作区执行,最后
点一下"完成交付"。

Agent 毫不知情。它收到一条完全标准的 SSE 流,然后对着一个它以为是矩阵乘法
的东西说"谢谢"。

本着诚实原则,附模型卡:

| 指标 | 数值 |
|---|---|
| 参数量 | 1 个人 |
| 上下文窗口 | 取决于睡眠质量 |
| Tokens/秒 | 状态好的时候,2 |
| 幻觉率 | 不为零,但会道歉 |
| 对齐 | 可以谈 |

## 说正经的,这有什么用

把人塞进模型插槽,其实真有用:

- **升级救场**:凌晨两点 Agent 卡死,资深工程师顶进"模型"的位置接管一轮——
  Agent 的工具循环、权限确认、工作区原封不动。不用共享屏幕,不用"把报错发我"。
- **真的能循环的 human-in-the-loop**:人可以回答、反问、**借 Agent 自己的执行
  闸**跑命令、在实时镜像里改文件并作为原生 `write`/`edit` 交付——客户工作树
  始终是唯一真相。
- **真值数据**:把一个靠谱人类在模型位上的操作录下来,就是任何 benchmark 都
  卖不了你的评测数据。
- **还有一个持久任务面**:实时的 HumanLLM 之外还有 HumanAgent——A2A 1.0 端点,
  durable Task/Artifact、无时钟 lease,适合比一杯咖啡更长的工作。

所有链路都 fail-closed、逐字节幂等、经 durable outbox 崩溃恢复,且有 TLA+
建模——人可以慢,但管道必须正确。(细节见 [docs/](docs/):90 个 formal gate、
真实 CLI 故障注入门。这部分我们不开玩笑。)

## 快速上手:60 秒成为一个模型

需要:Go(或 release 二进制)、一个浏览器、一个人类(你就行)。

**终端 1 —— 启动服务和你的收件箱:**

```sh
human local --workspace .
# ...
# model base URL: http://127.0.0.1:19080/v1
# human side (browser): http://127.0.0.1:19081/?token=...   ← 浏览器打开这个
```

打开打印的 URL,那就是你的驾驶舱:收件箱、会话、命令行、todo 计划、
Live Workspace review 面板。默认英文,右上角一键切中文。

**终端 2 —— 让 Agent 指向你自己。** OpenCode 加一个 provider:

```jsonc
// opencode.json
"human": {
  "npm": "@ai-sdk/openai-compatible",
  "name": "Human(本人)",
  "options": {
    "baseURL": "http://127.0.0.1:19080/v1",
    "apiKey": "{env:HUMAN_CALLER_TOKEN}"
  },
  "models": { "human-expert": { "name": "Human Expert" } }
}
```

```sh
export HUMAN_CALLER_TOKEN="$(human local credentials --workspace . --token-only)"
opencode --model human/human-expert
```

在 OpenCode 里随便问点什么。浏览器"叮"一声。你现在是模型了。深呼吸。

**手边没有 Agent?curl 你自己:**

```sh
curl -N http://127.0.0.1:19080/v1/chat/completions \
  -H "Authorization: Bearer $HUMAN_CALLER_TOKEN" \
  -H "Content-Type: application/json" -H "Idempotency-Key: try-1" \
  -d '{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"你好,模型"}]}'
```

curl 会礼貌地挂着,直到你在浏览器里作答。恭喜,你实现了人工人工智能。

## 远程 / 团队

```sh
human gateway --listen :8080          # 部署侧(前面放 TLS)
human worker --gateway wss://your-gateway/internal/v1/worker/ws
# 会打印你的浏览器收件箱 URL;token 走 HUMAN_GATEWAY_TOKEN
```

## 嵌入(写 Go 的看这里)

`human.NewLLM()` / `human.NewAgent()` 是 transport-neutral 内核:Store /
认证 / codec / transport / KMS 全部是可替换 port,`humantest` 提供公开
conformance,[`examples/custom-framework`](examples/custom-framework/README.md)
是完全自有装配的可运行示例。web UI 只是公共 `workerkit` 领域层上的无状态
投影——看不上我们的界面,欢迎自带。

## 严肃的部分

| 文档 | 内容 |
|---|---|
| [01 目标](docs/01-goals.md) · [02 Gateway](docs/02-gateway.md) · [05 契约](docs/05-m0-contract.md) | 产品与协议边界 |
| [07 嵌入](docs/07-embedding.md) · [10 框架合同](docs/10-framework-contract.md) | 库的 ports、conformance、资源所有权 |
| [08 运维](docs/08-operations.md) · [09 TLA+](docs/09-formal-model.md) · [11 人侧栈](docs/11-human-side.md) | 备份恢复、形式化模型、web worker 栈 |

诚实状态:OpenCode 1.17.18 单机路径已真实验收(真实 CLI 门、网络故障门);
Codex 部分验证,Claude 目前只有 codec。随包附带的人类,暂不支持微调。
