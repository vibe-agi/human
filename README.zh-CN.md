# Human

> 全世界最慢的 LLM。推理能力惊人,延迟感人,还要交社保。

一个 OpenAI/Anthropic 兼容的模型服务,只是模型是个人。

[English](README.md)

你的编程 Agent 照常调 `POST /v1/chat/completions`,请求出现在某个人的浏览器
里。这个人看完、打字回答,必要时回发原生 tool call 让 Agent 在自己工作区执行,
然后点交付。Agent 收到一条普通的 SSE 流,对此一无所知。

我们知道这听起来像什么。为了让一个人能以每秒 2 个 token 的速度打出"你重启试
过吗",我们造了一整套幂等、崩溃恢复、TLA+ 验证的管道。

它真正能做的(都有真实 CLI 门测试背书):

- 能真正干活的 human-in-the-loop:回答、反问、借 Agent 自己的执行闸跑命令、
  在实时镜像里改文件并作为原生 `write`/`edit` 交付。工作树始终在 Agent 那边。
- Wizard-of-Oz 原型:你的产品还没有 AI?先让人扮演一个。协议兼容意味着现有
  agent、harness、客户端零改造接入。

其它用途我们不好意思写进正式文档,留给你自己发现。比如:需要向客户现场演示
"我们自研大模型"的时候;凌晨两点顶替卡死的 Agent 救场(前提是 gateway 提前
部好了,而不是凌晨两点才开始读这份 README);把自己在模型位上的操作录下来当
评测数据(都在 SQLite 里,导出工具还没写,懂的自然懂);以及等 A2A 生态哪天
想正经雇一个人类——我们这儿有现成的端点。

管道是认真的:全链路 fail-closed、逐字节重放、durable outbox、90 个 formal
gate、跑真实 OpenCode CLI 的故障注入门。感兴趣看 [docs/](docs/)。

## 跑起来

需要 Go(或 [release 二进制](https://github.com/vibe-agi/human/releases))、
一个浏览器、一个人。

```sh
human local --workspace .
```

会打印两个 URL:

```
model base URL: http://127.0.0.1:19080/v1
human side (browser): http://127.0.0.1:19081/?token=...
```

打开第二个,那是你的收件箱。

然后把 Agent 指向第一个。OpenCode 配置:

```jsonc
// opencode.json
"human": {
  "npm": "@ai-sdk/openai-compatible",
  "name": "Human",
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

问它点什么,浏览器会响。现在你是模型了——慢慢想,Agent 等得起。

手边没有 Agent 的话,curl 也行:

```sh
curl -N http://127.0.0.1:19080/v1/chat/completions \
  -H "Authorization: Bearer $HUMAN_CALLER_TOKEN" \
  -H "Content-Type: application/json" -H "Idempotency-Key: try-1" \
  -d '{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"你好"}]}'
```

curl 会一直挂着,直到你在浏览器里回复。

远程部署就两条命令:服务器上 `human gateway --listen :8080`(前面放 TLS),
人的机器上 `human worker --gateway wss://.../internal/v1/worker/ws`。

## 嵌入

`human.NewLLM()` / `human.NewAgent()` 是 transport-neutral 内核。Store、认证、
codec、transport、KMS 都是可替换 port,`humantest` 提供公开 conformance;
[`examples/custom-framework`](examples/custom-framework/README.md) 完全跑在
自有 Store、认证和 transport 上。web UI 只是 `workerkit` 领域层上的无状态投
影,不喜欢也可以换掉。

文档:[目标](docs/01-goals.md)、[gateway](docs/02-gateway.md)、
[嵌入](docs/07-embedding.md)、[运维](docs/08-operations.md)、
[TLA+ 模型](docs/09-formal-model.md)、[框架合同](docs/10-framework-contract.md)、
[人侧栈](docs/11-human-side.md)。

老实说状态:OpenCode 1.17.18 单机是已验收路径;Codex 部分验证,Claude 目前
只有 codec。
