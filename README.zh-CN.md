# Human

一个 OpenAI/Anthropic 兼容的模型服务,只是模型是个人。

[English](README.md)

你的编程 Agent 照常调 `POST /v1/chat/completions`,请求出现在某个人的浏览器
里。这个人看完、打字回答,必要时回发原生 tool call 让 Agent 在自己工作区执行,
然后点交付。Agent 收到一条普通的 SSE 流,对此一无所知。

我们知道这听起来像什么。为了让一个人能以每秒 2 个 token 的速度打出"你重启试
过吗",我们造了一整套幂等、崩溃恢复、TLA+ 验证的管道。不过它确实有用:

- 凌晨两点 Agent 卡死,资深工程师顶进"模型"的位置接管一轮——Agent 的工具循环、
  权限确认、工作树都不动。不用共享屏幕。
- 能真正干活的 human-in-the-loop:回答、反问、借 Agent 自己的执行闸跑命令、
  在实时镜像里改文件并作为原生 `write`/`edit` 交付。
- 把靠谱人类在模型位上的操作录下来,就是没处买的评测数据。
- 更长的活有 HumanAgent:A2A 1.0 端点,durable Task/Artifact,和实时路径分开。

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
