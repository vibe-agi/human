# Human

> 全世界最慢的 LLM。推理能力惊人，延迟感人，还要交社保。

一个 OpenAI/Anthropic 兼容的模型服务，只是模型是个人。

[English](README.md)

你的编程 Agent 照常调 `POST /v1/chat/completions`，请求出现在某个人的浏览器里。这个人看完、打字回答，必要时回发原生 tool call 让 Agent 在自己工作区执行，然后点交付。Agent 收到一条普通的 SSE 流，对此一无所知。

## 同一次请求，两边视角

浏览器是 Human 的接单台：对话、原生 tool call、Tasks 和经过确认的工作区改动都在这里。

[![Human 接单台中的工具调用、Tasks 与工作区改动](docs/assets/screenshots/human-console.webp)](docs/assets/screenshots/human-console.webp)

Agent 侧仍然是熟悉的工作流。这里 OpenCode 接收 Human 给出的计划，并在自己的工作区执行文件写入。

[![OpenCode 接收 Human 生成的计划与文件改动](docs/assets/screenshots/opencode-caller.webp)](docs/assets/screenshots/opencode-caller.webp)

我们知道这听起来像什么。为了让一个人能以每秒 2 个 token 的速度打出“你重启试过吗”，我们造了一整套幂等、崩溃恢复、TLA+ 验证的管道。

它真正能做的（都有真实 CLI 门测试背书）：

- 能真正干活的 human-in-the-loop：回答、反问、借 Agent 自己的执行闸跑命令、在实时镜像里改文件并作为原生 `write`/`edit` 交付。工作树始终在 Agent 那边。
- Wizard-of-Oz 原型：你的产品还没有 AI？先让人扮演一个。协议兼容意味着现有 agent、harness、客户端零改造接入。

其它用途我们不好意思写进正式文档，留给你自己发现。比如：

- 需要向客户现场演示“我们自研大模型”的时候；
- 凌晨两点顶替卡死的 Agent 救场——前提是 gateway 提前部好了，而不是凌晨两点才开始读这份 README；
- 把自己在模型位上的操作录下来当评测数据——都在 SQLite 里，导出工具还没写，懂的自然懂；
- 等 A2A 生态哪天想正经雇一个人类——我们这儿有现成的端点。

管道是认真的：全链路 fail-closed、逐字节重放、远程 worker durable journal、91 个 formal gate、跑真实 OpenCode CLI 的故障注入门。感兴趣看 [docs/](docs/)。

## 跑起来

需要 Go（或 [release 二进制](https://github.com/vibe-agi/human/releases)）、一个浏览器、一个人。

```sh
human local --workspace ~/human-workspace
```

它会打印 Human 侧基础工作目录和两个 URL：

```
Human workspace base: /home/human/human-workspace
model base URL: http://127.0.0.1:19080/v1
human side (browser): http://127.0.0.1:19081/?token=...
```

打开第二个，那是你的收件箱。

`--workspace` 只表示 Human User 这台机器上的基础目录，不是 Agent User 的 cwd。
每个 workspace-capable harness session 默认得到一个稳定的 `session-<hash>` 子目录；
接单后也可在 Web 中把该会话切换到 Human 已有的任意 repo。切换时 repo 当前内容成为
baseline，不会整仓误交付。Human 与 Agent 只需约定两边目录代表同一个逻辑项目：
交付的 tool call 始终使用项目相对路径，由 Agent 在自己的 cwd 执行。两边的绝对路径
无需相同，也不会通过模型协议互相暴露。

然后把 Agent 指向第一个。OpenCode 配置：

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
export HUMAN_CALLER_TOKEN="$(human local credentials --workspace ~/human-workspace --token-only)"
opencode --model human/human-expert
```

若 Agent User 与 Human User 不是同一账号或机器，由 Human 通过安全渠道把 caller token
交给 Agent User；Agent 无需、也不应知道 Human 的工作目录。

问它点什么，浏览器会响。现在你是模型了——慢慢想，Agent 等得起。

手边没有 Agent 的话，curl 也行：

```sh
curl -N http://127.0.0.1:19080/v1/chat/completions \
  -H "Authorization: Bearer $HUMAN_CALLER_TOKEN" \
  -H "Content-Type: application/json" -H "Idempotency-Key: try-1" \
  -d '{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"你好"}]}'
```

curl 会一直挂着，直到你在浏览器里回复。

同一个本地端点还提供 `POST /v1/responses` 与 `POST /v1/messages`，同时接受
OpenAI 风格的 `Authorization: Bearer` 和 Anthropic 风格的 `X-Api-Key`；重复或冲突
凭据会 fail closed。三个 API 的 wire compatibility 都已实现。harness 能力另行记账：
能正确解码某种 API，不自动等于对应 CLI 的全部 Workspace/tool 特性都已真实验证。

核心 wire contract 还直接通过官方 Go 客户端验证，当前固定
`openai-go/v3 v3.37.0` 与 `anthropic-sdk-go v1.58.1`：由官方客户端序列化请求进入
Human，再由同一个客户端解码 aggregate 响应、SSE 文本与 function/tool call、回传 tool result、
标准错误 envelope，以及 Anthropic `count_tokens`。这里承诺的是模型调用兼容，不是两个厂商全部管理 API 或
provider-hosted tool。

远程部署就两条命令：服务器上 `human gateway --listen :8080`（前面放 TLS），人的机器上
`human worker --gateway wss://.../internal/v1/worker/ws --caller-scope <caller-id>
--workspace-scope <opaque-workspace-key>`。scope 把 Human 自己的 mirror 绑定到一个经过认证的
Agent User workspace；它不是目录路径。

## 嵌入

`human.NewLLM()` / `human.NewAgent()` 是 transport-neutral 内核。Store、认证、codec、transport、KMS 都是可替换 port，`humantest` 提供公开 conformance；[`examples/custom-framework`](examples/custom-framework/README.md) 完全跑在自有 Store、认证和 transport 上。web UI 只是 `workerkit` 领域层上的无状态投影，不喜欢也可以换掉。

文档：[目标](docs/01-goals.md)、[gateway](docs/02-gateway.md)、[嵌入](docs/07-embedding.md)、[运维](docs/08-operations.md)、[TLA+ 模型](docs/09-formal-model.md)、[框架合同](docs/10-framework-contract.md)、[人侧栈](docs/11-human-side.md)。

老实说状态：三个对外 API 都已有默认执行的 aggregate + stream 产品门，且全程由 Web
人侧完成。真实客户端门给 Claude Code 2.1.217、OpenCode 1.17.18、Codex 0.145.0 同一条
基线：final、第二个 CLI 进程恢复 session、调用方命令成功、命令失败 result 回流后恢复 final、
有界 Web 拒单。Claude、OpenCode 与 Codex 都有独立 Human/Agent 工作目录下的
Workspace create→原生文件工具、modify→原生文件工具及 caller 侧最终字节实证；
Codex 使用 Responses `custom` freeform `apply_patch`，并且只有请求声明精确匹配的
工具/grammar 时才获得 Workspace profile。正式 Web Tasks 面板还让 Claude 完成
`TaskCreate → TaskUpdate → TaskList`，并让 OpenCode `todowrite`、Codex `update_plan`
各自完成 pending→in_progress→completed 三段 continuation。正式 Web Command 面板也通过
可替换的 exact profile 分别生成 Claude `Bash`、OpenCode `bash`、Codex `exec_command`；
未知版本、缺少 Codex 模型元数据或行为 schema 漂移会 fail closed 到
Chat/RemoteTools 与通用 declared-tool 编辑器。Claude/Codex 均通过 progress 后强制断
SSE 的真实恢复门；这项证据仍不外推为 Workspace 故障恢复。
Testcontainers 门还把“协议直连”独立成另一层，对三个 API 分别跑 aggregate + stream；两层
统一访问主机 `local` 栈，并可由
主机 OpenAI-compatible 模型通过 Web API 扮演人类；见[运维手册](docs/08-operations.md#testcontainers-协议直连--三客户端--llm-人类门)。
