# Human

> The world's slowest LLM. Astonishing reasoning. Terrible latency. Unionized.

An OpenAI/Anthropic-compatible model server where the model is a person.

[简体中文](README.zh-CN.md)

Your coding agent calls `POST /v1/chat/completions` like it always does. The
request shows up in someone's browser. They read it, type an answer, maybe
send back native tool calls for the agent to run in its own workspace, and
hit deliver. The agent gets a normal SSE stream and is none the wiser.

Yes, we know how this sounds. We built an entire idempotent, crash-recovering,
TLA+-verified pipeline so that a human can type "have you tried restarting it"
at 2 tokens per second.

What it verifiably does (real-CLI test doors and all):

- Human-in-the-loop where the human can actually do things: answer, ask back,
  run commands through the agent's own execution gate, edit files in a live
  mirror and deliver them as native `write`/`edit` calls. The working tree
  stays on the agent's side.
- Wizard-of-Oz prototyping: your product doesn't have its AI yet? Ship a
  human. Protocol compatibility means existing agents, harnesses, and clients
  connect unchanged.

Other uses we're not comfortable putting in official documentation, so you'll
have to discover them yourself. For instance: live-demoing "our in-house
foundation model" to a client; taking over a stuck agent at 2am (assuming the
gateway was set up beforehand, not at 2am while reading this README);
recording your own model-seat sessions as evaluation data (it's all in
SQLite, the export tool doesn't exist yet, you know what to do); and, should
the A2A ecosystem ever want to hire an actual human — we happen to have an
endpoint ready.

The plumbing is the serious part: fail-closed everywhere, byte-exact replay,
durable outboxes, 90 formal gates, fault-injection doors that run the real
OpenCode CLI. See [docs/](docs/) if that's your thing.

## Run it

You need Go (or a [release binary](https://github.com/vibe-agi/human/releases)),
a browser, and a human.

```sh
human local --workspace .
```

It prints two URLs:

```
model base URL: http://127.0.0.1:19080/v1
human side (browser): http://127.0.0.1:19081/?token=...
```

Open the second one. That's your inbox.

Then point an agent at the first one. OpenCode config:

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

Ask it something. Your browser pings. You're the model now — take your time,
the agent will wait.

No agent handy? curl works:

```sh
curl -N http://127.0.0.1:19080/v1/chat/completions \
  -H "Authorization: Bearer $HUMAN_CALLER_TOKEN" \
  -H "Content-Type: application/json" -H "Idempotency-Key: try-1" \
  -d '{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"hello"}]}'
```

The curl hangs until you answer in the browser.

Remote setup is two commands: `human gateway --listen :8080` on a server (put
TLS in front), `human worker --gateway wss://.../internal/v1/worker/ws` on the
human's machine.

## Embedding

`human.NewLLM()` / `human.NewAgent()` are transport-neutral cores. Store,
auth, codecs, transports, and KMS are replaceable ports with public
conformance suites in `humantest`; [`examples/custom-framework`](examples/custom-framework/README.md)
runs entirely on its own store, auth, and transport. The web UI is a
stateless projection over the `workerkit` domain layer, so you can replace it
too.

Docs: [goals](docs/01-goals.md), [gateway](docs/02-gateway.md),
[embedding](docs/07-embedding.md), [operations](docs/08-operations.md),
[TLA+ model](docs/09-formal-model.md),
[framework contract](docs/10-framework-contract.md),
[the human-side stack](docs/11-human-side.md).

Status, honestly: OpenCode 1.17.18 on a single machine is the validated path.
Codex is partially validated; Claude Code has a verified basic text loop, workspace tier pending.
