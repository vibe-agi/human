# Human

> The world's slowest LLM. Astonishing reasoning. Terrible latency. Unionized.

[简体中文](README.zh-CN.md)

## What is this

Every AI product pitch says "AI replaces humans." We went the other way:
**this project replaces the AI with a human.** You.

Human is an OpenAI/Anthropic-compatible model server where the "model" is a
person with a browser tab. Your coding agent (OpenCode, or anything that
speaks Chat Completions / Messages / Responses) calls `POST
/v1/chat/completions` like always — and the request lands in a human's inbox.
The human reads it, thinks (weights: ~86 billion neurons, training data: one
childhood), types an answer, maybe sends back *native tool calls* that the
agent executes in its own workspace, and clicks "Deliver final."

The agent never knows. It gets a perfectly ordinary SSE stream. It says
"thank you" to what it believes is a matrix multiplication.

Model card, for honesty:

| Metric | Value |
|---|---|
| Parameters | 1 human |
| Context window | depends on sleep |
| Tokens/sec | 2, on a good day |
| Hallucination rate | nonzero, but apologizes |
| Alignment | negotiable |

## Why would anyone want this

Jokes aside — dropping a human into the model slot is genuinely useful:

- **Escalation**: your agent is stuck at 2am; a senior engineer takes over
  the *model* seat for one turn — with the agent's full tool loop, permission
  gates, and workspace intact. No screen sharing, no "paste me the error".
- **Human-in-the-loop that actually loops**: the human can answer, ask back,
  run commands *through the agent's own execution gate*, edit files in a live
  mirror and deliver them as native `write`/`edit` tool calls — the agent's
  working tree stays the single source of truth.
- **Ground truth**: record what a competent human does in the model seat, and
  you have evaluation data no benchmark sells you.
- **A durable task surface too**: besides the real-time HumanLLM, there's
  HumanAgent — an A2A 1.0 endpoint with durable tasks, artifacts, and
  clock-free leases, for work that takes longer than a coffee.

Everything is fail-closed, byte-exact idempotent, crash-recovered through
durable outboxes, and modeled in TLA+ — because if the human is going to be
slow, the plumbing at least should be correct. (Details: [docs/](docs/), 90
formal gates, real-CLI fault-injection doors. We are not joking about this
part.)

## Quickstart: become a model in 60 seconds

Requirements: Go (or a release binary), a browser, one human (you qualify).

**Terminal 1 — start the server + your inbox:**

```sh
human local --workspace .
# ...
# model base URL: http://127.0.0.1:19080/v1
# human side (browser): http://127.0.0.1:19081/?token=...   ← open this
```

Open the printed URL. That's your cockpit: inbox, conversations, a command
console, a todo planner, and a Live Workspace review panel. English by
default; 中文 one click away.

**Terminal 2 — point an agent at yourself.** For OpenCode, add a provider:

```jsonc
// opencode.json
"human": {
  "npm": "@ai-sdk/openai-compatible",
  "name": "Human (me)",
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

Ask OpenCode anything. Your browser pings. You are now the model. Breathe.

**No agent handy? curl yourself:**

```sh
curl -N http://127.0.0.1:19080/v1/chat/completions \
  -H "Authorization: Bearer $HUMAN_CALLER_TOKEN" \
  -H "Content-Type: application/json" -H "Idempotency-Key: try-1" \
  -d '{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"hello, model"}]}'
```

The curl hangs politely until you answer in the browser. Congratulations:
you have achieved artificial artificial intelligence.

## Remote / team

```sh
human gateway --listen :8080          # deployment side (put TLS in front)
human worker --gateway wss://your-gateway/internal/v1/worker/ws
# prints your browser inbox URL; token via HUMAN_GATEWAY_TOKEN
```

## Embedding (for Go people)

`human.NewLLM()` / `human.NewAgent()` are transport-neutral cores with
replaceable Store / auth / codec / transport / KMS ports, public conformance
suites in `humantest`, and a fully self-owned example in
[`examples/custom-framework`](examples/custom-framework/README.md). The web
UI is a stateless projection over the public `workerkit` domain layer — bring
your own UI if ours offends you.

## The fine print

| Doc | What |
|---|---|
| [01 Goals](docs/01-goals.md) · [02 Gateway](docs/02-gateway.md) · [05 Contract](docs/05-m0-contract.md) | Product and protocol boundaries |
| [07 Embedding](docs/07-embedding.md) · [10 Framework contract](docs/10-framework-contract.md) | Library ports, conformance, ownership |
| [08 Operations](docs/08-operations.md) · [09 TLA+](docs/09-formal-model.md) · [11 Human side](docs/11-human-side.md) | Backup/restore, formal model, the web worker stack |

Honest status: OpenCode 1.17.18 single-machine is the validated path (real
CLI doors, network fault gates). Codex is partially validated, Claude is
codec-only so far. The human, as shipped, is not fine-tunable.
