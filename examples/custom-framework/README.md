# Custom HumanLLM framework embedding

This executable runs one aggregate OpenAI Chat request entirely in-process. It
does not open a socket, call an external model, or depend on package-global
state. The example shows that HumanLLM is a correctness core assembled from
replaceable ports rather than a server that owns an application.

```sh
go run ./examples/custom-framework
```

The composition has four application-defined adapters:

- `customstore` contains a genuinely application-owned physical Store opened by
  `customstore.Open`: it implements every `llm.StoreView`/`llm.StoreTx` method
  itself and persists a versioned, SHA-256-checked snapshot with fsync plus
  atomic replacement. Its direct `humantest.TestLLMStore` run, release/reopen
  test, and truncation/tamper tests show how an application can implement the
  public contract without importing Human internals. It is intentionally a
  compact, single-owner teaching adapter (every commit rewrites the complete
  image), not a production database. It fences a second cooperating process
  with an advisory lock on supported Unix systems and fails closed on other
  systems; its parent directory remains a trusted private boundary. The
  executable uses this physical adapter directly. The same package retains
  `Own`/`Borrow` as a separate
  policy-middleware example. A Postgres, MySQL, service API, or other
  conforming Store can replace either choice without changing `human.NewLLM`.
- `tokenAuthenticator` maps a verified token to `llm.CallerID`. Caller identity
  is absent from the inbound `call` value, so untrusted input cannot select its
  own authority. A real embedding can use its existing session service, mTLS,
  IAM, or tenant database here.
- `customprotect` is a separate application package implementing
  `protect.Protector`. It safely decorates the official AES-256-GCM keyring,
  preserves its authenticated persisted envelope identity, defensively copies
  byte ownership, and emits only low-cardinality metadata (never authority,
  record IDs, keys, plaintext, or ciphertext). It demonstrates extension at the
  real protection port without introducing toy cryptography.
- `inProcessTransport` implements `llm.CallerTransport` and projects ordinary
  Go method calls onto `llm.CallerEndpoint`. A queue, gRPC service, Unix socket,
  or proprietary RPC adapter can implement the same lifecycle and replay
  contract. It verifies authenticated admission identity, request digest,
  response mode, monotonic cursors, ordered event sequences, and the immutable
  first committed response decision instead of trusting an endpoint blindly.
  Events before that decision, aggregate events, stream decision bodies, and
  later decision changes are rejected. A safe `llm.AdmissionError` becomes an
  ordinary `callResult` containing only status, content type, retry hint, and a
  copied body; its `Cause` is never exposed, and unknown endpoint errors collapse
  to one fixed adapter error. The example supports both aggregate bodies and
  ordered stream frames even though the demonstrated request is aggregate.

## Ownership

Ownership is explicit at every boundary:

- `customstore.Open` creates an **owned** application Store resource and the
  example transfers it directly to `human.NewLLM`. HumanLLM releases it on
  shutdown (or constructor failure); the host must not also release it.
- `customprotect.OpenLocal` similarly creates an **owned** decorated Protector.
  Its release callback releases the underlying AEAD resource and wipes the
  keyring's copied material. That resource is transferred through
  `LLMConfig.Protector`, so HumanLLM releases the Protector before the Store.
  `customprotect.Borrow` shows the other ownership choice: use it when the host
  owns a shared KMS/HSM client, keep that client alive until HumanLLM reaches
  `Done`, and close it only afterward.
- The host owns the caller transport runtime and shuts it down before HumanLLM.
  The transport only **borrows** `llm.CallerEndpoint`; its `Shutdown` cancels
  active adapter waits and joins them, but never shuts down HumanLLM.
- The host opens and owns the direct local `llm.WorkerConnection`. The embedded
  worker shares HumanLLM's failure domain and uses the core's durable assignment
  as its journal before ACK. A remote worker transport must instead ACK only
  after its own durable assignment journal commit.
- `tokenAuthenticator` is borrowed by the transport. A stateful authentication
  adapter remains host-owned and must stay alive until transport shutdown.

For production, use a production-grade database-backed Store, a
real authentication authority, a transport with bounded messages and durable
reconnect/replay behavior, and a `protect.Protector` backed by the deployment's
KMS/HSM. This executable's random key is appropriate only because its temporary
snapshot is removed when the process exits; durable storage requires the same
historical key IDs and versions to remain retrievable across restart. A native KMS/HSM
implementation should define and authenticate its own stable Provider/Format
envelope identity; a transparent decorator such as this example must preserve
the underlying identity rather than merely relabeling ciphertext. The HumanLLM
core and worker event flow stay unchanged.
