# Custom HumanLLM framework embedding

This executable runs one aggregate OpenAI Chat request entirely in-process. It
does not open a socket, call an external model, or depend on package-global
state. The example shows that HumanLLM is a correctness core assembled from
replaceable ports rather than a server that owns an application.

```sh
go run ./examples/custom-framework
```

The composition has four application-defined adapters:

- `auditedStore` explicitly forwards the complete `llm.Store` contract to the
  official `llm/sqlite` implementation. Its `Description` advertises the
  application provider while retaining a frozen negotiated copy of the
  underlying contract and features. `Bind`, `View`, and `Update` are all
  forwarded explicitly. Its audit policy records operation names without
  logging customer payloads. Replace the wrapped SQLite store with Postgres,
  MySQL, a service API, or another conforming store without changing
  `human.NewLLM`.
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
  response mode, monotonic cursors, and ordered event sequences instead of
  trusting an endpoint blindly. The example supports both aggregate bodies and
  ordered stream frames even though the demonstrated request is aggregate.

## Ownership

Ownership is explicit at every boundary:

- `llm/sqlite.Open` creates an **owned** SQLite resource. The example transfers
  it into an owned `auditedStore` resource, then transfers that resource to
  `human.NewLLM`. HumanLLM releases it on shutdown (or constructor failure).
  The host must not also close the decorator or SQLite database.
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

For production, use a file-backed database path (or a custom durable Store), a
real authentication authority, a transport with bounded messages and durable
reconnect/replay behavior, and a `protect.Protector` backed by the deployment's
KMS/HSM. This executable's random key is appropriate only because its SQLite
database is also ephemeral; durable storage requires the same historical key
IDs and versions to remain retrievable across restart. A native KMS/HSM
implementation should define and authenticate its own stable Provider/Format
envelope identity; a transparent decorator such as this example must preserve
the underlying identity rather than merely relabeling ciphertext. The HumanLLM
core and worker event flow stay unchanged.
