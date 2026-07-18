// Package workspace contains transport-neutral identities and the HumanAgent
// caller-side apply journal. HumanLLM does not yet use these values or join the
// same durable revision/apply chain. Payload values remain declarative;
// filesystem mutation belongs to an explicitly supplied, authorized CASApplier.
package workspace

// Revision is an opaque workspace chain identity. Implementations compare it
// for exact equality; they must not infer ordering from its contents.
type Revision string

// Digest is a content identity in algorithm:value form.
type Digest string

// Payload is a declarative workspace artifact. Consumers must treat Data as
// data, never as an implicit command to execute; this value type does not
// enforce a media-type allowlist. Executable effects need their own authorized,
// idempotent protocol.
type Payload struct {
	MediaType string `json:"media_type"`
	Data      []byte `json:"data"`
}

// ApplyDecision is the caller-side durable outcome of applying one exact
// Artifact. Indeterminate is terminal: reconciliation requires a new Task and
// Artifact rather than replaying a possibly completed external side effect.
type ApplyDecision string

const (
	ApplySuccess       ApplyDecision = "success"
	ApplyConflict      ApplyDecision = "conflict"
	ApplyRejected      ApplyDecision = "rejected"
	ApplyIndeterminate ApplyDecision = "indeterminate"
)

func (decision ApplyDecision) Valid() bool {
	switch decision {
	case ApplySuccess, ApplyConflict, ApplyRejected, ApplyIndeterminate:
		return true
	default:
		return false
	}
}
