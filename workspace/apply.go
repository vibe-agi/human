package workspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const absoluteApplyPayloadMax = 64 << 20

var applyIdentityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

var (
	ErrApplyNotFound       = errors.New("workspace Artifact apply is not recorded")
	ErrApplyIntentConflict = errors.New("workspace Artifact identity was reused with different apply input")
	ErrApplyJournalInUse   = errors.New("workspace apply journal is already held by another live owner")
	ErrApplyJournalClosed  = errors.New("workspace apply journal is closed")
	ErrInvalidApply        = errors.New("invalid workspace Artifact apply")
	ErrCorruptApplyJournal = errors.New("corrupt workspace apply journal")
)

// ApplyIdentity is the caller-side idempotency key for applying one immutable
// Artifact. Authority prevents identities issued by independent security
// domains from colliding; Workspace identifies the revision chain; Artifact is
// the immutable object being applied to that chain.
type ApplyIdentity struct {
	Authority string `json:"authority"`
	Workspace string `json:"workspace"`
	Artifact  string `json:"artifact"`
}

// ApplyIntent is the complete, transport-neutral input to one caller-side CAS.
// Every field participates in exact-retry comparison. Payload is declarative
// data; an applier must not infer permission to execute it.
type ApplyIntent struct {
	Identity       ApplyIdentity `json:"identity"`
	ArtifactDigest Digest        `json:"artifact_digest"`
	PayloadDigest  Digest        `json:"payload_digest"`
	BaseRevision   Revision      `json:"base_revision"`
	ResultRevision Revision      `json:"result_revision"`
	Payload        Payload       `json:"payload"`
}

// CASOutcome is the externally observed result of applying an Artifact. A
// success is valid only when ObservedRevision is exactly the intended result.
// Indeterminate is terminal and must be reconciled with a new Artifact rather
// than by replaying this intent.
type CASOutcome struct {
	Decision         ApplyDecision `json:"decision"`
	ObservedRevision Revision      `json:"observed_revision,omitempty"`
	Code             string        `json:"code,omitempty"`
	Message          string        `json:"message,omitempty"`
}

// CASApplier owns the external compare-and-swap boundary. ApplyCAS may change
// state before returning, so every returned error is treated as an unknown
// external outcome and durably recorded as ApplyIndeterminate. A callback may
// call Lookup, but must not synchronously call Close or recursively Apply the
// same identity: both operations would wait for the callback itself.
type CASApplier interface {
	ApplyCAS(context.Context, ApplyIntent) (CASOutcome, error)
}

// CASApplierFunc makes a function usable as a CASApplier.
type CASApplierFunc func(context.Context, ApplyIntent) (CASOutcome, error)

func (function CASApplierFunc) ApplyCAS(ctx context.Context, intent ApplyIntent) (CASOutcome, error) {
	return function(ctx, intent)
}

// ApplyState is the durable lifecycle of one apply intent. Pending is never
// safe to execute after a process boundary; OpenSQLiteApplyJournal atomically
// converts every recovered pending row to indeterminate.
type ApplyState string

const (
	ApplyStatePending       ApplyState = "pending"
	ApplyStateSuccess       ApplyState = "success"
	ApplyStateConflict      ApplyState = "conflict"
	ApplyStateRejected      ApplyState = "rejected"
	ApplyStateIndeterminate ApplyState = "indeterminate"
)

func (state ApplyState) Terminal() bool {
	return state == ApplyStateSuccess || state == ApplyStateConflict ||
		state == ApplyStateRejected || state == ApplyStateIndeterminate
}

func stateForDecision(decision ApplyDecision) ApplyState {
	return ApplyState(decision)
}

// ApplyRecord is the durable caller-side evidence for an Artifact apply. A
// pending record has a nil Outcome and CompletedAt. Every terminal record has
// both. IntentDigest protects the exact canonical Intent bytes at rest.
type ApplyRecord struct {
	Intent       ApplyIntent `json:"intent"`
	IntentDigest Digest      `json:"intent_digest"`
	State        ApplyState  `json:"state"`
	Outcome      *CASOutcome `json:"outcome,omitempty"`
	CreatedAt    time.Time   `json:"created_at"`
	CompletedAt  *time.Time  `json:"completed_at,omitempty"`
}

// ApplyResult reports whether this call performed the external CAS or replayed
// a previously durable terminal record.
type ApplyResult struct {
	Record ApplyRecord `json:"record"`
	Replay bool        `json:"replay"`
}

// ApplyJournal is the embeddable caller-side durability boundary.
type ApplyJournal interface {
	Apply(context.Context, ApplyIntent, CASApplier) (ApplyResult, error)
	Lookup(context.Context, ApplyIdentity) (ApplyRecord, error)
	Close() error
}

// DigestPayload returns the canonical digest used by ApplyIntent. It hashes the
// JSON representation of both media type and bytes, so changing either changes
// the workspace identity.
func DigestPayload(payload Payload) Digest {
	encoded, _ := json.Marshal(payload)
	return sha256Digest(encoded)
}

func cloneApplyIntent(intent ApplyIntent) ApplyIntent {
	intent.Payload.Data = bytes.Clone(intent.Payload.Data)
	return intent
}

func cloneCASOutcome(outcome CASOutcome) CASOutcome {
	return outcome
}

func cloneApplyRecord(record ApplyRecord) ApplyRecord {
	record.Intent = cloneApplyIntent(record.Intent)
	if record.Outcome != nil {
		outcome := cloneCASOutcome(*record.Outcome)
		record.Outcome = &outcome
	}
	if record.CompletedAt != nil {
		completed := *record.CompletedAt
		record.CompletedAt = &completed
	}
	return record
}

func canonicalApplyIntent(intent ApplyIntent) ([]byte, Digest, error) {
	intent = cloneApplyIntent(intent)
	if err := validateApplyIntent(intent); err != nil {
		return nil, "", err
	}
	encoded, err := json.Marshal(intent)
	if err != nil {
		return nil, "", fmt.Errorf("%w: encode intent: %v", ErrInvalidApply, err)
	}
	return encoded, sha256Digest(encoded), nil
}

func validateApplyIntent(intent ApplyIntent) error {
	if err := validateApplyIdentity(intent.Identity); err != nil {
		return err
	}
	if err := validateApplyDigest("artifact digest", intent.ArtifactDigest); err != nil {
		return err
	}
	if err := validateApplyDigest("payload digest", intent.PayloadDigest); err != nil {
		return err
	}
	if intent.PayloadDigest != DigestPayload(intent.Payload) {
		return fmt.Errorf("%w: payload digest does not identify the exact payload", ErrInvalidApply)
	}
	if err := validateApplyRevision("base revision", intent.BaseRevision); err != nil {
		return err
	}
	if err := validateApplyRevision("result revision", intent.ResultRevision); err != nil {
		return err
	}
	if intent.BaseRevision == intent.ResultRevision {
		return fmt.Errorf("%w: base and result revisions must differ", ErrInvalidApply)
	}
	mediaType := intent.Payload.MediaType
	if mediaType == "" || mediaType != strings.TrimSpace(mediaType) || len(mediaType) > 128 ||
		!utf8.ValidString(mediaType) || strings.ContainsAny(mediaType, "\r\n\x00") {
		return fmt.Errorf("%w: payload media type is invalid", ErrInvalidApply)
	}
	if _, _, err := mime.ParseMediaType(mediaType); err != nil {
		return fmt.Errorf("%w: payload media type is invalid: %v", ErrInvalidApply, err)
	}
	if len(intent.Payload.Data) == 0 || len(intent.Payload.Data) > absoluteApplyPayloadMax {
		return fmt.Errorf("%w: payload must be 1..%d bytes", ErrInvalidApply, absoluteApplyPayloadMax)
	}
	return nil
}

func validateApplyIdentity(identity ApplyIdentity) error {
	for label, value := range map[string]string{
		"authority": identity.Authority,
		"workspace": identity.Workspace,
		"artifact":  identity.Artifact,
	} {
		if !applyIdentityPattern.MatchString(value) {
			return fmt.Errorf("%w: %s must match %s", ErrInvalidApply, label, applyIdentityPattern.String())
		}
	}
	return nil
}

func validateApplyDigest(label string, digest Digest) error {
	value := string(digest)
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidApply, label)
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	if err != nil || "sha256:"+hex.EncodeToString(decoded) != value {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidApply, label)
	}
	return nil
}

func validateApplyRevision(label string, revision Revision) error {
	value := string(revision)
	if value == "" || len(value) > 256 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidApply, label)
	}
	return nil
}

func validateCASOutcome(outcome CASOutcome, intent ApplyIntent) error {
	if !outcome.Decision.Valid() {
		return fmt.Errorf("%w: CAS decision is invalid", ErrInvalidApply)
	}
	if outcome.ObservedRevision != "" {
		if err := validateApplyRevision("observed revision", outcome.ObservedRevision); err != nil {
			return err
		}
	}
	if outcome.Decision == ApplySuccess && outcome.ObservedRevision != intent.ResultRevision {
		return fmt.Errorf("%w: success must observe the exact result revision", ErrInvalidApply)
	}
	if outcome.Decision == ApplyConflict && outcome.ObservedRevision == "" {
		return fmt.Errorf("%w: conflict must include the observed revision", ErrInvalidApply)
	}
	if len(outcome.Code) > 128 || !utf8.ValidString(outcome.Code) || strings.ContainsAny(outcome.Code, "\x00\r\n") {
		return fmt.Errorf("%w: CAS outcome code is invalid", ErrInvalidApply)
	}
	if len(outcome.Message) > 4096 || !utf8.ValidString(outcome.Message) || strings.ContainsRune(outcome.Message, '\x00') {
		return fmt.Errorf("%w: CAS outcome message is invalid", ErrInvalidApply)
	}
	return nil
}

func indeterminateOutcome(code, message string) CASOutcome {
	message = strings.ToValidUTF8(message, "�")
	message = strings.ReplaceAll(message, "\x00", "�")
	if len(message) > 4096 {
		message = truncateUTF8(message, 4096)
	}
	return CASOutcome{Decision: ApplyIndeterminate, Code: code, Message: message}
}

func truncateUTF8(value string, maximum int) string {
	if len(value) <= maximum {
		return value
	}
	value = value[:maximum]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func sha256Digest(value []byte) Digest {
	sum := sha256.Sum256(value)
	return Digest("sha256:" + hex.EncodeToString(sum[:]))
}
