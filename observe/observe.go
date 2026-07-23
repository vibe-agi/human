// Package observe defines the shared observability port for Human runtimes.
//
// An Observer receives typed runtime events from a core (llm.Service,
// workerkit.Worker) for logging, metrics, or tracing. It is deliberately not
// a correctness surface:
//
//   - Events are emitted outside Store callbacks and after the durable
//     decision they describe; an Observer can never veto or reorder work.
//   - A slow or panicking Observer must not stall a runtime. Cores call
//     Observe synchronously on hot paths, so implementations must return
//     quickly — buffer or drop internally rather than block.
//   - A nil Observer everywhere means no-op; cores never require one.
//
// The event model is a single flat struct with a Kind discriminator rather
// than one type per event: hosts route on Kind, and the identity fields are
// stable, optional strings so adapters (slog, Prometheus, OTel) stay thin.
package observe

import (
	"log/slog"
	"time"
)

// Kind discriminates one runtime event.
type Kind string

const (
	// KindAdmissionAdmitted fires after a completion request is durably
	// admitted (or exactly replayed; Detail is "replay" then).
	KindAdmissionAdmitted Kind = "admission_admitted"
	// KindAdmissionRejected fires when admission fails before durable effects.
	// Detail carries the wire failure code (e.g. "worker_unavailable").
	KindAdmissionRejected Kind = "admission_rejected"
	// KindWorkerConnected and KindWorkerDisconnected track worker sessions.
	KindWorkerConnected    Kind = "worker_connected"
	KindWorkerDisconnected Kind = "worker_disconnected"
	// KindWorkerEventSettled fires when a worker event reaches its durable
	// settlement. Detail is "ack" or the rejection code.
	KindWorkerEventSettled Kind = "worker_event_settled"
	// KindRetentionCompleted fires after one explicit core retention sweep.
	// Detail contains a compact, non-secret count summary.
	KindRetentionCompleted Kind = "retention_completed"
	// KindExpiryCompleted fires after one explicit pending-request expiry sweep.
	// Detail contains a compact, non-secret count summary.
	KindExpiryCompleted Kind = "expiry_completed"

	// KindAssignmentArrived fires when the human side receives an assignment;
	// Detail is "inbox", "resumed", or "auto".
	KindAssignmentArrived Kind = "assignment_arrived"
	// KindCommandSent fires after a human command's event is durably owned by
	// the transport. Detail is the event type (progress, final, ...).
	KindCommandSent Kind = "command_sent"
	// KindRejectionReceived fires when a sent event is NACKed back.
	KindRejectionReceived Kind = "rejection_received"
	// KindAlert fires for conditions a human must see (e.g. a quarantined
	// outbox row whose reply will never be delivered).
	KindAlert Kind = "alert"
)

// Event is one observed runtime occurrence. Identity fields are optional and
// stable; Err is nil unless the event describes a failure.
type Event struct {
	Kind   Kind
	Time   time.Time
	Caller string
	Task   string
	Worker string
	Detail string
	Err    error
}

// Observer consumes events. Implementations must be safe for concurrent use
// and must not block; see the package contract.
type Observer interface {
	Observe(Event)
}

// Func adapts a function to Observer.
type Func func(Event)

func (function Func) Observe(event Event) { function(event) }

// NewSlog returns an Observer that logs each event at Info (Error for events
// carrying Err) with stable attribute names. A nil logger uses slog.Default.
func NewSlog(logger *slog.Logger) Observer {
	if logger == nil {
		logger = slog.Default()
	}
	return Func(func(event Event) {
		attributes := make([]any, 0, 12)
		attributes = append(attributes, "kind", string(event.Kind))
		if event.Caller != "" {
			attributes = append(attributes, "caller", event.Caller)
		}
		if event.Task != "" {
			attributes = append(attributes, "task", event.Task)
		}
		if event.Worker != "" {
			attributes = append(attributes, "worker", event.Worker)
		}
		if event.Detail != "" {
			attributes = append(attributes, "detail", event.Detail)
		}
		if event.Err != nil {
			attributes = append(attributes, "error", event.Err.Error())
			logger.Error("human observe", attributes...)
			return
		}
		logger.Info("human observe", attributes...)
	})
}

// Emit invokes observer with event, filling a zero Time and swallowing an
// Observer panic so instrumentation can never take down a correctness path.
// Cores call this instead of observer.Observe directly.
func Emit(observer Observer, event Event) {
	if observer == nil {
		return
	}
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	defer func() { _ = recover() }()
	observer.Observe(event)
}
