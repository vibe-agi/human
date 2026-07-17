// Package dialect defines the pure wire-codec boundary for model APIs.
package dialect

import (
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
)

type Codec interface {
	Dialect() canonical.Dialect
	Decode([]byte) (canonical.Request, error)
	NewStream(responseID, model string, seed ...StreamSeed) Stream
	NewAggregate(responseID, model string, seed ...StreamSeed) Aggregate
	AdmissionError(status int, code, message string) []byte
	OverloadedStatus() int
}

// StreamSeed contains the wall-clock input that affects stable wire fields.
// The gateway persists it before exposing the response so a restarted codec
// can reproduce the exact same stream rather than sampling a new clock.
type StreamSeed struct {
	CreatedAtUnix  int64
	ToolCallPolicy canonical.ToolCallPolicy
}

// EventSeed is persisted with each worker-event checkpoint. It makes any
// event-local clock fields reproducible when recovery replays the codec state.
type EventSeed struct {
	EncodedAtUnix int64
}

// Encoder is the durable, dialect-specific projection of canonical worker
// events. Streaming encoders return observable frames as work progresses;
// aggregate encoders retain that state internally and return exactly one JSON
// body at the terminal event. The gateway persists every event and its encoded
// bytes, so either implementation can be reconstructed after a crash without
// parsing its own wire format.
type Encoder interface {
	Start() ([][]byte, error)
	Heartbeat() []byte
	Encode(completion.Event, ...EventSeed) (frames [][]byte, done bool, err error)
}

type Stream interface{ Encoder }

type Aggregate interface{ Encoder }
