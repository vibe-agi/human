// Package dialect defines the pure wire-codec boundary for model APIs.
package dialect

import (
	"errors"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
)

var ErrUnsupportedNonStreaming = errors.New("non-streaming completion requests are not supported")

type Codec interface {
	Dialect() canonical.Dialect
	Decode([]byte) (canonical.Request, error)
	NewStream(responseID, model string, seed ...StreamSeed) Stream
	AdmissionError(status int, code, message string) []byte
	OverloadedStatus() int
}

// StreamSeed contains the wall-clock input that affects stable wire fields.
// The gateway persists it before exposing the response so a restarted codec
// can reproduce the exact same stream rather than sampling a new clock.
type StreamSeed struct {
	CreatedAtUnix int64
}

// EventSeed is persisted with each worker-event checkpoint. It makes any
// event-local clock fields reproducible when recovery replays the codec state.
type EventSeed struct {
	EncodedAtUnix int64
}

type Stream interface {
	Start() ([][]byte, error)
	Heartbeat() []byte
	Encode(completion.Event, ...EventSeed) (frames [][]byte, done bool, err error)
}
