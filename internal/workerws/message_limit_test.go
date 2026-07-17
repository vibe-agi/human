package workerws

import (
	"testing"

	"github.com/vibe-agi/human/internal/workerproto"
)

func TestDefaultReadLimitUsesProtocolWireBudget(t *testing.T) {
	if got := (Config{}).withDefaults().ReadLimit; got != workerproto.MaxWireMessageBytes {
		t.Fatalf("default worker read limit = %d, want %d", got, workerproto.MaxWireMessageBytes)
	}
}
