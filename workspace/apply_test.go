package workspace_test

import (
	"strings"
	"testing"

	"github.com/vibe-agi/human/workspace"
)

func TestIndeterminateOutcomeAlwaysReturnsValidBoundedMetadata(t *testing.T) {
	code := strings.Repeat("故障", 100) + "\x00\r\n"
	message := strings.Repeat("未知", 3000) + "\x00"
	outcome := workspace.IndeterminateOutcome(code, message)
	intent := workspace.ApplyIntent{
		ResultRevision: "result-revision",
	}
	if err := workspace.ValidateCASOutcome(outcome, intent); err != nil {
		t.Fatalf("IndeterminateOutcome returned invalid metadata: %v", err)
	}
	if len(outcome.Code) > 128 || len(outcome.Message) > 4096 {
		t.Fatalf("IndeterminateOutcome exceeded bounds: code=%d message=%d", len(outcome.Code), len(outcome.Message))
	}
}
