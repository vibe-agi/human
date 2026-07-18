package a2a

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/vibe-agi/human/agent"
)

func TestTaskCursorRoundTripAndEncodedSizeGate(t *testing.T) {
	want := agent.TaskQueryCursor{
		UpdatedAt: time.Date(2026, time.July, 19, 12, 0, 0, 123, time.UTC),
		Workspace: "workspace-a", Task: "task-a",
	}
	token, err := encodeTaskCursor(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeTaskCursor(token)
	if err != nil || got == nil || *got != want {
		t.Fatalf("decoded cursor = %#v, %v; want %#v", got, err, want)
	}

	oversized := strings.Repeat("A", base64.RawURLEncoding.EncodedLen(maxTaskCursorBytes)+1)
	if _, err := decodeTaskCursor(oversized); !errors.Is(err, sdk.ErrInvalidParams) {
		t.Fatalf("oversized token error = %v", err)
	}
	if _, err := decodeTaskCursor("not+url/base64"); !errors.Is(err, sdk.ErrInvalidParams) {
		t.Fatalf("invalid token error = %v", err)
	}
	invalid, err := encodeTaskCursor(agent.TaskQueryCursor{
		UpdatedAt: want.UpdatedAt, Workspace: "bad workspace", Task: want.Task,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeTaskCursor(invalid); !errors.Is(err, sdk.ErrInvalidParams) {
		t.Fatalf("invalid cursor identity error = %v", err)
	}
}
