package workerproto

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/delegation"
)

func TestEnvelopeAndCommandValidation(t *testing.T) {
	envelope, err := NewEnvelope(MessageCommand, Command{
		EventID: "event-1", Kind: CommandAccept, TaskID: "task-1", ExpectedRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope.Seq = 1
	if err := envelope.Validate(); err != nil {
		t.Fatal(err)
	}
	if !json.Valid(envelope.Payload) {
		t.Fatalf("invalid payload: %s", envelope.Payload)
	}

	for name, command := range map[string]Command{
		"missing identity": {Kind: CommandAccept, TaskID: "task-1", ExpectedRevision: 1},
		"unknown kind":     {EventID: "e", Kind: "unknown", TaskID: "task-1", ExpectedRevision: 1},
		"missing delivery": {EventID: "e", Kind: CommandDeliver, TaskID: "task-1", ExpectedRevision: 1},
		"extra delivery": {
			EventID: "e", Kind: CommandComplete, TaskID: "task-1", ExpectedRevision: 1,
			Delivery: &Delivery{ArtifactID: "a", ArtifactMediaType: "text/plain"},
		},
		"missing exec": {EventID: "e", Kind: CommandExec, TaskID: "task-1", ExpectedRevision: 1},
		"exec with delivery": {
			EventID: "e", Kind: CommandExec, TaskID: "task-1", ExpectedRevision: 1,
			Delivery: &Delivery{ArtifactID: "a", ArtifactMediaType: "text/plain"},
			Exec:     &ExecRequest{RequestID: "r", Command: "pwd", Reason: "inspect"},
		},
		"non-canonical exec": {
			EventID: "e", Kind: CommandExec, TaskID: "task-1", ExpectedRevision: 1,
			Exec: &ExecRequest{RequestID: " r", Command: "pwd", Reason: "inspect"},
		},
		"empty command": {
			EventID: "e", Kind: CommandExec, TaskID: "task-1", ExpectedRevision: 1,
			Exec: &ExecRequest{RequestID: "r", Command: " \t", Reason: "inspect"},
		},
		"command nul": {
			EventID: "e", Kind: CommandExec, TaskID: "task-1", ExpectedRevision: 1,
			Exec: &ExecRequest{RequestID: "r", Command: "pwd\x00", Reason: "inspect"},
		},
		"timeout too long": {
			EventID: "e", Kind: CommandExec, TaskID: "task-1", ExpectedRevision: 1,
			Exec: &ExecRequest{RequestID: "r", Command: "pwd", Reason: "inspect", TimeoutMS: 60*60*1000 + 1},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := command.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}

	exec := Command{
		EventID: "exec-event", Kind: CommandExec, TaskID: "task-1", ExpectedRevision: 1,
		Exec: &ExecRequest{RequestID: "request-1", Command: "go test ./...", CWD: ".", TimeoutMS: 30_000, Reason: "verify change"},
	}
	if err := exec.Validate(); err != nil {
		t.Fatalf("valid exec command: %v", err)
	}
	encoded, err := json.Marshal(exec)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "worker_id") {
		t.Fatalf("exec wire payload leaks worker identity: %s", encoded)
	}

	result := CommandResult{
		EventID: "exec-event", Kind: CommandExec,
		Exec: &delegation.ExecResult{Task: delegation.Task{ID: "task-1"}},
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("valid exec result: %v", err)
	}
	result.Delivery = &delegation.DeliveryResult{}
	if err := result.Validate(); err == nil {
		t.Fatal("expected mixed exec result validation error")
	}
}
