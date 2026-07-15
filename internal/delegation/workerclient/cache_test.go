package workerclient

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/delegation"
	"github.com/vibe-agi/human/internal/delegation/workerproto"
)

func TestRecoveryCacheNeverRegressesTaskRevision(t *testing.T) {
	client := &Client{snapshots: make(map[string]delegation.Snapshot)}
	newer := delegation.Snapshot{Task: delegation.Task{ID: "task-1", Revision: 3, State: delegation.StateInputRequired}}
	if !client.cacheSnapshot(newer) {
		t.Fatal("new snapshot was ignored")
	}
	older := delegation.Snapshot{Task: delegation.Task{ID: "task-1", Revision: 2, State: delegation.StateWorking}}
	if client.cacheSnapshot(older) {
		t.Fatal("stale recovery snapshot was accepted")
	}
	client.cacheResult(workerproto.CommandResult{
		EventID: "event-old", Kind: workerproto.CommandAccept,
		Transition: &delegation.TransitionResult{
			Task: delegation.Task{ID: "task-1", Revision: 2, State: delegation.StateWorking},
		},
	})
	got, err := client.GetTask(nil, "task-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != 3 || got.State != delegation.StateInputRequired {
		t.Fatalf("cached task regressed to %#v", got)
	}
}

func TestExecResultUpdatesCacheWithoutAliasing(t *testing.T) {
	client := &Client{snapshots: make(map[string]delegation.Snapshot)}
	client.snapshots["task-1"] = delegation.Snapshot{
		Task: delegation.Task{ID: "task-1", Revision: 2, State: delegation.StateWorking},
	}
	exitCode := 0
	resolutionSequence := int64(4)
	resolvedAt := time.Unix(10, 0).UTC()
	result := workerproto.CommandResult{
		EventID: "exec-event", Kind: workerproto.CommandExec,
		Exec: &delegation.ExecResult{
			Task: delegation.Task{ID: "task-1", Revision: 4, State: delegation.StateWorking},
			Request: delegation.ExecRequest{
				TaskID: "task-1", ID: "request-1", WorkerID: "worker-1", Command: "pwd",
				Reason: "inspect", Status: delegation.ExecCompleted, ExitCode: &exitCode,
				Stdout: []byte("ok"), ResolutionSequence: &resolutionSequence, ResolvedAt: &resolvedAt,
			},
			Event: delegation.Event{TaskID: "task-1", Sequence: 4, Kind: delegation.EventExecCompleted, Data: []byte("event")},
		},
	}
	client.cacheResult(result)

	result.Exec.Request.Stdout[0] = 'x'
	*result.Exec.Request.ExitCode = 1
	*result.Exec.Request.ResolutionSequence = 5
	result.Exec.Event.Data[0] = 'X'
	cached := client.snapshots["task-1"]
	if cached.Task.Revision != 4 || len(cached.Exec) != 1 || string(cached.Exec[0].Stdout) != "ok" ||
		cached.Exec[0].ExitCode == nil || *cached.Exec[0].ExitCode != 0 ||
		cached.Exec[0].ResolutionSequence == nil || *cached.Exec[0].ResolutionSequence != 4 ||
		len(cached.Events) != 1 || string(cached.Events[0].Data) != "event" {
		t.Fatalf("cached exec result aliases input: %#v", cached)
	}
}

func TestRequestExecRejectsClaimedWorkerMismatchLocally(t *testing.T) {
	client := &Client{workerID: "worker-1"}
	_, err := client.RequestExec(context.Background(), delegation.RequestExecInput{WorkerID: "worker-2"})
	if !errors.Is(err, ErrOwnershipConflict) {
		t.Fatalf("worker mismatch error = %v", err)
	}
}
