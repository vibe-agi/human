package hub

import (
	"testing"
	"time"
)

func TestSnapshotTracksOnlineWorkers(t *testing.T) {
	workerHub := New(4)
	if snapshot := workerHub.Snapshot(); snapshot.OnlineWorkers != 0 || snapshot.HasWorker {
		t.Fatalf("empty snapshot = %+v", snapshot)
	}

	first, err := workerHub.Register("worker-1")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := workerHub.Register("worker-2")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if snapshot := workerHub.Snapshot(); snapshot.OnlineWorkers != 2 || !snapshot.HasWorker {
		t.Fatalf("online snapshot = %+v", snapshot)
	}

	first.Close()
	if snapshot := workerHub.Snapshot(); snapshot.OnlineWorkers != 1 || !snapshot.HasWorker {
		t.Fatalf("snapshot after first close = %+v", snapshot)
	}
	second.Close()
	if snapshot := workerHub.Snapshot(); snapshot.OnlineWorkers != 0 || snapshot.HasWorker {
		t.Fatalf("snapshot after all close = %+v", snapshot)
	}
}

func TestSnapshotDoesNotWaitForHubMutex(t *testing.T) {
	workerHub := New(1)
	worker, err := workerHub.Register("worker-1")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	workerHub.mu.Lock()
	result := make(chan Snapshot, 1)
	go func() { result <- workerHub.Snapshot() }()
	select {
	case snapshot := <-result:
		workerHub.mu.Unlock()
		if snapshot.OnlineWorkers != 1 || !snapshot.HasWorker {
			t.Fatalf("snapshot while hub mutex held = %+v", snapshot)
		}
	case <-time.After(time.Second):
		workerHub.mu.Unlock()
		t.Fatal("Snapshot blocked on the hub mutex")
	}
}
