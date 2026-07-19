package agent

import (
	"crypto/sha256"
	"strconv"
	"testing"
)

func TestWorkerEventReceiptCacheEvictsOldestAtBound(t *testing.T) {
	connection := &agentWorkerConnection{
		receipts: make(map[WorkerDeliveryID]workerEventReceiptState),
	}
	digest := sha256.Sum256([]byte("receipt"))
	first := WorkerDeliveryID("receipt-0")
	for index := 0; index <= workerEventReceiptLimit; index++ {
		id := WorkerDeliveryID("receipt-" + strconv.Itoa(index))
		connection.rememberEventReceipt(id, digest, WorkerEventReceipt{
			Delivery: id,
			Event:    CommandID("event-" + strconv.Itoa(index)),
			Decision: WorkerEventACK,
		})
	}
	if len(connection.receipts) != workerEventReceiptLimit {
		t.Fatalf("receipt cache size = %d, want %d", len(connection.receipts), workerEventReceiptLimit)
	}
	if _, exists := connection.receipts[first]; exists {
		t.Fatal("oldest receipt remained cached after the bound was exceeded")
	}
}
