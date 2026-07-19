package humantest

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vibe-agi/human/agent"
	agentws "github.com/vibe-agi/human/agent/workerws"
	"github.com/vibe-agi/human/llm"
	llmws "github.com/vibe-agi/human/llm/workerws"
)

func TestMemoryAgentWorkerJournalAbandonRecoversCommittedImage(t *testing.T) {
	image := NewMemoryAgentWorkerJournalImage()
	first, releaseFirst, err := image.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	bindConformanceJournal(t.Context(), t, first)
	record := conformanceJournalEvent("abandon-agent-event", agent.WorkerEventAcceptTask, nil)
	if _, err := first.PutEvent(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	want := mustAgentRecoveryEvents(t.Context(), t, first, 1)
	if err := image.Abandon(first); err != nil {
		t.Fatal(err)
	}
	second, releaseSecond, err := image.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = releaseSecond(context.Background()) })
	if err := releaseFirst(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := mustAgentRecoveryEvents(t.Context(), t, second, 1)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("event after abandoned handle = %#v, want %#v", got, want)
	}
	divergent := record
	divergent.Delivery = agent.CloneWorkerEventDelivery(record.Delivery)
	divergent.Delivery.Event.ID += "-changed"
	divergent.Digest = mustJournalDigest(t, divergent.Delivery)
	if _, err := second.PutEvent(t.Context(), divergent); !errors.Is(err, agentws.ErrJournalConflict) {
		t.Fatalf("divergent replay after abandoned handle = %v, want ErrJournalConflict", err)
	}
}

func TestMemoryLLMWorkerJournalAbandonRecoversCommittedImage(t *testing.T) {
	image := NewMemoryLLMWorkerJournalImage()
	first, releaseFirst, err := image.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	bindLLMConformanceJournal(t.Context(), t, first)
	record := llmConformanceJournalEvent("abandon-llm-event", llm.EventProgress, nil)
	if _, err := first.PutEvent(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	want := mustLLMRecoveryEvents(t.Context(), t, first, 1)
	if err := image.Abandon(first); err != nil {
		t.Fatal(err)
	}
	second, releaseSecond, err := image.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = releaseSecond(context.Background()) })
	if err := releaseFirst(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := mustLLMRecoveryEvents(t.Context(), t, second, 1)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("event after abandoned handle = %#v, want %#v", got, want)
	}
	divergent := record
	divergent.Delivery = llm.CloneWorkerEventDelivery(record.Delivery)
	divergent.Delivery.Event.ID += "-changed"
	divergent.Digest = mustLLMJournalDigest(t, divergent.Delivery)
	if _, err := second.PutEvent(t.Context(), divergent); !errors.Is(err, llmws.ErrJournalConflict) {
		t.Fatalf("divergent replay after abandoned handle = %v, want ErrJournalConflict", err)
	}
}
