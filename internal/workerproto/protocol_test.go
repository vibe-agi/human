package workerproto

import (
	"encoding/json"
	"testing"
)

func TestEnvelopeValidation(t *testing.T) {
	t.Parallel()
	envelope, err := NewEnvelope(MessageHello, Hello{WorkerID: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	envelope.Seq = 1
	if err := envelope.Validate(); err != nil {
		t.Fatal(err)
	}
	envelope.Version = "2"
	if err := envelope.Validate(); err == nil {
		t.Fatal("unknown version accepted")
	}
}

func TestEventRejectedCarriesDurableIdentityAndAck(t *testing.T) {
	t.Parallel()
	envelope, err := NewEnvelope(MessageEventRejected, EventRejected{
		CallerID: "caller", IdempotencyKey: "request", EventID: "late-event",
		Message: "completion session is already closed",
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope.Seq = 2
	envelope.Ack = 7
	if err := envelope.Validate(); err != nil {
		t.Fatal(err)
	}
	var rejection EventRejected
	if err := json.Unmarshal(envelope.Payload, &rejection); err != nil {
		t.Fatal(err)
	}
	if rejection.Message == "" || rejection.CallerID != "caller" || rejection.IdempotencyKey != "request" ||
		rejection.EventID != "late-event" || envelope.Ack != 7 {
		t.Fatalf("rejection = %+v, envelope = %+v", rejection, envelope)
	}
}
