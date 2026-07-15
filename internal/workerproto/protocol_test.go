package workerproto

import "testing"

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
