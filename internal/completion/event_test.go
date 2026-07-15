package completion

import "testing"

func TestEventEndsResponse(t *testing.T) {
	t.Parallel()
	if (Event{Type: EventProgress}).EndsResponse() {
		t.Fatal("progress ended response")
	}
	for _, kind := range []EventType{EventFinal, EventClarification, EventToolCalls, EventRejected, EventExpired, EventFailed, EventUnavailable} {
		if !(Event{Type: kind}).EndsResponse() {
			t.Fatalf("%s did not end response", kind)
		}
	}
}
