package observe_test

import (
	"errors"
	"testing"
	"time"

	"github.com/vibe-agi/human/observe"
)

func TestEmitIsNilSafeAndPanicSafe(t *testing.T) {
	// A nil observer is a no-op.
	observe.Emit(nil, observe.Event{Kind: observe.KindAlert})

	// A panicking observer must not escape Emit (instrumentation can never take
	// down a correctness path).
	panicking := observe.Func(func(observe.Event) { panic("boom") })
	observe.Emit(panicking, observe.Event{Kind: observe.KindAlert})
}

func TestEmitFillsZeroTime(t *testing.T) {
	var seen observe.Event
	observe.Emit(observe.Func(func(event observe.Event) { seen = event }),
		observe.Event{Kind: observe.KindWorkerConnected, Worker: "w1"})
	if seen.Time.IsZero() {
		t.Fatal("Emit did not fill a zero time")
	}
	if time.Since(seen.Time) > time.Minute {
		t.Fatalf("filled time is implausible: %v", seen.Time)
	}
}

func TestSlogObserverDoesNotPanic(t *testing.T) {
	observer := observe.NewSlog(nil)
	observer.Observe(observe.Event{
		Kind: observe.KindAdmissionRejected, Caller: "c1", Task: "t1",
		Detail: "worker_unavailable", Err: errors.New("no worker"),
	})
	observer.Observe(observe.Event{Kind: observe.KindCommandSent, Detail: "final"})
}
