package gateway

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

type deadlineBlockingResponseWriter struct {
	header http.Header

	mu               sync.Mutex
	status           int
	deadline         time.Time
	deadlinesSet     int
	deadlinesCleared int
	writeStarted     chan struct{}
	writeOnce        sync.Once
}

func newDeadlineBlockingResponseWriter() *deadlineBlockingResponseWriter {
	return &deadlineBlockingResponseWriter{
		header: make(http.Header), writeStarted: make(chan struct{}),
	}
}

func (writer *deadlineBlockingResponseWriter) Header() http.Header { return writer.header }

func (writer *deadlineBlockingResponseWriter) WriteHeader(status int) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.status == 0 {
		writer.status = status
	}
}

func (writer *deadlineBlockingResponseWriter) Write([]byte) (int, error) {
	writer.writeOnce.Do(func() { close(writer.writeStarted) })
	writer.mu.Lock()
	deadline := writer.deadline
	writer.mu.Unlock()
	if deadline.IsZero() {
		select {}
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	<-timer.C
	return 0, os.ErrDeadlineExceeded
}

func (*deadlineBlockingResponseWriter) Flush() {}

func (writer *deadlineBlockingResponseWriter) SetWriteDeadline(deadline time.Time) error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.deadline = deadline
	if deadline.IsZero() {
		writer.deadlinesCleared++
	} else {
		writer.deadlinesSet++
	}
	return nil
}

func (writer *deadlineBlockingResponseWriter) snapshot() (status, set, cleared int) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.status, writer.deadlinesSet, writer.deadlinesCleared
}

type deadlineTrackingResponseWriter struct {
	*httptest.ResponseRecorder
	mu      sync.Mutex
	set     int
	cleared int
}

func (writer *deadlineTrackingResponseWriter) SetWriteDeadline(deadline time.Time) error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if deadline.IsZero() {
		writer.cleared++
	} else {
		writer.set++
	}
	return nil
}

func TestStreamResponseWriterBoundsOnlyOneWriteAndClearsDeadline(t *testing.T) {
	t.Parallel()
	recorder := &deadlineTrackingResponseWriter{ResponseRecorder: httptest.NewRecorder()}
	writer := newStreamResponseWriter(recorder, time.Second)
	if err := writer.writeAndFlush([]byte("data: one\n\n"), []byte("data: two\n\n")); err != nil {
		t.Fatal(err)
	}
	if got := recorder.Body.String(); got != "data: one\n\ndata: two\n\n" {
		t.Fatalf("stream body = %q", got)
	}
	recorder.mu.Lock()
	set, cleared := recorder.set, recorder.cleared
	recorder.mu.Unlock()
	if set != 1 || cleared != 1 {
		t.Fatalf("write deadlines = set %d cleared %d, want 1/1", set, cleared)
	}
}

func TestStreamResponseWriterKeepsBehaviorWhenDeadlineUnsupported(t *testing.T) {
	t.Parallel()
	recorder := httptest.NewRecorder()
	writer := newStreamResponseWriter(recorder, time.Nanosecond)
	if err := writer.writeAndFlush([]byte("data: supported fallback\n\n")); err != nil {
		t.Fatal(err)
	}
	if got := recorder.Body.String(); got != "data: supported fallback\n\n" {
		t.Fatalf("stream body = %q", got)
	}
}

func TestCallerThatStopsReadingCannotHoldFreshOrReplayHandler(t *testing.T) {
	fixture := newGatewayFixtureWithConfig(t, true, Config{
		HeartbeatInterval:  time.Hour,
		StreamWriteTimeout: 20 * time.Millisecond,
		MaxPending:         time.Minute,
	})
	body := chatBody("caller stops reading", false)

	run := func(label string) {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer hae_test")
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(headerIdempotencyKey, "half-open-caller")
		response := newDeadlineBlockingResponseWriter()
		returned := make(chan any, 1)
		go func() {
			defer func() { returned <- recover() }()
			fixture.gateway.ServeHTTP(response, request)
		}()
		select {
		case panicValue := <-returned:
			if panicValue != nil && !errors.Is(asError(panicValue), http.ErrAbortHandler) {
				t.Fatalf("%s handler panic = %v", label, panicValue)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s handler remained blocked after its per-write deadline", label)
		}
		status, set, cleared := response.snapshot()
		if status != http.StatusOK || set == 0 || cleared == 0 {
			t.Fatalf("%s response = status %d, deadlines set/cleared %d/%d", label, status, set, cleared)
		}
	}

	run("fresh")
	run("replay")
}

func asError(value any) error {
	if err, ok := value.(error); ok {
		return err
	}
	return nil
}
