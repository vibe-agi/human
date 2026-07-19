package framework

import (
	"errors"
	"testing"
)

func TestFaultPreservesClassificationAndCause(t *testing.T) {
	cause := errors.New("database unavailable")
	err := NewFault(CodeUnavailable, RetryBackoff, "try later", cause)
	if !errors.Is(err, cause) {
		t.Fatalf("fault does not wrap cause: %v", err)
	}
	code, retry, ok := FaultInfo(err)
	if !ok || code != CodeUnavailable || retry != RetryBackoff {
		t.Fatalf("FaultInfo = %q, %q, %v", code, retry, ok)
	}
}

func TestFaultRejectsUnsafeRetry(t *testing.T) {
	if err := NewFault(CodeForbidden, RetryBackoff, "", nil); err == nil {
		t.Fatal("forbidden fault accepted backoff retry")
	}
	if err := NewFault(CodeIndeterminate, RetryExactInput, "", nil); err == nil {
		t.Fatal("indeterminate fault accepted blind exact retry")
	}
}
