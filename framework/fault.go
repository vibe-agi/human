package framework

import (
	"errors"
	"fmt"
)

// FaultCode is a transport-neutral classification. Adapters map it to their
// wire protocol without parsing error text.
type FaultCode string

const (
	CodeInvalid         FaultCode = "invalid"
	CodeUnauthenticated FaultCode = "unauthenticated"
	CodeForbidden       FaultCode = "forbidden"
	CodeNotFound        FaultCode = "not_found"
	CodeConflict        FaultCode = "conflict"
	CodeStale           FaultCode = "stale"
	CodeUnavailable     FaultCode = "unavailable"
	CodeIndeterminate   FaultCode = "indeterminate"
	CodeCorrupt         FaultCode = "corrupt"
	CodeClosed          FaultCode = "closed"
)

// RetryMode tells an adapter which class of retry is safe. It never grants
// authorization and does not override a domain-specific idempotency contract.
type RetryMode string

const (
	RetryNever      RetryMode = "never"
	RetryExactInput RetryMode = "exact_input"
	RetryBackoff    RetryMode = "backoff"
	RetryReconcile  RetryMode = "reconcile"
)

// Fault carries a stable code and retry disposition while retaining the
// original error for errors.Is/errors.As. Message is safe for the embedding
// application; adapters must not expose Cause text unless their policy allows
// it.
type Fault struct {
	Code    FaultCode
	Retry   RetryMode
	Message string
	Cause   error
}

func (fault *Fault) Error() string {
	if fault == nil {
		return "<nil>"
	}
	if fault.Message != "" {
		return fault.Message
	}
	if fault.Cause != nil {
		return fault.Cause.Error()
	}
	return string(fault.Code)
}

func (fault *Fault) Unwrap() error {
	if fault == nil {
		return nil
	}
	return fault.Cause
}

// NewFault validates and constructs a classified fault.
func NewFault(code FaultCode, retry RetryMode, message string, cause error) error {
	if !code.valid() {
		return fmt.Errorf("framework: invalid fault code %q", code)
	}
	if !retry.valid() {
		return fmt.Errorf("framework: invalid retry mode %q", retry)
	}
	if !retryAllowed(code, retry) {
		return fmt.Errorf("framework: retry mode %q is unsafe for fault code %q", retry, code)
	}
	if message == "" && cause == nil {
		message = string(code)
	}
	return &Fault{Code: code, Retry: retry, Message: message, Cause: cause}
}

// FaultInfo returns a stable classification from err.
func FaultInfo(err error) (FaultCode, RetryMode, bool) {
	var fault *Fault
	if !errors.As(err, &fault) || fault == nil {
		return "", "", false
	}
	return fault.Code, fault.Retry, true
}

func (code FaultCode) valid() bool {
	switch code {
	case CodeInvalid, CodeUnauthenticated, CodeForbidden, CodeNotFound,
		CodeConflict, CodeStale, CodeUnavailable, CodeIndeterminate,
		CodeCorrupt, CodeClosed:
		return true
	default:
		return false
	}
}

func (mode RetryMode) valid() bool {
	switch mode {
	case RetryNever, RetryExactInput, RetryBackoff, RetryReconcile:
		return true
	default:
		return false
	}
}

func retryAllowed(code FaultCode, mode RetryMode) bool {
	switch code {
	case CodeUnavailable:
		return mode == RetryBackoff || mode == RetryExactInput
	case CodeIndeterminate:
		return mode == RetryReconcile
	case CodeConflict, CodeStale:
		return mode == RetryNever || mode == RetryExactInput
	default:
		return mode == RetryNever
	}
}
