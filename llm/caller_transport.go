package llm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/vibe-agi/human/framework"
)

const (
	// CallerTransportContractID names the authenticated HumanLLM caller-side
	// transport contract. It describes admission and exact response replay, not a
	// particular wire protocol such as HTTP, gRPC, or a message queue.
	CallerTransportContractID framework.ContractID = "human.llm.caller.transport"
	// CallerTransportContractMajor includes authenticated caller identity,
	// durable admission, cursor-based exact replay, and disconnect-independent
	// completion lifetime.
	CallerTransportContractMajor uint16 = 1
)

var (
	// ErrCallerTransportContractMismatch aliases the framework-wide construction
	// error so composition roots can classify incompatible adapters uniformly.
	ErrCallerTransportContractMismatch = framework.ErrContractMismatch
	ErrCallerTransportDescription      = errors.New("llm: invalid caller transport description")
)

// CallerTransportRequirements returns a fresh copy of the contract required by
// the HumanLLM core.
func CallerTransportRequirements() framework.Requirements {
	return framework.Requirements{
		ID:    CallerTransportContractID,
		Major: CallerTransportContractMajor,
	}
}

// CallerTransportDescription is immutable, non-secret adapter metadata.
// Provider identifies the implementation and Version identifies that
// implementation, not the model API projected by a Codec.
type CallerTransportDescription struct {
	Contract framework.Contract
	Provider string
	Version  string
}

func (description CallerTransportDescription) Validate() error {
	_, err := NegotiateCallerTransport(description)
	return err
}

// NegotiateCallerTransport validates a description and returns an independent
// frozen copy. Composition roots must cache this value once rather than calling
// Description again after Start.
func NegotiateCallerTransport(
	description CallerTransportDescription,
) (CallerTransportDescription, error) {
	contract, err := framework.Negotiate(description.Contract, CallerTransportRequirements())
	if err != nil {
		return CallerTransportDescription{}, err
	}
	if !validCallerTransportMetadata(description.Provider) ||
		!validCallerTransportMetadata(description.Version) {
		return CallerTransportDescription{}, ErrCallerTransportDescription
	}
	description.Contract = contract
	return description, nil
}

// CallerTransport authenticates callers and projects a wire protocol onto
// CallerEndpoint. Start borrows endpoint and uses ctx only to bound
// initialization. The returned runtime owns all listener/session/background
// lifetime. Shutdown must stop new admission, unblock active waits, and join its
// goroutines; it must never shut down endpoint or resources reachable through it.
// It must project only the safe fields of AdmissionError. A caller disconnect
// never cancels durable work: exact retry resumes response replay from a cursor.
type CallerTransport interface {
	Description() CallerTransportDescription
	Start(context.Context, CallerEndpoint) (CallerTransportRuntime, error)
}

// CallerTransportRuntime separates reusable adapter configuration from one
// running instance.
type CallerTransportRuntime interface {
	framework.Runtime
}

// ValidateCallerTransport rejects typed-nil adapters and returns their frozen
// negotiated description. It does not start network listeners.
func ValidateCallerTransport(
	transport CallerTransport,
) (CallerTransportDescription, error) {
	if isNilCallerTransport(transport) {
		return CallerTransportDescription{}, fmt.Errorf(
			"%w: transport is nil", ErrCallerTransportDescription,
		)
	}
	return NegotiateCallerTransport(transport.Description())
}

func validCallerTransportMetadata(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 128 || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func isNilCallerTransport(transport CallerTransport) bool {
	if transport == nil {
		return true
	}
	value := reflect.ValueOf(transport)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
