package completion

import (
	"fmt"
	"slices"
)

type State string

const (
	StateAdmitted        State = "admitted"
	StateLeased          State = "leased"
	StateAwaitingHuman   State = "awaiting_human"
	StateResponded       State = "responded"
	StateAwaitingCaller  State = "awaiting_caller"
	StateToolsDispatched State = "tools_dispatched"
	StateAwaitingResults State = "awaiting_results"
	StateReconciled      State = "reconciled"
	StateCompleted       State = "completed"
	StateCanceled        State = "canceled"
	StateRejected        State = "rejected"
	StateExpired         State = "expired"
	StateFailed          State = "failed"
)

var terminalStates = []State{
	StateCompleted,
	StateCanceled,
	StateRejected,
	StateExpired,
	StateFailed,
}

var transitions = map[State][]State{
	StateAdmitted:        {StateLeased},
	StateLeased:          {StateAwaitingHuman},
	StateAwaitingHuman:   {StateResponded},
	StateResponded:       {StateCompleted, StateAwaitingCaller, StateToolsDispatched},
	StateAwaitingCaller:  {StateReconciled},
	StateToolsDispatched: {StateAwaitingResults},
	StateAwaitingResults: {StateReconciled},
	StateReconciled:      {StateLeased},
}

func (state State) IsTerminal() bool {
	return slices.Contains(terminalStates, state)
}

func (state State) Valid() bool {
	if state.IsTerminal() {
		return true
	}
	_, ok := transitions[state]
	return ok
}

func CanTransition(from, to State) bool {
	if !from.Valid() || !to.Valid() || from.IsTerminal() {
		return false
	}
	if to != StateCompleted && to.IsTerminal() {
		return true
	}
	return slices.Contains(transitions[from], to)
}

func ValidateTransition(from, to State) error {
	if CanTransition(from, to) {
		return nil
	}
	return fmt.Errorf("invalid completion task transition %q -> %q", from, to)
}
