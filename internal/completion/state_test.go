package completion

import "testing"

func TestToolLoopTransitions(t *testing.T) {
	t.Parallel()
	path := []State{
		StateAdmitted,
		StateLeased,
		StateAwaitingHuman,
		StateResponded,
		StateToolsDispatched,
		StateAwaitingResults,
		StateReconciled,
		StateLeased,
	}
	for i := 1; i < len(path); i++ {
		if err := ValidateTransition(path[i-1], path[i]); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
}

func TestClarificationLoopsToLease(t *testing.T) {
	t.Parallel()
	path := []State{StateResponded, StateAwaitingCaller, StateReconciled, StateLeased}
	for i := 1; i < len(path); i++ {
		if !CanTransition(path[i-1], path[i]) {
			t.Fatalf("expected %s -> %s", path[i-1], path[i])
		}
	}
}

func TestTerminalTransitions(t *testing.T) {
	t.Parallel()
	for _, terminal := range terminalStates {
		if terminal != StateCompleted && !CanTransition(StateAwaitingHuman, terminal) {
			t.Fatalf("non-completed terminal %s should be reachable from a live state", terminal)
		}
		if CanTransition(terminal, StateLeased) {
			t.Fatalf("terminal %s must not transition", terminal)
		}
	}
	if CanTransition(StateAdmitted, StateCompleted) {
		t.Fatal("admitted task completed without a response")
	}
}

func TestUnknownStateRejected(t *testing.T) {
	t.Parallel()
	if CanTransition(State("unknown"), StateLeased) {
		t.Fatal("unknown state accepted")
	}
}
