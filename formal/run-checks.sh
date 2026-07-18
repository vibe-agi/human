#!/usr/bin/env bash
# Reproduce every positive model check and prove the important oracles are
# live by requiring intentionally broken mutants to fail.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FORMAL="$ROOT/formal"
JAR="${TLA_TOOLS_JAR:-$FORMAL/tla2tools.jar}"
WORKERS="${TLC_WORKERS:-1}"
PHASE="${TLA_CHECK_PHASE:-all}"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/human-tla.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

case "$PHASE" in
  all|positive|mutants) ;;
  *) echo "TLA_CHECK_PHASE must be all, positive, or mutants" >&2; exit 2 ;;
esac

# Always pass an existing tool through the pinned digest verifier too.  The
# installer returns the selected path; canonicalize it because run_tlc changes
# into each module directory before launching Java.
JAR="$($FORMAL/install-tools.sh "$JAR")"
JAR_DIR="$(cd -P "$(dirname "$JAR")" && pwd -P)"
JAR="$JAR_DIR/$(basename "$JAR")"

TLC_VERSION="$(java -cp "$JAR" tlc2.TLC -help 2>&1 | grep -m 1 'Version 2.19' || true)"
case "$TLC_VERSION" in
  *"Version 2.19"*) ;;
  *) echo "expected TLC 2.19, got: $TLC_VERSION" >&2; exit 1 ;;
esac

PASS_COUNT=0

distinct_states() {
  awk '/distinct states found/ {gsub(/,/, "", $4); value=$4} END {print value+0}' "$1"
}

run_tlc() {
  local module="$1" config="$2" fingerprint="$3" output="$4"
  local metadir="$TMP/states-$(basename "$output" .log)"
  local module_dir config_dir
  module_dir="$(cd -P "$(dirname "$module")" && pwd -P)"
  config_dir="$(cd -P "$(dirname "$config")" && pwd -P)"
  if [ "$module_dir" != "$config_dir" ]; then
    echo "module and config must share a directory: $module / $config" >&2
    return 2
  fi
  if (cd "$module_dir" && java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
      -workers "$WORKERS" -fp "$fingerprint" -metadir "$metadir" \
      "$(basename "$module")" -config "$(basename "$config")") \
      >"$output" 2>&1; then
    return 0
  else
    return $?
  fi
}

expect_success() {
  local name="$1" module="$2" config="$3" minimum="$4" fingerprint="${5:-0}"
  local output="$TMP/$name-fp$fingerprint.log"
  if ! run_tlc "$module" "$config" "$fingerprint" "$output"; then
    cat "$output" >&2
    echo "FAIL $name: TLC unexpectedly rejected the model" >&2
    exit 1
  fi
  grep -q "Model checking completed. No error has been found." "$output" || {
    cat "$output" >&2
    echo "FAIL $name: success marker missing" >&2
    exit 1
  }
  local states
  states="$(distinct_states "$output")"
  if [ "$states" -lt "$minimum" ]; then
    cat "$output" >&2
    echo "FAIL $name: only $states distinct states (minimum $minimum)" >&2
    exit 1
  fi
  PASS_COUNT=$((PASS_COUNT + 1))
  printf 'PASS %-36s %8s states (fp %s)\n' "$name" "$states" "$fingerprint"
}

expect_failure() {
  local name="$1" mutant="$2" config="$3" oracle="$4" minimum="${5:-1}"
  local mutant_dir="$TMP/$mutant"
  local module local_config output status states
  module="$(python3 "$FORMAL/make-mutant.py" "$mutant" "$mutant_dir")"
  local_config="$mutant_dir/Mutant.cfg"
  cp "$config" "$local_config"
  output="$TMP/$name.log"
  set +e
  run_tlc "$module" "$local_config" 0 "$output"
  status=$?
  set -e
  if [ "$status" -eq 0 ]; then
    cat "$output" >&2
    echo "FAIL $name: mutant escaped its oracle" >&2
    exit 1
  fi
  grep -q "$oracle" "$output" || {
    cat "$output" >&2
    echo "FAIL $name: expected oracle '$oracle' was not reported" >&2
    exit 1
  }
  states="$(distinct_states "$output")"
  if [ "$states" -lt "$minimum" ]; then
    cat "$output" >&2
    echo "FAIL $name: failure was too early/vacuous ($states states)" >&2
    exit 1
  fi
  PASS_COUNT=$((PASS_COUNT + 1))
  printf 'PASS %-36s caught %-34s (%s states)\n' "$name" "$oracle" "$states"
}

expect_model_failure() {
  local name="$1" module="$2" config="$3" oracle="$4" minimum="${5:-1}"
  local output="$TMP/$name.log" status states
  set +e
  run_tlc "$module" "$config" 0 "$output"
  status=$?
  set -e
  if [ "$status" -eq 0 ]; then
    cat "$output" >&2
    echo "FAIL $name: the expected counterexample was not reachable" >&2
    exit 1
  fi
  grep -q "$oracle" "$output" || {
    cat "$output" >&2
    echo "FAIL $name: expected property '$oracle' was not reported" >&2
    exit 1
  }
  states="$(distinct_states "$output")"
  if [ "$states" -lt "$minimum" ]; then
    cat "$output" >&2
    echo "FAIL $name: counterexample was too early/vacuous ($states states)" >&2
    exit 1
  fi
  PASS_COUNT=$((PASS_COUNT + 1))
  printf 'PASS %-36s disproved %-31s (%s states)\n' "$name" "$oracle" "$states"
}

write_late_event_config() {
  python3 - "$1" <<'PY'
from pathlib import Path
import sys

Path(sys.argv[1]).write_text(r'''CONSTANTS
  ModeledSurfaces = {"llm"}
  Scopes = {scope}
  ItemIDs = {item}
  EventIDs = {event}
  Digests = {digest}
  Workers = {worker_a, worker_b}
  Capacity = 1
  MaxFence = 2
  MaxVersion = 1
  MaxFaults = 0
SPECIFICATION LiveSpec
PROPERTIES
  LateEventsDoNotPoison
''', encoding='utf-8')
PY
}

if [ "$PHASE" != "mutants" ]; then
echo "TLC 2.19 positive matrix"
expect_success runtime-safety "$FORMAL/HumanRuntime.tla" "$FORMAL/HumanRuntimeSafety.cfg" 100
expect_success runtime-faults "$FORMAL/HumanRuntime.tla" "$FORMAL/HumanRuntimeFaults.cfg" 500
expect_success runtime-liveness "$FORMAL/HumanRuntime.tla" "$FORMAL/HumanRuntimeLiveness.cfg" 500
expect_success runtime-triple-outage "$FORMAL/HumanRuntime.tla" "$FORMAL/HumanRuntimeTripleOutage.cfg" 2000
expect_success runtime-retry-storm "$FORMAL/HumanRuntime.tla" "$FORMAL/HumanRuntimeRetryStorm.cfg" 3000
expect_success runtime-digest-conflict "$FORMAL/HumanRuntime.tla" "$FORMAL/HumanRuntimeDigestConflict.cfg" 1000
expect_success runtime-fencing "$FORMAL/HumanRuntime.tla" "$FORMAL/HumanRuntimeFencing.cfg" 5000
expect_success runtime-workspace-race "$FORMAL/HumanRuntime.tla" "$FORMAL/HumanRuntimeWorkspaceRace.cfg" 10
expect_success runtime-both-surfaces "$FORMAL/HumanRuntime.tla" "$FORMAL/HumanRuntimeBothSurfaces.cfg" 20000
expect_success worker-sequence "$FORMAL/HumanWorkerSequence.tla" "$FORMAL/HumanWorkerSequence.cfg" 30

expect_success llm-safety "$FORMAL/HumanLLM.tla" "$FORMAL/HumanLLMSafety.cfg" 20000
expect_success llm-liveness "$FORMAL/HumanLLM.tla" "$FORMAL/HumanLLMLiveness.cfg" 3000
expect_success llm-no-caller "$FORMAL/HumanLLM.tla" "$FORMAL/HumanLLMNoCaller.cfg" 50
expect_success llm-transition-oracles "$FORMAL/HumanLLM.tla" "$FORMAL/HumanLLMTransitionOracles.cfg" 1000
expect_success llm-continuous-progress "$FORMAL/HumanLLM.tla" "$FORMAL/HumanLLMProgress.cfg" 5

expect_success agent-safety "$FORMAL/HumanAgent.tla" "$FORMAL/HumanAgentSafety.cfg" 250000
expect_success agent-liveness "$FORMAL/HumanAgent.tla" "$FORMAL/HumanAgentLiveness.cfg" 500
expect_success agent-no-human "$FORMAL/HumanAgent.tla" "$FORMAL/HumanAgentNoHuman.cfg" 100
expect_success agent-followup "$FORMAL/HumanAgent.tla" "$FORMAL/HumanAgentFollowup.cfg" 50000
expect_success agent-two-input-rounds "$FORMAL/HumanAgent.tla" "$FORMAL/HumanAgentConversation.cfg" 1900
expect_success agent-shared-workspace "$FORMAL/HumanAgent.tla" "$FORMAL/HumanAgentSharedWorkspace.cfg" 500
expect_success agent-parallel-context "$FORMAL/HumanAgent.tla" "$FORMAL/HumanAgentParallelContext.cfg" 10
expect_success agent-identity-oracles "$FORMAL/HumanAgent.tla" "$FORMAL/HumanAgentIdentityOracles.cfg" 50
expect_success agent-apply-oracles "$FORMAL/HumanAgent.tla" "$FORMAL/HumanAgentApplyOracles.cfg" 100000
expect_success agent-baseline-oracles "$FORMAL/HumanAgent.tla" "$FORMAL/HumanAgentBaselineOracles.cfg" 100
expect_success system-composition "$FORMAL/HumanSystem.tla" "$FORMAL/HumanSystem.cfg" 200
expect_success system-workspace-race "$FORMAL/HumanSystem.tla" "$FORMAL/HumanSystemRace.cfg" 15

# Independent fingerprints on the three public semantic cores reduce the
# already tiny chance that a fingerprint collision hides a reachable state.
expect_success llm-safety-alt-fp "$FORMAL/HumanLLM.tla" "$FORMAL/HumanLLMSafety.cfg" 20000 1
expect_success agent-safety-alt-fp "$FORMAL/HumanAgent.tla" "$FORMAL/HumanAgentSafety.cfg" 250000 1
expect_success system-composition-alt-fp "$FORMAL/HumanSystem.tla" "$FORMAL/HumanSystem.cfg" 200 1

echo
echo "Expected counterexamples"
expect_model_failure no-human-cannot-guarantee-progress \
  "$FORMAL/HumanAgent.tla" "$FORMAL/HumanAgentNoHumanLiveness.cfg" \
  "Temporal properties were violated" 10
expect_model_failure triple-outage-is-reachable \
  "$FORMAL/HumanRuntime.tla" "$FORMAL/HumanRuntimeTripleOutageWitness.cfg" \
  "SomePartyUp" 30
expect_model_failure five-fault-storm-is-reachable \
  "$FORMAL/HumanRuntime.tla" "$FORMAL/HumanRuntimeRetryStormWitness.cfg" \
  "FaultBudgetRemaining" 100
expect_model_failure shared-workspace-has-independent-contexts \
  "$FORMAL/HumanAgent.tla" "$FORMAL/HumanAgentSharedWorkspaceWitness.cfg" \
  "AtMostOneArtifactContextPerWorkspace" 500
expect_model_failure parallel-tasks-in-context-are-reachable \
  "$FORMAL/HumanAgent.tla" "$FORMAL/HumanAgentParallelContextWitness.cfg" \
  "AtMostOneActiveTaskPerContext" 5
fi

if [ "$PHASE" != "positive" ]; then
echo
echo "Mutation/oracle matrix"
expect_failure old-frame-absorbs-future-ack sequence_old_frame_absorbs_future_ack \
  "$FORMAL/HumanWorkerSequence.cfg" OutboundACKsBoundAtEnqueue 3
expect_failure rejection-delete-not-atomic sequence_rejection_not_atomic \
  "$FORMAL/HumanWorkerSequence.cfg" NoPrematureCumulativeDelete 8
expect_failure stale-snapshot-regresses-ack sequence_stale_snapshot_regresses_ack \
  "$FORMAL/HumanWorkerSequence.cfg" OutboundACKsMonotone 5
expect_failure ack-before-durable runtime_ack_before_durable \
  "$FORMAL/HumanRuntimeSafety.cfg" AckAfterDurable 10
expect_failure crash-drops-outbox runtime_crash_drops_outbox \
  "$FORMAL/HumanRuntimeFaults.cfg" OutboxAccounting 10
expect_failure durable-history-drops-retired runtime_durable_history_drops_retired \
  "$FORMAL/HumanRuntimeSafety.cfg" DurableHistoriesAppendOnly 5
expect_failure stale-fence-commit runtime_stale_fence_commits \
  "$FORMAL/HumanRuntimeFencing.cfg" EffectAuthorizedAtCommit 100

LATE_CONFIG="$TMP/LateEvent.cfg"
write_late_event_config "$LATE_CONFIG"
expect_failure late-event-poison runtime_late_event_poison \
  "$LATE_CONFIG" "Deadlock reached" 100
expect_failure workspace-lost-update runtime_workspace_lost_update \
  "$FORMAL/HumanRuntimeWorkspaceRace.cfg" WorkspaceCASOK 5

expect_failure digest-conflict-lies llm_digest_conflict_lies \
  "$FORMAL/HumanLLMSafety.cfg" IdempotencyObservationsAreExact 10
expect_failure stream-before-http-decision llm_stream_before_decision \
  "$FORMAL/HumanLLMSafety.cfg" HTTPDecisionPrecedesVisibility 10
expect_failure admitted-result-map-mutates llm_request_results_mutate \
  "$FORMAL/HumanLLMSafety.cfg" RequestIdentityImmutable 10
expect_failure terminal-llm-task-reopens llm_terminal_reopens \
  "$FORMAL/HumanLLMSafety.cfg" TerminalTaskCannotBeReopened 100
expect_failure failed-result-advances-baseline llm_failure_advances_baseline \
  "$FORMAL/HumanLLMSafety.cfg" BaselineAdvancesOnlyOnSuccess 100
expect_failure tool-calls-terminate-task llm_tool_calls_terminate_task \
  "$FORMAL/HumanLLMSafety.cfg" ResponseTerminalTransitionMatchesTask 100
expect_failure error-marked-completed llm_error_marked_completed \
  "$FORMAL/HumanLLMSafety.cfg" ResponseTerminalTransitionMatchesTask 100
expect_failure text-final-marked-failed llm_text_final_marked_failed \
  "$FORMAL/HumanLLMTransitionOracles.cfg" ResponseTerminalTransitionMatchesTask 10
expect_failure clarification-terminates-task llm_clarification_terminates_task \
  "$FORMAL/HumanLLMTransitionOracles.cfg" ResponseTerminalTransitionMatchesTask 15
expect_failure reconcile-misclassifies-result llm_reconcile_misclassifies_result \
  "$FORMAL/HumanLLMTransitionOracles.cfg" ReconcileResultIsExact 80
expect_failure reconcile-swallows-pending llm_reconcile_swallows_pending \
  "$FORMAL/HumanLLMSafety.cfg" ReconcileResultIsExact 100
expect_failure terminal-overwrites-progress llm_terminal_overwrites_progress \
  "$FORMAL/HumanLLMProgress.cfg" TraceMatchesCursor 5

expect_failure terminal-agent-task-reopens agent_terminal_reopens \
  "$FORMAL/HumanAgentSafety.cfg" TerminalTasksImmutable 100
expect_failure agent-submission-nonatomic agent_submission_nonatomic \
  "$FORMAL/HumanAgentSafety.cfg" PublicationAtomic 100
expect_failure dirty-workspace-apply-succeeds agent_dirty_apply_succeeds \
  "$FORMAL/HumanAgentSafety.cfg" ApplySuccessRequiresVerifiedCAS 1000
expect_failure wrong-base-apply-succeeds agent_wrong_base_succeeds \
  "$FORMAL/HumanAgentApplyOracles.cfg" ApplySuccessRequiresVerifiedCAS 40000
expect_failure cancel-leaves-frozen-artifact agent_cancel_leaves_frozen \
  "$FORMAL/HumanAgentSafety.cfg" TerminalArtifactsSettled 100
expect_failure wrong-message-role agent_wrong_message_role \
  "$FORMAL/HumanAgentSafety.cfg" MessageTurnsAlternate 20
expect_failure task-identity-changes agent_task_identity_changes \
  "$FORMAL/HumanAgentIdentityOracles.cfg" TaskIdentityImmutable 4
expect_failure conflict-advances-baseline agent_conflict_advances_baseline \
  "$FORMAL/HumanAgentBaselineOracles.cfg" BaselineChangesOnlyOnApplySuccess 100
expect_failure caller-reply-does-not-resume agent_reply_does_not_resume \
  "$FORMAL/HumanAgentConversation.cfg" "Deadlock reached" 5

expect_failure surface-key-collision system_surface_collision \
  "$FORMAL/HumanSystem.cfg" \
  "Invariant SurfaceIsolation is violated by the initial state" 0
expect_failure premature-baseline system_premature_baseline \
  "$FORMAL/HumanSystem.cfg" BaselineConfirmed 20
expect_failure frozen-payload-mutates-base system_frozen_payload_mutates_base \
  "$FORMAL/HumanSystem.cfg" FrozenIntentsChangeOnlyOnReset 5
expect_failure frozen-intent-dropped-early system_frozen_intent_dropped_early \
  "$FORMAL/HumanSystem.cfg" FrozenIntentsChangeOnlyOnReset 5
expect_failure baseline-selects-old-success system_baseline_selects_old_success \
  "$FORMAL/HumanSystem.cfg" BaselineChangesOnlyOnExactReceipt 100
expect_failure late-receipt-drops-newer system_late_receipt_drops_newer \
  "$FORMAL/HumanSystem.cfg" PendingNewerDraftPreserved 50
expect_failure agent-artifact-nonatomic system_artifact_nonatomic \
  "$FORMAL/HumanSystem.cfg" ArtifactAtomic 20
expect_failure cross-surface-race-loses-cas system_race_agent_wrong_base_succeeds \
  "$FORMAL/HumanSystemRace.cfg" ValidWriteChain 10
fi

printf '\nAll %s checks passed.\n' "$PASS_COUNT"
