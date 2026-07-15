#!/usr/bin/env bash
# Reproducible TLC runs for HumanAgent.tla. See README.md for expected results.
# Exit 0 iff every experiment matched its expectation (incl. the intentional
# liveness failure and the two mutant invariant violations).
#
# NOTE on -deadlock: TLC's -deadlock flag *disables* deadlock checking. We do
# NOT pass it — deadlock checking is ON by default; legal quiescent terminals
# are modeled with an explicit Terminating self-loop so they aren't flagged.
set -uo pipefail   # NOT -e: we inspect TLC exit codes ourselves.
cd "$(dirname "$0")"

JAR="${TLA2TOOLS:-./tla2tools.jar}"
[ -f "$JAR" ] || { echo "tla2tools.jar not found; set TLA2TOOLS=/path/to/tla2tools.jar"; exit 1; }
ABSJAR="$(cd "$(dirname "$JAR")" && pwd)/$(basename "$JAR")"

VER=$(java -cp "$ABSJAR" tlc2.TLC 2>&1 | grep -oE 'Version 2\.[0-9]+' | head -1)
echo "TLC ${VER:-unknown}"
[ "$VER" = "Version 2.19" ] || { echo "FAIL  requires TLC Version 2.19"; exit 1; }

MUT="$(mktemp -d)"
trap 'rm -rf "$MUT"' EXIT
FAIL=0
WORKERS=1  # Pin scheduling so failing-mutant state counts are reproducible.
tlc() { java -XX:+UseParallelGC -cp "$ABSJAR" tlc2.TLC -workers "$WORKERS" "$@" 2>&1; }
distinct_states() {
  grep -oE '[0-9,]+ distinct states found' <<<"$1" | tail -1 | grep -oE '^[0-9,]+' | tr -d ','
}

expect_success() { # name cfg tla expected-distinct-states
  local out rc meta="$MUT/meta-${2%.cfg}"; mkdir -p "$meta"
  out=$(tlc -metadir "$meta" -config "$2" "$3"); rc=$?
  local states; states=$(distinct_states "$out")
  if [ "$rc" -eq 0 ] && grep -q "No error has been found" <<<"$out" && [ "$states" = "$4" ]; then
    echo "PASS  $1  ($states distinct states)"
  else
    echo "FAIL  $1  (rc=$rc, states=${states:-missing}; expected rc=0, states=$4, No error)"
    FAIL=1
  fi
}
expect_failure() { # name dir cfg tla expected-rc expected-substring expected-distinct-states
  local out rc meta="$MUT/meta-${3%.cfg}"; mkdir -p "$meta"
  out=$( cd "$2" && java -XX:+UseParallelGC -cp "$ABSJAR" tlc2.TLC -workers "$WORKERS" -metadir "$meta" -config "$3" "$4" 2>&1 ); rc=$?
  local states; states=$(distinct_states "$out")
  if [ "$rc" -eq "$5" ] && grep -q "$6" <<<"$out" && [ "$states" = "$7" ]; then
    echo "PASS  $1  (rc=$rc, $states distinct states; expected: $6)"
  else
    echo "FAIL  $1  (rc=$rc, states=${states:-missing}; expected rc=$5, states=$7, text: $6)"
    FAIL=1
  fi
}

expect_success "main  (3/1/1)" HumanAgent.cfg HumanAgent.tla 3486
expect_success "large (4/2/2)" HumanAgentLarge.cfg HumanAgent.tla 54478

# TLC reports a generic temporal violation, not its property name. Keep this
# experiment's config single-property so that the failing property is
# mechanically unambiguous.
NO_HUMAN_PROPERTIES=$(awk '
  /^PROPERTIES$/ { in_properties=1; next }
  in_properties && NF && $1 !~ /^\\\*/ { print $1 }
' HumanAgentNoHumanFair.cfg | paste -sd, -)
if [ "$NO_HUMAN_PROPERTIES" != "EventuallyTerminal" ]; then
  echo "FAIL  no-human config must contain only PROPERTIES EventuallyTerminal"
  FAIL=1
else
  expect_failure "no-human-fairness (EventuallyTerminal only)" . HumanAgentNoHumanFair.cfg HumanAgent.tla 13 "Temporal properties were violated" 3486
fi

# Mutants: inject a bug, assert an invariant catches it. Generated into $MUT
# (file name must equal module name); python asserts the replacement happened.
python3 - "$MUT" <<'PY'
import sys, pathlib
mut = pathlib.Path(sys.argv[1]); src = pathlib.Path("HumanAgent.tla").read_text()
c = src.replace('MODULE HumanAgent','MODULE MutantC').replace(
  "/\\ \\E v \\in Live : /\\ v < latest\n                     /\\ rewindTo' = v",
  "/\\ \\E v \\in 0..MaxTurns : /\\ v # latest\n                            /\\ rewindTo' = v")
d = src.replace('MODULE HumanAgent','MODULE MutantD').replace(
  '''  /\\ superseded' = superseded \\union { v \\in Range(turns) : v > rewindTo }
  /\\ taskState' = "input_required"
  /\\ UNCHANGED <<turns, rewindTo, rewinds, bobNext,''',
  '''  /\\ superseded' = superseded \\union { v \\in Range(turns) : v > rewindTo }
  /\\ turns' = SelectSeq(turns, LAMBDA t: t <= rewindTo)
  /\\ taskState' = "input_required"
  /\\ UNCHANGED <<rewindTo, rewinds, bobNext,''')
assert c.count("v \\in 0..MaxTurns : /\\ v # latest") == 1, "mutant C replacement did not apply exactly once"
assert d.count("turns' = SelectSeq(turns, LAMBDA t: t <= rewindTo)") == 1, "mutant D replacement did not apply exactly once"
(mut/'MutantC.tla').write_text(c); (mut/'MutantD.tla').write_text(d)
cfg = pathlib.Path("HumanAgent.cfg").read_text()
(mut/'MutantC.cfg').write_text(cfg); (mut/'MutantD.cfg').write_text(cfg)
print("mutants generated + replacement asserted")
PY

expect_failure "mutant C (rewind target unvalidated)" "$MUT" MutantC.cfg MutantC.tla 12 "Invariant RewindPendingOK is violated" 9
expect_failure "mutant D (audit deleted on rewind)" "$MUT" MutantD.cfg MutantD.tla 12 "Invariant SupersededOK is violated" 30

echo
[ $FAIL -eq 0 ] && echo "ALL EXPECTATIONS MET" || echo "SOME EXPECTATIONS FAILED"
exit $FAIL
