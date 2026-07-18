#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 vMAJOR.MINOR.PATCH[-PRERELEASE]" >&2
  exit 2
fi

tag="$1"
if [[ ! "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*))?$ ]]; then
  echo "release tag must be SemVer with a leading v, such as v0.1.0 or v0.1.0-rc.1" >&2
  exit 2
fi

prerelease="${BASH_REMATCH[5]:-}"
if [[ -n "$prerelease" ]]; then
  IFS='.' read -r -a identifiers <<<"$prerelease"
  for identifier in "${identifiers[@]}"; do
    if [[ "$identifier" =~ ^[0-9]+$ && "$identifier" != "0" && "$identifier" == 0* ]]; then
      echo "numeric prerelease identifiers must not contain leading zeroes: $tag" >&2
      exit 2
    fi
  done
  # One argument per line keeps the workflow invocation shell-safe. A SemVer
  # prerelease is both visibly marked and explicitly excluded from GitHub's
  # latest pointer.
  printf '%s\n' --prerelease --latest=false
  exit 0
fi

# A stable SemVer tag is a normal GitHub release and intentionally advances the
# latest pointer.
printf '%s\n' --latest
