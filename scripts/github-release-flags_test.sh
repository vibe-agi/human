#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
subject="$root/scripts/github-release-flags.sh"

assert_flags() {
  local tag="$1"
  local expected="$2"
  local actual
  actual="$($subject "$tag")"
  if [[ "$actual" != "$expected" ]]; then
    printf 'unexpected flags for %s\nexpected:\n%s\nactual:\n%s\n' "$tag" "$expected" "$actual" >&2
    exit 1
  fi
}

assert_invalid() {
  local tag="$1"
  if "$subject" "$tag" >/dev/null 2>&1; then
    echo "expected invalid release tag to fail: $tag" >&2
    exit 1
  fi
}

assert_flags v0.1.0 '--latest'
assert_flags v12.34.56 '--latest'
assert_flags v0.1.0-rc.1 $'--prerelease\n--latest=false'
assert_flags v1.2.3-alpha.beta-1 $'--prerelease\n--latest=false'

assert_invalid 0.1.0
assert_invalid v1.2
assert_invalid v01.2.3
assert_invalid v1.2.3-
assert_invalid v1.2.3-rc..1
assert_invalid v1.2.3-01
assert_invalid v1.2.3+build.1

echo "GitHub release tag semantics: PASS"
