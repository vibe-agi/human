#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary="$(mktemp -d)"
fixture="$temporary/source"
trap 'rm -rf "$temporary"' EXIT

# Exercise the current scripts in an otherwise-clean repository. This keeps the
# dirty-source assertion meaningful even when a developer runs the contract test
# from a checkout that already contains unrelated work in progress.
mkdir -p "$fixture/scripts" "$fixture/cmd/human" "$fixture/internal/buildinfo" \
  "$fixture/docs" "$fixture/examples"
cp "$root/scripts/build-release.sh" "$fixture/scripts/build-release.sh"
cp "$root/scripts/github-release-flags.sh" "$fixture/scripts/github-release-flags.sh"
chmod +x "$fixture/scripts/build-release.sh" "$fixture/scripts/github-release-flags.sh"
cat >"$fixture/go.mod" <<'EOF'
module github.com/vibe-agi/human

go 1.25
EOF
cat >"$fixture/internal/buildinfo/info.go" <<'EOF'
package buildinfo

var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)
EOF
cat >"$fixture/cmd/human/main.go" <<'EOF'
package main

import (
	"fmt"
	"os"

	"github.com/vibe-agi/human/internal/buildinfo"
)

func main() {
	if len(os.Args) != 2 || os.Args[1] != "version" {
		os.Exit(2)
	}
	fmt.Printf(
		"{\"version\":%q,\"commit\":%q,\"build_date\":%q}\n",
		buildinfo.Version, buildinfo.Commit, buildinfo.Date,
	)
}
EOF
printf 'Human release contract fixture\n' >"$fixture/README.md"
printf 'fixture documentation\n' >"$fixture/docs/contract.md"
printf 'fixture example\n' >"$fixture/examples/example.txt"
printf 'fixture license\n' >"$fixture/LICENSE"
git init -q "$fixture"
git -C "$fixture" add .
git -C "$fixture" -c user.name='Human Release Test' -c user.email='release-test@invalid' \
  commit -qm 'release contract fixture'
subject="$fixture/scripts/build-release.sh"

assert_rejected() {
  local version="$1"
  local commit="$2"
  local expected="$3"
  local output
  if output="$(VERSION="$version" COMMIT="$commit" DIST="$temporary/dist" "$subject" 2>&1)"; then
    echo "expected release build contract to reject VERSION=$version COMMIT=$commit" >&2
    exit 1
  fi
  if [[ "$output" != *"$expected"* ]]; then
    printf 'unexpected rejection for VERSION=%s COMMIT=%s\n%s\n' "$version" "$commit" "$output" >&2
    exit 1
  fi
}

assert_rejected 01.2.3 unknown "VERSION must be a release SemVer"
assert_rejected 0.1.0-01 unknown "VERSION must be a release SemVer"
assert_rejected 0.1.0 0000000000000000000000000000000000000000 "COMMIT does not match"

version="0.1.0-rc.1"
commit="$(git -C "$fixture" rev-parse HEAD)"
build_date="2026-07-18T00:00:00Z"
success_dist="$temporary/success-dist"
VERSION="$version" COMMIT="$commit" BUILD_DATE="$build_date" DIST="$success_dist" "$subject"

expected_archives=(
  "human_${version}_darwin_amd64.tar.gz"
  "human_${version}_darwin_arm64.tar.gz"
  "human_${version}_linux_amd64.tar.gz"
  "human_${version}_linux_arm64.tar.gz"
)
for archive in "${expected_archives[@]}"; do
  test -s "$success_dist/$archive"
done
test "$(wc -l <"$success_dist/checksums.txt" | tr -d ' ')" = "${#expected_archives[@]}"
(
  cd "$success_dist"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum --check checksums.txt >/dev/null
  else
    shasum -a 256 --check checksums.txt >/dev/null
  fi
)

host_os="$(go env GOOS)"
host_arch="$(go env GOARCH)"
case "$host_os" in
  darwin|linux) ;;
  *)
    echo "release contract host OS is not in the supported release matrix: $host_os" >&2
    exit 1
    ;;
esac
case "$host_arch" in
  amd64|arm64) ;;
  *)
    echo "release contract host architecture is not in the release matrix: $host_arch" >&2
    exit 1
    ;;
esac
host_stage="$temporary/host-stage"
mkdir -p "$host_stage"
host_archive="$success_dist/human_${version}_${host_os}_${host_arch}.tar.gz"
test -s "$host_archive"
tar -xzf "$host_archive" -C "$host_stage"
host_binary="$host_stage/human"
for entry in "$host_binary" "$host_stage/README.md" "$host_stage/LICENSE" \
  "$host_stage/docs/contract.md" "$host_stage/examples/example.txt"; do
  test -e "$entry"
done
expected_identity="{\"version\":\"$version\",\"commit\":\"$commit\",\"build_date\":\"$build_date\"}"
test "$("$host_binary" version)" = "$expected_identity"

probe="$(mktemp "$fixture/.human-release-dirty-probe.XXXXXX")"
assert_rejected 0.1.0 "$(git -C "$fixture" rev-parse HEAD)" "source checkout is dirty"
rm -f "$probe"

echo "Release build contract: PASS"
