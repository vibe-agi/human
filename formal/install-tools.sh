#!/usr/bin/env bash
# Install the exact stable TLA+ command-line tools used by this repository.
# The release asset and digest are pinned so a replaced download fails closed.
set -euo pipefail

VERSION="1.7.4"
SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
URL="https://github.com/tlaplus/tlaplus/releases/download/v${VERSION}/tla2tools.jar"
DESTINATION="${1:-$(cd "$(dirname "$0")" && pwd)/tla2tools.jar}"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

verify() {
  [ -f "$1" ] && [ "$(sha256_file "$1")" = "$SHA256" ]
}

if verify "$DESTINATION"; then
  printf '%s\n' "$DESTINATION"
  exit 0
fi

command -v curl >/dev/null 2>&1 || {
  echo "curl is required to install tla2tools.jar" >&2
  exit 1
}

mkdir -p "$(dirname "$DESTINATION")"
TEMPORARY="$(mktemp "${DESTINATION}.tmp.XXXXXX")"
trap 'rm -f "$TEMPORARY"' EXIT
curl --fail --location --retry 3 --output "$TEMPORARY" "$URL"
if ! verify "$TEMPORARY"; then
  echo "downloaded tla2tools.jar failed the pinned SHA-256 check" >&2
  exit 1
fi
chmod 0644 "$TEMPORARY"
mv "$TEMPORARY" "$DESTINATION"
trap - EXIT
printf '%s\n' "$DESTINATION"
