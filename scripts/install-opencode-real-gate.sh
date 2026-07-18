#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 || -z "$1" ]]; then
  echo "usage: $0 DESTINATION_DIRECTORY" >&2
  exit 2
fi
if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
  echo "the release gate installer supports only Linux x86_64 GitHub runners" >&2
  exit 2
fi

readonly version="1.17.18"
readonly asset="opencode-linux-x64-baseline.tar.gz"
readonly expected_sha256="c81d5c469220604f6506b09dc46568bcdd55d2d4e63f6b4ac23e62ccd8c19479"
readonly url="https://github.com/anomalyco/opencode/releases/download/v${version}/${asset}"

destination="$1"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
archive="$work/$asset"

# The asset comes from the official anomalyco/opencode v1.17.18 release. Its
# digest is pinned here rather than fetched beside the asset, so replacement or
# corruption fails closed without assuming GitHub asset immutability.
curl --proto '=https' --tlsv1.2 --fail --location --silent --show-error \
  --retry 3 --retry-all-errors "$url" --output "$archive"
printf '%s  %s\n' "$expected_sha256" "$archive" | sha256sum --check --strict -

entries="$(tar -tzf "$archive")"
if [[ "$entries" != "opencode" ]]; then
  echo "unexpected OpenCode archive layout" >&2
  exit 1
fi
tar -xzf "$archive" -C "$work" --no-same-owner --no-same-permissions
install -d -m 0755 "$destination"
install -m 0755 "$work/opencode" "$destination/opencode"

actual_version="$($destination/opencode --version)"
if [[ "$actual_version" != "$version" ]]; then
  echo "installed OpenCode version mismatch: expected $version, got $actual_version" >&2
  exit 1
fi
echo "installed verified OpenCode $version at $destination/opencode"
