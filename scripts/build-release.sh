#!/usr/bin/env bash
set -euo pipefail

version="${VERSION:-}"
commit="${COMMIT:-unknown}"
build_date="${BUILD_DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
dist="${DIST:-dist}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

version="${version#v}"
if ! "$root/scripts/github-release-flags.sh" "v$version" >/dev/null 2>&1; then
  echo "VERSION must be a release SemVer such as 0.1.0 or 0.1.0-rc.1" >&2
  exit 2
fi
if [[ "$commit" != "unknown" && ! "$commit" =~ ^[0-9a-f]{40}$ ]]; then
  echo "COMMIT must be a full lowercase Git SHA or unknown" >&2
  exit 2
fi
if [[ -z "$build_date" ]]; then
  echo "BUILD_DATE is required" >&2
  exit 2
fi
if [[ "$commit" != "unknown" ]]; then
  actual_commit="$(git -C "$root" rev-parse HEAD 2>/dev/null || true)"
  if [[ "$actual_commit" != "$commit" ]]; then
    echo "COMMIT does not match the source checkout HEAD" >&2
    exit 2
  fi
  if [[ -n "$(git -C "$root" status --porcelain=v1 --untracked-files=normal)" ]]; then
    echo "release source checkout is dirty; commit or remove every source change before building" >&2
    exit 2
  fi
fi

dist="$(mkdir -p "$dist" && cd "$dist" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
rm -f "$dist"/human_"$version"_* "$dist/checksums.txt"

ldflags="-s -w -X github.com/vibe-agi/human/internal/buildinfo.Version=$version -X github.com/vibe-agi/human/internal/buildinfo.Commit=$commit -X github.com/vibe-agi/human/internal/buildinfo.Date=$build_date"
platforms=(
  "darwin amd64"
  "darwin arm64"
  "linux amd64"
  "linux arm64"
  "windows amd64"
  "windows arm64"
)

for platform in "${platforms[@]}"; do
  read -r goos goarch <<<"$platform"
  archive="human_${version}_${goos}_${goarch}"
  stage="$work/$archive"
  mkdir -p "$stage"
  binary="human"
  if [[ "$goos" == "windows" ]]; then
    binary="human.exe"
  fi
  (
    cd "$root"
    CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" go build \
      -buildvcs=false -trimpath -ldflags "$ldflags" -o "$stage/$binary" ./cmd/human
  )
  cp "$root/README.md" "$stage/README.md"
  cp -R "$root/docs" "$stage/docs"
  cp -R "$root/examples" "$stage/examples"
  if [[ -f "$root/LICENSE" ]]; then
    cp "$root/LICENSE" "$stage/LICENSE"
  fi
  if [[ "$goos" == "windows" ]]; then
    (
      cd "$stage"
      entries=("$binary" README.md docs examples)
      [[ -f LICENSE ]] && entries+=(LICENSE)
      zip -q -9 -r "$dist/$archive.zip" "${entries[@]}"
    )
  else
    entries=("$binary" README.md docs examples)
    [[ -f "$stage/LICENSE" ]] && entries+=(LICENSE)
    tar -C "$stage" -czf "$dist/$archive.tar.gz" "${entries[@]}"
  fi
done

(
  cd "$dist"
  artifacts=(human_"$version"_*)
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${artifacts[@]}" > checksums.txt
  else
    shasum -a 256 "${artifacts[@]}" > checksums.txt
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum --check checksums.txt >/dev/null
  else
    shasum -a 256 --check checksums.txt >/dev/null
  fi
)

echo "built Human $version for ${#platforms[@]} platforms in $dist"
