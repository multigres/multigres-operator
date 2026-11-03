#!/usr/bin/env bash
# Compare actual image manifests, not run-specific provenance envelopes.
set -euo pipefail

arch=${1:-amd64}
case "$arch" in amd64|arm64) ;; *) echo 'Expected amd64 or arm64' >&2; exit 1 ;; esac
work=$(mktemp -d)
builder="operator-repro-$$"
cleanup() {
  docker buildx rm "$builder-1" "$builder-2" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

sha=$(git rev-parse HEAD)
date=$(git show -s --format=%cI HEAD)
epoch=$(git show -s --format=%ct HEAD)
for attempt in 1 2; do
  docker buildx create --name "$builder-$attempt" --driver docker-container \
    --driver-opt image=moby/buildkit:v0.34.1@sha256:99368f3e46177c6405b9740e4461d91cee004128daeecca78d4ceac50ec1da5c \
    --driver-opt network=host
  docker buildx build --builder "$builder-$attempt" --network=host --no-cache \
    --platform "linux/$arch" --provenance=false --sbom=false \
    --build-arg "VERSION=$sha" --build-arg "GIT_COMMIT=$sha" \
    --build-arg "BUILD_DATE=$date" --build-arg "SOURCE_DATE_EPOCH=$epoch" \
    --output "type=oci,dest=$work/$attempt.tar,rewrite-timestamp=true" \
    --metadata-file "$work/$attempt.json" .
  jq -er '.["containerimage.digest"] | select(test("^sha256:[0-9a-f]{64}$"))' \
    "$work/$attempt.json" > "$work/$attempt.digest"
done
diff -u "$work/1.digest" "$work/2.digest"
printf 'Reproducible linux/%s image: %s\n' "$arch" "$(cat "$work/1.digest")"
