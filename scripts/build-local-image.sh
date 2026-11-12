#!/usr/bin/env bash
# Package an already-built Nix-linked Linux binary without rebuilding Rust in a container.
set -euo pipefail
cd "$(dirname "$0")/.."
binary=${1:-target/debug/weiche}
image=${2:-localhost/weiche:demo}
context=$(mktemp -d)
trap 'chmod -R u+w "$context"; rm -r "$context"' EXIT
cp "$binary" "$context/weiche"
if command -v strip >/dev/null; then strip "$context/weiche"; fi
mapfile -t roots < <(ldd "$binary" | sed -nE 's|.*(/nix/store/[^/ ]+).*|\1|p' | sort -u)
if ((${#roots[@]} == 0)); then
    echo 'Expected a Nix-linked binary; use the repository Dockerfile for a conventional build.' >&2
    exit 1
fi
while IFS= read -r path; do cp -a --parents "$path" "$context"; done < <(nix-store -qR "${roots[@]}")
cat > "$context/Dockerfile" <<'DOCKERFILE'
FROM scratch
COPY nix /nix
COPY weiche /weiche
USER 65532:65532
ENTRYPOINT ["/weiche"]
DOCKERFILE
podman build --network=none -t "$image" "$context"
