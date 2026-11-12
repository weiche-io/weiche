#!/usr/bin/env bash
# Reuse the local Nix Python closure; no remote backend image is needed.
set -euo pipefail
cd "$(dirname "$0")/.."
python_root=$(python3 -c 'import sysconfig; print(sysconfig.get_config_var("prefix"))')
python="$python_root/bin/python3"
if [[ $python_root != /nix/store/* ]]; then
    echo 'Expected Nix Python; use demo/Dockerfile.backend for a conventional build.' >&2
    exit 1
fi
context=$(mktemp -d)
trap 'chmod -R u+w "$context"; rm -r "$context"' EXIT
while IFS= read -r path; do cp -a --parents "$path" "$context"; done < <(nix-store -qR "$python_root")
cat > "$context/Dockerfile" <<DOCKERFILE
FROM scratch
COPY nix /nix
USER 65532:65532
ENTRYPOINT ["$python"]
DOCKERFILE
podman build --network=none -t localhost/weiche-echo:demo "$context"
