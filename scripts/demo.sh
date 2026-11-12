#!/usr/bin/env bash
# Only operates on its dedicated local kind cluster, never the current kubeconfig.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p target/demo
export KIND_EXPERIMENTAL_PROVIDER=podman
export KUBECONFIG="$PWD/target/demo/kubeconfig"
cluster=weiche-demo
if ! kind get clusters | rg -Fxq "$cluster"; then
    kind create cluster --name "$cluster" --config demo/kind.yaml \
        --kubeconfig "$KUBECONFIG" --image "${KIND_NODE_IMAGE:-kindest/node:v1.35.0}" --wait 120s
else
    kind export kubeconfig --name "$cluster" --kubeconfig "$KUBECONFIG"
fi
# Be explicit on every kubectl call even if the caller has a different active context.
k=(kubectl --kubeconfig "$KUBECONFIG" --context kind-weiche-demo)
"${k[@]}" apply --server-side -f weiche-gtwapi/manifests/standard-install.yaml
"${k[@]}" wait --for=condition=Established --timeout=60s crd/gatewayclasses.gateway.networking.k8s.io crd/gateways.gateway.networking.k8s.io crd/httproutes.gateway.networking.k8s.io
if [[ ${SKIP_BUILD:-0} != 1 ]]; then
    cargo build --locked -p weiche
    scripts/build-local-image.sh
    scripts/build-local-backend.sh
fi
podman save --format docker-archive localhost/weiche:demo > target/demo/weiche.tar
kind load image-archive target/demo/weiche.tar --name "$cluster"
podman save --format docker-archive localhost/weiche-echo:demo > target/demo/backend.tar
kind load image-archive target/demo/backend.tar --name "$cluster"
"${k[@]}" apply -f demo/gateway.yaml
"${k[@]}" -n weiche-demo create configmap echo-script --from-file=backend.py=demo/backend.py --dry-run=client -o yaml | "${k[@]}" apply -f -
"${k[@]}" apply -f demo/backends.yaml
# Also restarts existing pods when rerunning after a local code/configmap change.
"${k[@]}" -n weiche-demo rollout restart deployment/weiche deployment/echo-a deployment/echo-b
for name in weiche echo-a echo-b; do
    "${k[@]}" -n weiche-demo rollout status "deployment/$name" --timeout=180s
done
"${k[@]}" -n weiche-demo wait --for=condition=Programmed gateway/weiche --timeout=60s
port=${DEMO_PORT:-18080}
"${k[@]}" -n weiche-demo port-forward service/weiche "$port:8080" >target/demo/port-forward.log 2>&1 &
forward=$!
trap 'kill "$forward" 2>/dev/null || true; wait "$forward" 2>/dev/null || true' EXIT
python3 scripts/smoke-http.py "http://127.0.0.1:$port"
printf '\nDemo passed. Cluster retained: kind-weiche-demo\n'
printf 'Kubeconfig: %s\n' "$KUBECONFIG"
printf 'Run: kubectl --kubeconfig %q -n weiche-demo port-forward service/weiche %s:8080\n' "$KUBECONFIG" "$port"
