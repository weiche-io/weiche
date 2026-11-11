# Introduction

Weiche is a high performance k8s networking gateway.

The initial control plane uses [kube-rs](https://kube.rs/) and our
[`weiche-gtwapi`](weiche-gtwapi) bindings, generated from the official
[Gateway API v1.7.0 manifests](https://github.com/kubernetes-sigs/gateway-api/releases/tag/v1.7.0), to list and watch
`gateway.networking.k8s.io/v1` resources: `GatewayClass`, `Gateway`, and
`HTTPRoute`. It observes all classes and namespaces. Reconciliation currently
logs the resources; status updates, route attachment validation, and traffic
forwarding are not implemented yet.

## Run locally

Requires Rust 1.92+ and access to a Kubernetes 1.37+ cluster with the three
Gateway API CRDs installed. Kubernetes API bindings explicitly target 1.37
(`k8s-openapi` 0.29 with `v1_37`, `kube` 5). The Gateway API bindings and
`weiche-gtwapi` crate version track upstream v1.7.0. To install its Standard CRDs
and admission policies, follow the upstream
[installation instructions](https://gateway-api.sigs.k8s.io/guides/) using the
release manifest committed at `weiche-gtwapi/manifests/standard-install.yaml`.

```sh
# Once, as a cluster administrator, if these CRDs are not installed:
kubectl apply --server-side -f weiche-gtwapi/manifests/standard-install.yaml

cargo run -p weiche
# Select a kubeconfig and print full observed resource snapshots:
KUBECONFIG=/path/to/kubeconfig RUST_LOG=info,weiche=debug cargo run -p weiche
```

The client uses the current kubeconfig context locally and service account
credentials inside Kubernetes. The active identity needs `get`, `list`, and
`watch` permissions on all three resources cluster-wide. Missing CRDs or denied
access are logged and retried with kube-runtime's backoff. Stop with Ctrl-C or
SIGTERM to shut down the controllers gracefully.

## Controller integration

`weiche::controller::GatewayController::new(client)` prepares the controllers.
Call `stores()` to obtain cloneable, typed kube-runtime cache readers, then drive
`run(shutdown_future)` in a task. Wait for each store's `wait_until_ready()` before
reading it with `state()` or `get()`. Updates, deletions, and watch reconnects are
handled by kube-runtime; the three caches are eventually consistent rather than
an atomic snapshot. The caches contain observed resources, not validated or
accepted routing configuration.

## Development

`weiche-gtwapi` contains bindings generated from the working group's Standard
release manifest. It includes all served versions of all ten Standard CRDs;
the controller currently watches the three resources listed above. See its
[crate documentation](weiche-gtwapi/README.md).

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
