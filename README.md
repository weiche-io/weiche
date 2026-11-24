# Weiche

An experimental, high-performance Kubernetes Gateway API implementation built in Rust with `io_uring`.

[Documentation](https://weiche.io) · [Get started](https://weiche.io/guide/) · [Routing](https://weiche.io/routing/) · [Architecture](https://weiche.io/architecture/) · [Reference](https://weiche.io/reference/)

## Highlights

- **Thread-per-core `io_uring` data plane**: Each worker owns a thread-local `compio` `io_uring` runtime on `SO_REUSEPORT` sockets. Routing tables and upstream connection pools stay worker-local, keeping the hot path free of cross-thread locks.
- **Full-stack Gateway API routing**: Supports `HTTPRoute`, `GRPCRoute`, `TLSRoute` (SNI passthrough), `TCPRoute`, `UDPRoute`, `BackendTLSPolicy`, and `ReferenceGrant` against Gateway API v1.7.0.
- **Modern protocols**: Downstream HTTP/1.1, HTTP/2, opt-in HTTP/3 (QUIC v1 with `Alt-Svc` and Linux cBPF Connection ID steering), WebSocket, gRPC, and L4 TCP/UDP forwarding.
- **Per-Gateway isolation**: Every accepted `Gateway` receives its own proxy `Deployment`, `Service`, least-privilege RBAC, and compressed snapshot `Secret`. TLS certificates rotate in place without restarting proxy pods.
- **Single static binary**: No external proxy sidecar or enterprise control-plane stack. Multi-architecture container images (`linux/amd64`, `linux/arm64`) and a Helm chart are published to GHCR.

## Quick start

### Prerequisites

- A Kubernetes cluster running on Linux nodes with `io_uring` enabled
- `kubectl` and `helm`

### 1. Install Gateway API v1.7.0 CRDs

```sh
kubectl apply --server-side \
  -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.7.0/standard-install.yaml
```

### 2. Install Weiche with Helm

Create a platform-controlled namespace for your Gateways and install the chart:

```sh
kubectl create namespace edge-gateways

helm install weiche oci://ghcr.io/weiche-io/charts/weiche \
  --version 0.2.0 \
  --namespace weiche-system --create-namespace \
  --set 'gatewayNamespaces={edge-gateways}'
```

Set `--set proxy.service.type=LoadBalancer` if your cluster provisions external load balancers, or `--set proxy.http3.enabled=true` to serve HTTP/3 on HTTPS listeners.

Verify that the controller and `GatewayClass` are ready:

```sh
kubectl -n weiche-system rollout status deployment/weiche
kubectl get gatewayclass weiche
```

### 3. Create a Gateway and HTTPRoute

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: public
  namespace: edge-gateways
spec:
  gatewayClassName: weiche
  listeners:
    - name: http
      protocol: HTTP
      port: 80
      allowedRoutes:
        namespaces:
          from: All
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: echo
  namespace: default
spec:
  parentRefs:
    - name: public
      namespace: edge-gateways
  hostnames:
    - echo.example.com
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /
      backendRefs:
        - name: echo
          port: 8080
```

Wait until the Gateway reports `Programmed: True` and inspect its assigned address:

```sh
kubectl -n edge-gateways wait --for=condition=Programmed gateway/public
kubectl -n edge-gateways get gateway public
```

## Supported Gateway API resources

Weiche targets the **Gateway API v1.7.0 standard channel**:

| Resource | Support |
| --- | --- |
| `GatewayClass` | Controller `weiche.io/gateway-controller` with advertised `supportedFeatures` |
| `Gateway` | `HTTP`, `HTTPS` (termination), `TLS` (passthrough), `TCP`, and `UDP` listeners; `spec.addresses` and `spec.infrastructure` |
| `HTTPRoute` | Path/method/header/query matching (including regex), weighted backends, header modifiers, redirects (`301`/`302`/`303`/`307`/`308`), URL/host rewrites, CORS, request/backend timeouts, `h2c`, and WebSocket |
| `GRPCRoute` | Service/method and header matching, header modifiers, weighted HTTP/2 backends |
| `TLSRoute` | SNI-based TLS passthrough to weighted TCP backends |
| `TCPRoute` | Weighted Layer 4 TCP forwarding |
| `UDPRoute` | Weighted UDP datagram relay with per-client reply tracking |
| `BackendTLSPolicy` | Upstream TLS verification using a same-namespace CA `ConfigMap` (`ca.crt`) or system trust roots |
| `ReferenceGrant` | Cross-namespace access for Gateway TLS `Secret`s and route backend `Service`s |

See the [Reference](https://weiche.io/reference/) for the complete feature list, Helm values, CLI options, and status conditions.

## Documentation

Full user documentation is published at [weiche.io](https://weiche.io):

- [Get started](https://weiche.io/guide/) — install Weiche, expose your first service, or run the Gateway API conformance suite on `kind`
- [Routing](https://weiche.io/routing/) — configure `HTTPRoute`, `GRPCRoute`, `TLSRoute`, `TCPRoute`, `UDPRoute`, HTTP/3, and `BackendTLSPolicy`
- [Architecture](https://weiche.io/architecture/) — control plane, snapshot protocol, and `io_uring` data plane
- [Reference](https://weiche.io/reference/) — Helm chart values, CLI and environment variables, Gateway API support, and status conditions

## Status and requirements

Weiche is experimental software under active development. Proxy pods run as non-root (`UID 65532`) with all Linux capabilities dropped and default to `seccompProfile.type: Unconfined` so `io_uring` syscalls are permitted by the container runtime.

## License

[MIT](LICENSE)
