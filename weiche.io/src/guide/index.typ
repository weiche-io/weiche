#import "/page.typ": article
#import "/meta.typ": site
#import "meta.typ"

#show: article.with(meta)

#let blob = site.repository + "/blob/" + site.source-ref + "/"
#let tree = site.repository + "/tree/" + site.source-ref + "/"

= Prerequisites <prerequisites>

Weiche runs on Kubernetes clusters with Linux nodes that support `io_uring`.
Multi-architecture container images (`linux/amd64` and `linux/arm64`) and the
Helm chart are published to GitHub Container Registry. You need `kubectl` and
`helm` configured against your cluster.

= Install Gateway API CRDs <crds>

Weiche targets the #link(
  blob + "weiche-gtwapi/manifests/source.json",
)[Gateway API v1.7.0 standard channel]. Install the standard CRDs before
deploying the controller:

```sh
kubectl apply --server-side \
  -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.7.0/standard-install.yaml
```

= Install Weiche <install>

Create a platform-controlled namespace to host `Gateway` objects and their
per-Gateway proxy workloads, then install the Helm chart:

```sh
kubectl create namespace edge-gateways

helm install weiche oci://ghcr.io/weiche-io/charts/weiche \
  --version 0.2.0 \
  --namespace weiche-system --create-namespace \
  --set 'gatewayNamespaces={edge-gateways}'
```

By default, provisioned Gateway `Service`s use `ClusterIP`. Pass
`--set proxy.service.type=LoadBalancer` on clusters with a load balancer
controller, or `--set proxy.http3.enabled=true` to serve HTTP/3 on terminating
HTTPS listeners. See #link("../reference/#helm-values")[Reference] for all Helm
configuration options.

Confirm that the controller is running and has accepted the `weiche`
`GatewayClass`:

```sh
kubectl -n weiche-system rollout status deployment/weiche
kubectl get gatewayclass weiche
```

= Expose your first service <first-route>

Deploy a sample HTTP backend in the `default` namespace:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: echo
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: echo
  template:
    metadata:
      labels:
        app: echo
    spec:
      containers:
        - name: echo
          image: registry.k8s.io/e2e-test-images/agnhost:2.53
          args: ["netexec", "--http-port=8080"]
          ports:
            - containerPort: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: echo
  namespace: default
spec:
  selector:
    app: echo
  ports:
    - port: 8080
      targetPort: 8080
```

Create a `Gateway` in `edge-gateways` and an `HTTPRoute` in `default` that
attaches to it:

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

Wait until the controller provisions the proxy `Deployment` and `Service` and
marks the `Gateway` as `Programmed: True`:

```sh
kubectl -n edge-gateways wait --for=condition=Programmed gateway/public
kubectl -n edge-gateways get gateway public
```

Test the route using `kubectl port-forward` (or directly against the assigned
load balancer IP):

```sh
kubectl -n edge-gateways port-forward deployment/public 8080:80 &
curl -H 'Host: echo.example.com' http://127.0.0.1:8080/hostname
```

From here, explore #link("../routing/")[Routing] to add HTTPS, gRPC, TLS
passthrough, TCP/UDP routes and `BackendTLSPolicy`, or read #link(
  "../reference/",
)[Reference] for all Helm values.
