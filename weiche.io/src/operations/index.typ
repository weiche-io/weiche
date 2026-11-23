#import "/page.typ": article
#import "/meta.typ": site
#import "meta.typ"

#show: article.with(meta)

#let tree = site.repository + "/tree/" + site.source-ref + "/"
#let blob = site.repository + "/blob/" + site.source-ref + "/"

= Deployment boundary <boundary>

The #link(tree + "charts/weiche")[Weiche Helm chart] installs only the control
plane, the `weiche` `GatewayClass` and platform-level RBAC. It does not create
data-plane proxies or Gateway Services.

- *Helm release (platform)*: deploys the controller `Deployment`
  (`controller.replicas: 1` by default), writes platform proxy defaults to
  `<fullname>-settings` (`proxy.json`), registers the configured `GatewayClass`
  (`weiche` / `weiche.io/gateway-controller` by default), grants cluster read
  and `/status` patch access (`ClusterRole/<fullname>-gateway-controller`),
  grants release-namespace `Lease` election (`<fullname>-election`) and binds
  `ClusterRole/<fullname>-gateway-provisioner` in each namespace listed in
  `gatewayNamespaces`.
- *Controller (per Gateway)*: provisions and reconciles a dedicated Service,
  proxy `Deployment`, snapshot `Secret`, acknowledgement `ConfigMap` and
  least-privilege RBAC for every accepted `Gateway` in an authorized namespace.
- *Target namespace administrator*: owns cross-namespace `ReferenceGrant`
  objects in the namespace of any referenced TLS `Secret` or backend `Service`.

#table(
  columns: 3,
  table.header([Owner], [Resources], [Scope]),
  [Helm chart],
  [Controller `Deployment`, `<release>-settings`, `GatewayClass/weiche`,
    controller & election RBAC, `weiche-gateway-provisioner` `RoleBinding`s],
  [Release namespace + each `gatewayNamespaces` entry],

  [Controller leader],
  [`Deployment/<gateway-name>` and `weiche-gw-<uid>` `Secret`, `ConfigMap`,
    `ServiceAccount`, `Role`, `RoleBinding`, `Service`],
  [Each accepted `Gateway`'s namespace],

  [Target namespace admin],
  [`ReferenceGrant` (`gateway.networking.k8s.io/v1`)],
  [Namespace of the referenced `Secret` or `Service`],
)

Install the bundled Gateway API v1.7.0 standard CRDs, create the namespaces that
will host Gateways, and install the chart:

```sh
kubectl apply --server-side -f weiche-gtwapi/manifests/standard-install.yaml
kubectl create namespace edge-gateways

helm install weiche charts/weiche \
  --namespace weiche-system --create-namespace \
  --set 'gatewayNamespaces={edge-gateways}' \
  --set proxy.service.type=LoadBalancer
```

See #link(blob + "charts/weiche/values.yaml")[`values.yaml`] for the full set of
platform defaults (`gatewayClass.*`, `controller.*`, `proxy.*`,
`proxy.service.*` and `imagePullSecrets`). Controller and proxy workloads have
independent replicas, resources, scheduling (`nodeSelector`, `tolerations`,
`affinity`, `topologySpreadConstraints`, `priorityClassName`), pod metadata,
logging (`level` and `format: text|json`) and `extraEnv`. Changing chart
settings updates `<fullname>-settings` and its SHA-256 annotation
(`weiche.io/settings`) on the controller pod template, rolling controller
replicas cleanly.

Each proxy sizes its worker pool at startup using the CPU parallelism available
to the process. If you would like to pin the count, set `WEICHE_WORKERS` through
`proxy.extraEnv` to keep that override.

= Namespace authorization <namespaces>

`gatewayNamespaces` defines the explicit set of namespaces where Weiche may
create proxy workloads and deliver TLS material. When `gatewayNamespaces` is
empty, Helm authorizes only the release namespace.

- Every namespace listed in `gatewayNamespaces` must already exist before
  installing or upgrading the chart. Any configured `imagePullSecrets` must
  exist in each authorized namespace.
- Gateways created outside `gatewayNamespaces` are rejected with
  `Accepted: False` (`reason: NamespaceNotAllowed`), and the controller creates
  no proxy resources for them.
- Use platform-controlled namespaces for `gatewayNamespaces`. Each proxy reads a
  `Secret` in the Gateway's namespace containing its compiled routes and
  authorized TLS private keys, so untrusted workloads should not share Secret
  read access in those namespaces.

Authorization is enforced continuously on the data plane as well as the control
plane. Every proxy pod checks every 15 seconds that
`RoleBinding/weiche-gateway-provisioner` still exists in its namespace. Removing
a namespace from `gatewayNamespaces` withdraws that `RoleBinding`; running
proxies in that namespace drop readiness and exit instead of continuing to serve
cached routes.

= Per-Gateway proxies <proxies>

Controller replicas coordinate through a `coordination.k8s.io/v1` `Lease` named
`weiche-controller` in the release namespace (15s duration, 8s grace). The
active leader compiles each accepted `Gateway` into a deterministic,
zlib-compressed MessagePack snapshot (`routes.v1`) and reconciles seven
resources in the Gateway's namespace. The proxy Deployment has the Gateway's
name. Its Service, snapshot and access-control resources use
`weiche-gw-<gateway-uid>`. An authorized Gateway with a supported listener
receives its proxy and Service before its first Route attaches. Removing the
last Route leaves the Deployment and assigned address in place. A proxy can
acknowledge an empty snapshot once its workers have closed all listeners.
The Pod is then ready for configuration updates. The Gateway reports
`Programmed: False` until it has a configured listener and an assigned address.

- `Secret` (`type: weiche.io/snapshot`): carries `routes.v1` with validated
  routes and authorized TLS material. Snapshots are capped at 1 MiB compressed
  (64 MiB decoded). If a replacement exceeds the limit, the controller publishes
  an empty snapshot to withdraw stale routes and reports `Programmed: False`
  (`NoResources`).
- `ConfigMap`: records each proxy pod's acknowledged SHA-256 snapshot digest
  keyed by `POD_UID`.
- `ServiceAccount`, `Role` and `RoleBinding`: restricts each proxy strictly to
  `get`/`list`/`watch` on its own named `Secret`, `patch` on its own named
  `ConfigMap`, and `get` on `RoleBinding/weiche-gateway-provisioner`.
- `Deployment`: uses the pod template rendered by Helm and runs
  `weiche proxy weiche-gw-<uid>` (`proxy.replicas: 1` by
  default) as non-root UID `65532` with all capabilities dropped,
  `net.ipv4.ip_unprivileged_port_start=0` to bind ports such as 80 and 443 (or
  `NET_BIND_SERVICE` with `ClusterFirstWithHostNet` when
  `proxy.hostNetwork: true`), `proxy.seccompProfile: Unconfined` by default for
  `io_uring`, an in-memory `emptyDir` at `/run/weiche` and an exec readiness
  probe (`weiche ready`) every second.
- `Service`: `ClusterIP`, `LoadBalancer` or `NodePort` configured by
  `proxy.service.*` (`ipFamilyPolicy`, `ipFamilies`, `externalTrafficPolicy` and
  default annotations), selecting pods by `weiche.io/gateway-uid: <uid>` and
  exposing deduplicated listener ports (including matching `UDP` ports for
  `HTTPS` listeners when `proxy.http3.enabled: true`). Static IPv4/IPv6
  requests in `Gateway.spec.addresses` map to `clusterIP`/`clusterIPs` or
  `loadBalancerIP`, and `Gateway.spec.infrastructure` labels and annotations
  propagate to provisioned resources while preserving controller selector and
  ownership labels.

Each proxy watches only its own `Secret`. Once all local `compio` workers bind
every socket for a snapshot revision, the proxy atomically writes the digest to
`/run/weiche/ready` and patches its `POD_UID` entry in
`ConfigMap/weiche-gw-<uid>`. The controller reports `Programmed: True` on the
`Gateway` only after the `Service` has an assigned address and at least one
ready, non-terminating `EndpointSlice` endpoint has acknowledged the current
snapshot digest.

On a shared HTTPS port, the TLS handshake selects a listener by SNI. If a
request's authority selects a different listener, the proxy returns HTTP 421
so the client can open a connection for that authority. Two hostnames served
by the same wildcard listener can share a connection. An authority with no
matching HTTPS listener receives HTTP 404.

= Cross-namespace ReferenceGrant <referencegrant>

Helm authorization applies only to provisioning proxy resources inside
`gatewayNamespaces`. Cross-namespace references to TLS Secrets or backend
Services are never granted automatically; they must be maintained by the
administrator of the target namespace using `gateway.networking.k8s.io/v1`
`ReferenceGrant`.

== TLS Secrets in another namespace

An `HTTPS` listener accepts a single `kubernetes.io/tls` `Secret` (`tls.crt` and
`tls.key`) in TLS termination mode. When the `Secret` lives in a different
namespace than the `Gateway`, the Secret namespace administrator must grant
access:

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: ReferenceGrant
metadata:
  name: allow-edge-gateway-tls
  namespace: certs
spec:
  from:
    - group: gateway.networking.k8s.io
      kind: Gateway
      namespace: edge-gateways
  to:
    - group: ""
      kind: Secret
      name: app-tls
```

Omit `to[].name` to allow all Secrets in the target namespace, or keep it set to
scope the grant to one certificate. Without a matching grant, the listener
reports `ResolvedRefs: False` with reason `RefNotPermitted`.

== Cross-namespace backend Services

Routes (`HTTPRoute`, `TCPRoute` and `UDPRoute`) targeting a `Service` in another
namespace follow the same rule: the Service namespace administrator creates a
`ReferenceGrant` allowing that route kind from the route's namespace to
reference `kind: Service` (`group: ""`). `BackendTLSPolicy` always lives in the
target `Service`'s namespace and references either a same-namespace CA
`ConfigMap` (`ca.crt`) or `wellKnownCACertificates: System`.

= Certificate rotation <tls-rotation>

TLS rotation happens in place without restarting proxy pods:

+ Update `tls.crt` and `tls.key` in the referenced `kubernetes.io/tls` `Secret`
  (either in the Gateway's namespace or in a `ReferenceGrant`-authorized
  namespace).
+ The controller validates the PEM chain and private key match, recompiles the
  `GatewaySnapshot` with a new SHA-256 digest and patches
  `Secret/weiche-gw-<uid>`. If the replacement is malformed or mismatched, the
  listener reports `InvalidCertificateRef` instead of pushing broken TLS config.
+ Every proxy replica's Secret watcher decodes the new snapshot, rebuilds its
  TLS configuration, publishes it to all local workers and patches the new
  digest into `ConfigMap/weiche-gw-<uid>`. New TLS handshakes immediately
  present the rotated certificate.

== Verifying all proxy pods

Checking the `Gateway` status or hitting the `Service` VIP once can miss a
lagging replica. After rotating a Secret, confirm that every proxy pod has
acknowledged the same snapshot digest and inspect the certificate actually
served by each replica (for example, when running `proxy.replicas: 2`):

```sh
GW_NS=edge-gateways
GW_NAME=public
GW_UID=$(kubectl -n "$GW_NS" get gateway "$GW_NAME" -o jsonpath='{.metadata.uid}')

# 1. Confirm all proxy pod UIDs acknowledge the same snapshot digest.
kubectl -n "$GW_NS" get configmap "weiche-gw-$GW_UID" -o jsonpath='{.data}'; echo

# 2. Check the leaf certificate presented directly by each proxy Pod.
for pod in $(kubectl -n "$GW_NS" get pods -l "weiche.io/gateway-uid=$GW_UID" -o name); do
  echo "=== $pod ==="
  kubectl -n "$GW_NS" port-forward "$pod" 18443:443 >/dev/null 2>&1 &
  pf=$!
  sleep 1
  openssl s_client -servername example.com -connect 127.0.0.1:18443 </dev/null 2>/dev/null \
    | openssl x509 -noout -subject -serial -dates -fingerprint -sha256
  kill "$pf" && wait "$pf" 2>/dev/null || true
done
```

Both pods should report the rotated certificate's serial, validity window and
SHA-256 fingerprint, matching the new `tls.crt` stored in the `Secret`.

= Lifecycle and cleanup <lifecycle>

- *Deleting a `Gateway`*: each provisioned resource carries a controller
  `ownerReference` to its `Gateway`, so Kubernetes garbage collection removes
  the proxy `Deployment`, `Service`, snapshot `Secret`, acknowledgement
  `ConfigMap` and per-Gateway RBAC automatically.
- *Withdrawing `GatewayClass/weiche`*: if the `GatewayClass` is removed while a
  `Gateway` still exists, the active controller leader actively deletes the
  Gateway's seven resources using `uid` and `resourceVersion`
  preconditions.
- *Retiring a namespace or uninstalling Helm*: delete `Gateway` objects in the
  affected namespaces *before* removing those namespaces from
  `gatewayNamespaces` or running `helm uninstall`. Removing authorization first
  strips `RoleBinding/weiche-gateway-provisioner` and causes proxies to exit,
  leaving any remaining `Gateway`-owned objects to be cleaned up when those
  Gateways are deleted.
