#import "/page.typ": article
#import "/meta.typ": site
#import "meta.typ"

#show: article.with(meta)

#let blob = site.repository + "/blob/" + site.source-ref + "/"

= Helm values <helm-values>

The #link(blob + "charts/weiche/values.yaml")[Weiche Helm chart] installs the
control plane, `GatewayClass`, RBAC, and platform proxy defaults.

== Image and release

#table(
  columns: 3,
  table.header([Key], [Default], [Description]),
  [`nameOverride`], [`""`], [Override the chart name used in resource names.],
  [`fullnameOverride`], [`""`], [Override the full release resource prefix.],
  [`image.repository`],
  [`ghcr.io/weiche-io/weiche`],
  [Container image repository for controller and proxy pods.],

  [`image.tag`],
  [`""`],
  [Image tag override. Empty uses `Chart.yaml` `appVersion`.],

  [`image.pullPolicy`], [`IfNotPresent`], [Kubernetes image pull policy.],
  [`imagePullSecrets`],
  [`[]`],
  [Pull secrets attached to controller and proxy pods. Must exist in every
    authorized Gateway namespace.],
)

== GatewayClass and namespaces

#table(
  columns: 3,
  table.header([Key], [Default], [Description]),
  [`gatewayClass.create`],
  [`true`],
  [Create the `GatewayClass` resource with the release.],

  [`gatewayClass.name`],
  [`""`],
  [Name of the `GatewayClass`. Empty defaults to the release fullname (for
    example `weiche`).],

  [`gatewayClass.controllerName`],
  [`weiche.io/gateway-controller`],
  [Controller name matched on `GatewayClass.spec.controllerName`.],

  [`gatewayNamespaces`],
  [`[]`],
  [Existing namespaces where the controller may provision proxy workloads. Empty
    authorizes only the release namespace.],
)

== Controller workload

#table(
  columns: 3,
  table.header([Key], [Default], [Description]),
  [`controller.replicas`],
  [`1`],
  [Number of controller replicas (active-standby via `Lease` election).],

  [`controller.resources`],
  [`50m` CPU / `32Mi`–`128Mi` RAM],
  [Resource requests and limits for the controller container.],

  [`controller.terminationGracePeriodSeconds`],
  [`10`],
  [Grace period before killing the controller pod on shutdown.],

  [`controller.log.level`], [`info`], [Log filter passed via `RUST_LOG`.],
  [`controller.log.format`],
  [`text`],
  [Log format (`text` or `json`) passed via `WEICHE_LOG_FORMAT`.],

  [`controller.nodeSelector`, `tolerations`, `affinity`,
    `topologySpreadConstraints`, `priorityClassName`],
  [Empty],
  [Standard Kubernetes pod scheduling controls.],

  [`controller.podLabels`, `controller.podAnnotations`],
  [`{}`],
  [Extra labels and annotations on controller pods.],

  [`controller.extraEnv`],
  [`[]`],
  [Additional environment variables on the controller container.],
)

== Proxy workload, Service and HTTP/3

#table(
  columns: 3,
  table.header([Key], [Default], [Description]),
  [`proxy.replicas`],
  [`1`],
  [Replica count for each provisioned Gateway proxy `Deployment`.],

  [`proxy.bindAddress`], [`"::"`], [Local IP address bound by proxy listeners.],
  [`proxy.hostNetwork`],
  [`false`],
  [Run proxy pods on the host network (adds `NET_BIND_SERVICE` and defaults
    `dnsPolicy` to `ClusterFirstWithHostNet`).],

  [`proxy.dnsPolicy`], [`""`], [Optional pod `dnsPolicy` override.],
  [`proxy.terminationGracePeriodSeconds`],
  [`30`],
  [Grace period for proxy pods to drain active connections.],

  [`proxy.seccompProfile`],
  [`{type: Unconfined}`],
  [Seccomp profile for proxy pods. Must permit `io_uring` syscalls.],

  [`proxy.resources`],
  [`100m` CPU / `64Mi`–`256Mi` RAM],
  [Resource requests and limits for each proxy container.],

  [`proxy.log.level`], [`info`], [Log filter passed via `RUST_LOG`.],
  [`proxy.log.format`],
  [`text`],
  [Log format (`text` or `json`) passed via `WEICHE_LOG_FORMAT`.],

  [`proxy.nodeSelector`, `tolerations`, `affinity`, `topologySpreadConstraints`,
    `priorityClassName`],
  [Empty],
  [Standard Kubernetes pod scheduling controls (`kubernetes.io/os: linux` is
    always merged).],

  [`proxy.podLabels`, `proxy.podAnnotations`],
  [`{}`],
  [Extra labels and annotations on proxy pods.],

  [`proxy.extraEnv`],
  [`[]`],
  [Extra env vars for proxy pods (for example `WEICHE_WORKERS`). Cannot override
    chart-managed vars (`POD_UID`, `WEICHE_SNAPSHOT`,
    `WEICHE_PROVISIONER_BINDING`, `WEICHE_HTTP3`, `WEICHE_HTTP3_PORT`).],

  [`proxy.service.type`],
  [`ClusterIP`],
  [Service type for provisioned Gateway Services (`ClusterIP`, `LoadBalancer`,
    or `NodePort`).],

  [`proxy.service.ipFamilyPolicy`],
  [`PreferDualStack`],
  [IP family policy (`SingleStack`, `PreferDualStack`, or `RequireDualStack`).],

  [`proxy.service.ipFamilies`],
  [`[]`],
  [Optional ordered list of IP families (`IPv4`, `IPv6`).],

  [`proxy.service.externalTrafficPolicy`],
  [`""`],
  [Optional `Cluster` or `Local` traffic policy for `LoadBalancer` or `NodePort`
    Services.],

  [`proxy.service.annotations`],
  [`{}`],
  [Default annotations merged onto every provisioned Gateway Service.],

  [`proxy.http3.enabled`],
  [`false`],
  [Bind QUIC v1 (`h3`) UDP listeners alongside terminating HTTPS listeners and
    expose UDP ports on Gateway Services.],

  [`proxy.http3.advertisedPort`],
  [`null`],
  [Optional external UDP port (`1`–`65535`) advertised in `Alt-Svc` when
    different from the listener port.],
)

= CLI and environment variables <cli>

The `weiche` binary exposes three subcommands:

- `weiche controller` — starts the Kubernetes Gateway API controller. Reads
  platform installation settings from `/etc/weiche/proxy.json` and requires
  `POD_UID` for leader election identity.
- `weiche proxy <gateway-resource-name>` — starts the `io_uring` data plane for
  a single Gateway (for example `weiche proxy weiche-gw-<uid>`).
- `weiche ready` — kubelet exec readiness probe; prints the applied snapshot
  digest from `/run/weiche/ready` and exits non-zero while listeners are
  pending.

== Environment variables

#table(
  columns: 3,
  table.header([Variable], [Default], [Description]),
  [`RUST_LOG`], [`info`], [Log filter for controller and proxy, including per-target overrides.],
  [`WEICHE_LOG_FORMAT`], [`text`], [Log output format: `text` or `json`.],
  [`WEICHE_WORKERS`],
  [Available CPU parallelism],
  [Positive integer pinning the number of `compio` worker threads in
    `weiche proxy`.],

  [`WEICHE_HTTP3`],
  [`false`],
  [Enable downstream HTTP/3 (`true`/`1` or `false`/`0`). Managed by
    `proxy.http3.enabled`.],

  [`WEICHE_HTTP3_PORT`],
  [Listener port],
  [Override UDP port advertised in `Alt-Svc`. Managed by
    `proxy.http3.advertisedPort`.],

  [`WEICHE_PROVISIONER_BINDING`],
  [`weiche-gateway-provisioner`],
  [Name of the `RoleBinding` checked every 15 seconds by proxy pods for
    namespace authorization.],

  [`POD_UID`],
  [Required],
  [Pod UID used as controller leader identity and proxy acknowledgement key in
    `ConfigMap`.],
)

== HTTP access logs

The proxy emits one info-level event on the `weiche::access` target for each
parsed HTTP/1, HTTP/2, or HTTP/3 request. Each event records the peer address,
method, original path (without the query string), HTTP version, and status.
`duration_us` measures the interval from request headers to response selection.
A stream that ends before response selection has a null status.

Access events follow `WEICHE_LOG_FORMAT` and are enabled at the default
`info` level. Set `RUST_LOG=info,weiche::access=off` to keep operational logs
only. Each producer thread has a bounded insomnilog queue. When it fills,
new records are dropped and the backend reports the total at shutdown.

== Built-in proxy runtime defaults

#table(
  columns: 2,
  table.header([Setting], [Default]),
  [Downstream connections / flows (global)], [`1024`],
  [Upstream connections (per worker)], [`1024`],
  [Upstream idle connection timeout], [`30s`],
  [TCP listen backlog / `TCP_NODELAY`], [`1024` / `true`],
  [TCP upstream connect timeout], [`10s`],
  [Default HTTP response header timeout], [`10s`],
  [TLS handshake timeout], [`10s`],
  [Connection drain / worker drain timeout], [`5s` / `6s`],
  [HTTP/1 max headers / max buffer size], [`100` / `32 KiB`],
  [HTTP/2 max concurrent streams], [`128`],
  [QUIC max concurrent bidi streams / idle timeout], [`128` / `30s`],
  [UDP flow idle timeout / socket buffers], [`30s` / `2 MiB` recv & send],
)

= Gateway API support <gateway-api>

Weiche targets the *Gateway API v1.7.0 standard channel* and publishes the
following `supportedFeatures` on `GatewayClass.status.supportedFeatures`:

- *Core & Gateway*: `Gateway`, `GatewayPort8080`, `GatewayInfrastructure`,
  `GatewayHTTPListenerIsolation`,
  `GatewayHTTPSListenerDetectMisdirectedRequests`,
  `GatewayRouteHostnameIntersectionPrecedence`, `ReferenceGrant`.
- *HTTPRoute*: `HTTPRoute`, `HTTPRouteNamedRouteRule`, `HTTPRouteParentRefPort`,
  `HTTPRouteDestinationPortMatching`, `HTTPRouteMethodMatching`,
  `HTTPRouteQueryParamMatching`, `HTTPRouteRequestTimeout`,
  `HTTPRouteBackendTimeout`, `HTTPRouteCORS`, `HTTPRouteHostRewrite`,
  `HTTPRoutePathRewrite`, `HTTPRouteBackendURLRewrite`, `HTTPRoutePathRedirect`,
  `HTTPRoutePortRedirect`, `HTTPRouteSchemeRedirect`,
  `HTTPRoute303RedirectStatusCode`, `HTTPRoute307RedirectStatusCode`,
  `HTTPRoute308RedirectStatusCode`, `HTTPRouteResponseHeaderModification`,
  `HTTPRouteBackendRequestHeaderModification`, `HTTPRouteBackendProtocolH2C`,
  `HTTPRouteBackendProtocolWebSocket`.
- *GRPCRoute*: `GRPCRoute`, `GRPCRouteNamedRouteRule`,
  `GRPCRouteRequestHeaderModifier`.
- *Layer 4 & TLS*: `TLSRoute`, `TCPRoute`, `UDPRoute`, `BackendTLSPolicy`.

== Current limitations

- `GatewayClass.spec.parametersRef` and
  `Gateway.spec.infrastructure.parametersRef` are not supported
  (`InvalidParameters`).
- Gateway-wide `Gateway.spec.tls` options are not supported.
- `TLS` listeners support `Passthrough` mode only; `HTTPS` listeners support
  `Terminate` mode with a single `kubernetes.io/tls` `Secret`.
- `HTTPRoute` rule `retry` and `RequestMirror` filters are not supported.
- `BackendTLSPolicy` supports a single `Service` target, DNS
  `validation.hostname`, and either one same-namespace CA `ConfigMap` (`ca.crt`)
  or `wellKnownCACertificates: System` (`subjectAltNames` and custom `options`
  are not supported).

= Status conditions <status-conditions>

== GatewayClass

#table(
  columns: 3,
  table.header([Condition], [Reasons], [Meaning]),
  [`Accepted`],
  [`Accepted`, `InvalidParameters`],
  [Accepted when `parametersRef` is omitted.],
)

== Gateway and listeners

#table(
  columns: 3,
  table.header([Scope / Condition], [Reasons], [Meaning]),
  [Gateway `Accepted`],
  [`Accepted`, `ListenersNotValid`, `NamespaceNotAllowed`, `InvalidParameters`],
  [Whether the Gateway is in an authorized namespace and its listeners are
    valid.],

  [Gateway `Programmed`],
  [`Programmed`, `Pending`, `NoResources`],
  [`True` only after the Gateway `Service` has an address and at least one ready
    proxy pod acknowledges the current snapshot.],

  [Listener `Accepted`],
  [`Accepted`, `Conflicted`, `ProtocolConflict`, `UnsupportedProtocol`,
    `UnsupportedValue`, `Invalid`],
  [Validation result for the individual listener.],

  [Listener `Conflicted`],
  [`NoConflicts`, `ProtocolConflict`, `HostnameConflict`],
  [Port sharing conflict detection across listeners on the same Gateway.],

  [Listener `ResolvedRefs`],
  [`ResolvedRefs`, `InvalidCertificateRef`, `RefNotPermitted`,
    `InvalidRouteKinds`],
  [Validation of listener TLS `Secret` references and `allowedRoutes.kinds`.],

  [Listener `Programmed`],
  [`Programmed`, `Pending`],
  [Whether the listener is accepted and programmed on the data plane.],
)

== Routes and BackendTLSPolicy

#table(
  columns: 3,
  table.header([Resource / Condition], [Reasons], [Meaning]),
  [Route `Accepted`],
  [`Accepted`, `NoMatchingParent`, `NotAllowedByListeners`,
    `NoMatchingListenerHostname`, `UnsupportedValue`, `IncompatibleFilters`],
  [Per-`parentRef` attachment and rule validation result.],

  [Route `ResolvedRefs`],
  [`ResolvedRefs`, `InvalidKind`, `RefNotPermitted`, `BackendNotFound`,
    `InvalidBackendTLSPolicy`, `UnsupportedProtocol`],
  [Backend `Service`, `ReferenceGrant`, `appProtocol`, and `BackendTLSPolicy`
    resolution.],

  [Route `PartiallyInvalid`],
  [`UnsupportedValue`],
  [Set to `True` when a route contains both valid rules and rejected rules.],

  [`BackendTLSPolicy` `Accepted`],
  [`Accepted`, `Conflicted`, `Invalid`, `NoValidCACertificate`,
    `TargetNotFound`],
  [Per-Gateway ancestor policy validation and conflict precedence.],

  [`BackendTLSPolicy` `ResolvedRefs`],
  [`ResolvedRefs`, `InvalidCACertificateRef`, `InvalidKind`, `TargetNotFound`],
  [Resolution of the target `Service` port and CA `ConfigMap`.],
)
