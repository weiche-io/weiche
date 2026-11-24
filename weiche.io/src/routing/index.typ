#import "/page.typ": article
#import "/meta.typ": site
#import "meta.typ"

#show: article.with(meta)

= Listeners and protocols <listeners>

A `Gateway` declares one or more listeners. Weiche accepts five listener
protocols and maps each to the corresponding Gateway API route kinds:

#table(
  columns: 4,
  table.header([Protocol], [TLS mode], [Hostname], [Supported route kinds]),
  [`HTTP`], [None], [Optional], [`HTTPRoute`, `GRPCRoute`],
  [`HTTPS`],
  [`Terminate` (one `kubernetes.io/tls` `Secret`)],
  [Optional],
  [`HTTPRoute`, `GRPCRoute`],

  [`TLS`], [`Passthrough`], [Optional], [`TLSRoute`],
  [`TCP`], [None], [Must be empty], [`TCPRoute`],
  [`UDP`], [None], [Must be empty], [`UDPRoute`],
)

Multiple `HTTP`, `HTTPS`, or `TLS` passthrough listeners can share the same port
as long as they use the same protocol and distinct hostnames. Raw `TCP` and
`UDP` listeners cannot share a port with another listener of the same transport.

== HTTPS termination and SNI isolation

Terminating `HTTPS` listeners negotiate HTTP/2 (`h2`) and HTTP/1.1 (`http/1.1`)
over TLS. Each listener references a single core `kubernetes.io/tls` `Secret`
(`tls.crt` and `tls.key`). Cross-namespace `Secret` references require a
`ReferenceGrant` in the `Secret` namespace.

When multiple `HTTPS` listeners share a port, the TLS handshake selects the
listener by SNI (preferring an exact hostname, then the longest wildcard suffix,
then an unrestricted listener). On an established connection, if a request's
`:authority` or `Host` header maps to a different listener, Weiche responds with
HTTP `421 Misdirected Request` so the client opens a fresh connection for that
hostname. Two hostnames covered by the same wildcard listener can share a
connection. Requests whose authority matches no listener receive HTTP `404`.

== Opt-in HTTP/3

Enabling `proxy.http3.enabled: true` in Helm binds a QUIC v1 (`h3`) UDP listener
alongside every terminating `HTTPS` listener and adds matching UDP ports to the
provisioned Gateway `Service`. Weiche automatically advertises HTTP/3 via the
`Alt-Svc` response header on HTTPS responses (using `proxy.http3.advertisedPort`
when the external UDP port differs from the listener port).

If a `Gateway` also defines an explicit `UDP` listener on the same port as an
`HTTPS` listener, the explicit `UDP` listener takes precedence over HTTP/3 on
that port.

= HTTPRoute <http>

`HTTPRoute` attaches to `HTTP` and `HTTPS` listeners. Hostname intersections
follow Gateway API precedence (exact hostnames before wildcards, then longest
wildcard suffix).

== Matching

Each rule supports any combination of match predicates:

- *Path*: `PathPrefix` (default `/`), `Exact`, or `RegularExpression` (Rust
  regex syntax, validated by the controller before snapshot publication).
- *Method*: `GET`, `HEAD`, `POST`, `PUT`, `DELETE`, `CONNECT`, `OPTIONS`,
  `TRACE`, `PATCH`.
- *Headers* and *query parameters*: `Exact` or `RegularExpression`. When a match
  lists duplicate header or query parameter names, the first entry wins.

== Filters and traffic splitting

Rules and individual `backendRefs` support weighted load balancing (`weight`,
default `1`; `weight: 0` disables a backend) and the following filters:

- `RequestHeaderModifier` (`set`, `add`, `remove`) — supported on rules and
  individual `backendRefs`.
- `ResponseHeaderModifier` (`set`, `add`, `remove`) — supported on rules and
  individual `backendRefs`.
- `RequestRedirect` — supports `scheme` (`http`/`https`), `hostname`, `port`,
  `path` (`ReplaceFullPath` or `ReplacePrefixMatch`), and status codes `301`,
  `302` (default), `303`, `307`, and `308`.
- `URLRewrite` — supports `hostname` and `path` (`ReplaceFullPath` or
  `ReplacePrefixMatch`) on rules and `backendRefs`. `ReplacePrefixMatch`
  requires a single `PathPrefix` match in the rule.
- `CORS` — configures `allowOrigins`, `allowMethods`, `allowHeaders`,
  `exposeHeaders`, `allowCredentials`, and `maxAge` (default `5` seconds).

`RequestRedirect` and `URLRewrite` cannot be combined on the same rule or
between a rule and its backend filters (`IncompatibleFilters`). Rule-level
`retry` and `RequestMirror` are not currently supported.

== Timeouts

`HTTPRoute` rules support GEP-2257 `timeouts.request` (total request budget) and
`timeouts.backendRequest` (per-backend attempt budget) using `ms`, `s`, `m`, and
`h` units. A value of `0s` disables the corresponding timeout. When omitted, the
proxy enforces a default 10-second response-header timeout.

== Upstream protocols (`appProtocol`)

Weiche inspects `Service.spec.ports[].appProtocol` to select the upstream
transport protocol:

#table(
  columns: 3,
  table.header([`appProtocol`], [Upstream transport], [BackendTLSPolicy]),
  [Omitted, `http`, `kubernetes.io/http`], [HTTP/1.1], [Optional],
  [`https`], [HTTP/1.1 over TLS], [Required],
  [`kubernetes.io/h2c`],
  [Cleartext HTTP/2 (prior knowledge)],
  [Must not be set],

  [`h2`], [HTTP/2 over TLS], [Required],
  [`kubernetes.io/ws`], [HTTP/1.1 + WebSocket upgrade], [Must not be set],
  [`kubernetes.io/wss`], [HTTP/1.1 over TLS + WebSocket upgrade], [Required],
)

If a `Service` port declares an `appProtocol` that requires TLS without a
matching `BackendTLSPolicy` (or declares a cleartext protocol while a
`BackendTLSPolicy` targets it), the route reports `ResolvedRefs: False` with
reason `UnsupportedProtocol`.

== Example

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: api
  namespace: apps
spec:
  parentRefs:
    - name: public
      namespace: edge-gateways
      sectionName: https
  hostnames:
    - api.example.com
  rules:
    - name: v2-canary
      matches:
        - path:
            type: PathPrefix
            value: /v2
          headers:
            - name: x-canary
              value: "true"
      filters:
        - type: URLRewrite
          urlRewrite:
            path:
              type: ReplacePrefixMatch
              replacePrefixMatch: /
        - type: ResponseHeaderModifier
          responseHeaderModifier:
            set:
              - name: x-served-by
                value: weiche
      timeouts:
        request: 15s
        backendRequest: 5s
      backendRefs:
        - name: api-v2
          port: 8080
          weight: 20
        - name: api-v1
          port: 8080
          weight: 80
```

= GRPCRoute <grpc>

`GRPCRoute` attaches to `HTTP` and `HTTPS` listeners and forwards matched calls
to upstream Kubernetes `Service`s over HTTP/2 (either cleartext `h2c` or HTTP/2
over TLS when paired with a `BackendTLSPolicy`).

- Match by gRPC `method.service` and/or `method.method` using `Exact` or
  `RegularExpression`, plus request `headers`.
- Apply `RequestHeaderModifier` and `ResponseHeaderModifier` filters on rules
  and individual `backendRefs`.
- Split traffic across multiple weighted `backendRefs`.

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: GRPCRoute
metadata:
  name: payments
  namespace: apps
spec:
  parentRefs:
    - name: public
      namespace: edge-gateways
      sectionName: https
  hostnames:
    - grpc.example.com
  rules:
    - name: charge
      matches:
        - method:
            service: payments.v1.Billing
            method: Charge
      backendRefs:
        - name: billing-svc
          port: 50051
```

= TLSRoute <tls>

`TLSRoute` attaches to `Gateway` listeners with `protocol: TLS` and
`tls.mode: Passthrough`. The proxy reads the TLS ClientHello SNI without
terminating encryption and streams the raw TCP connection to a weighted backend
endpoint.

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: passthrough
  namespace: edge-gateways
spec:
  gatewayClassName: weiche
  listeners:
    - name: tls
      protocol: TLS
      port: 443
      hostname: "*.internal.example.com"
      tls:
        mode: Passthrough
      allowedRoutes:
        namespaces:
          from: All
---
apiVersion: gateway.networking.k8s.io/v1
kind: TLSRoute
metadata:
  name: vault
  namespace: security
spec:
  parentRefs:
    - name: passthrough
      namespace: edge-gateways
  hostnames:
    - vault.internal.example.com
  rules:
    - backendRefs:
        - name: vault
          port: 8200
```

Each `TLSRoute` accepts a single rule with one or more weighted `backendRefs`.
If two `TLSRoute`s claim the same effective hostname on a listener, the oldest
route takes precedence.

= TCPRoute and UDPRoute <l4>

`TCPRoute` and `UDPRoute` attach to raw `TCP` and `UDP` listeners respectively.
Listeners for these protocols must omit `hostname` and `tls`, and each route
specifies a single rule with weighted `backendRefs`.

- *`TCPRoute`*: accepts TCP connections and proxies them bidirectionally to
  ready endpoints of the selected backend `Service`.
- *`UDPRoute`*: relays UDP datagrams to weighted backend endpoints and tracks
  per-client return flows so backend replies return to the originating client.
  Idle UDP flows expire after 30 seconds.

= Upstream TLS with BackendTLSPolicy <backend-tls>

Use `BackendTLSPolicy` (`gateway.networking.k8s.io/v1`) to require TLS and
certificate verification when Weiche connects to an upstream `Service`.

- A `BackendTLSPolicy` must reside in the same namespace as its target `Service`
  and specify a single `targetRefs` entry (`group: ""`, `kind: Service`, with
  optional `sectionName` matching a named `Service` port).
- `validation.hostname` must be a valid DNS hostname used for SNI and
  certificate verification.
- Provide trust roots via either a single same-namespace `ConfigMap` in
  `validation.caCertificateRefs` containing `ca.crt` (in `data` or
  `binaryData`), or `validation.wellKnownCACertificates: System` to use the
  container's system CA store.
- When multiple policies target the same `Service`, an explicit `sectionName`
  match takes precedence over a Service-wide policy, followed by the oldest
  policy and then lexical name. Overridden policies report `Accepted: False`
  (`Conflicted`).

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: BackendTLSPolicy
metadata:
  name: api-upstream-tls
  namespace: apps
spec:
  targetRefs:
    - group: ""
      kind: Service
      name: api-v2
      sectionName: https
  validation:
    hostname: api-v2.apps.svc.cluster.local
    caCertificateRefs:
      - group: ""
        kind: ConfigMap
        name: internal-ca
```
