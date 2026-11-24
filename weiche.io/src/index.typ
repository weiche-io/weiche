#import "/page.typ": page
#import "/meta.typ": site
#show: page.with(
  title: site.name + " — An experimental Kubernetes gateway, built in Rust",
  main-class: "landing",
)

#html.header(class: "hero wrap", {
  html.h1(id: "title")[#site.name]
  html.p(class: "lede")[An experimental Kubernetes gateway built in Rust with
    io_uring.]
  html.nav(class: "actions", aria-label: "Project resources", {
    html.a(class: "button primary", href: "guide/")[Get started]
    html.a(class: "button secondary", href: site.repository)[View source]
  })
})

#html.section(class: "wrap features", aria-labelledby: "features", {
  html.h2(id: "features")[Highlights]
  html.ul(class: "feature-list", {
    for (name, detail) in (
      (
        "Thread-per-core io_uring data plane",
        "Each worker owns a thread-local compio io_uring loop on SO_REUSEPORT sockets. Routing tables and upstream connection pools stay worker-local, keeping the hot path free of cross-thread locks.",
      ),
      (
        "HTTP/1.1, HTTP/2, HTTP/3 and L4",
        "Terminates HTTPS with SNI listener isolation, serves opt-in HTTP/3 over QUIC with cBPF worker steering, and forwards gRPC, WebSocket, TLS passthrough, TCP and UDP traffic.",
      ),
      (
        "Simple by design",
        "Built for clusters that want a gateway without an enterprise proxy stack: one static Rust binary, isolated per-Gateway proxies, live TLS reloads and plain Gateway API resources.",
      ),
    ) {
      html.li({
        html.h3(name)
        html.p(detail)
      })
    }
  })
})

#html.section(class: "wrap routes", aria-labelledby: "routes", {
  html.h2(id: "routes")[Gateway API resources]
  html.ul(class: "route-list", {
    for (name, detail, doc) in (
      (
        "HTTPRoute",
        "Match by hostname, path, method, headers and query params; apply redirects, rewrites, CORS, timeouts and WebSocket or h2c upstreams.",
        "httproute",
      ),
      (
        "GRPCRoute",
        "Route gRPC traffic by service, method and headers to weighted HTTP/2 backends.",
        "grpcroute",
      ),
      (
        "TLSRoute",
        "Forward TLS sessions directly to weighted TCP backends using SNI passthrough.",
        "tlsroute",
      ),
      (
        "TCPRoute",
        "Forward raw TCP connections to weighted Kubernetes Service endpoints.",
        "tcproute",
      ),
      (
        "UDPRoute",
        "Relay UDP datagrams to weighted backends while tracking per-client replies.",
        "udproute",
      ),
      (
        "BackendTLSPolicy",
        "Verify upstream TLS certificates against a ConfigMap CA bundle or system trust roots.",
        "backendtlspolicy",
      ),
    ) {
      html.li({
        html.h3(html.a(
          href: "https://gateway-api.sigs.k8s.io/reference/api-types/"
            + doc
            + "/",
          name,
        ))
        html.p(detail)
      })
    }
  })
})
