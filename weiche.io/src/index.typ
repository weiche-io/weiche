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
        "Simply and naive",
        "Built for homelab clusters that want a gateway without an enterprise proxy stack: one static Rust binary from scratch, and plain Gateway API resources.",
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
  html.h2(id: "routes")[Gateway API routes]
  html.ul(class: "route-list", {
    for (name, detail) in (
      ("HTTPRoute", "Match requests by hostname, path, method and headers."),
      ("TCPRoute", "Forward TCP connections to weighted backends."),
      ("UDPRoute", "Relay datagrams and track replies for each client."),
    ) {
      html.li({
        html.h3(html.a(
          href: "https://gateway-api.sigs.k8s.io/reference/api-types/"
            + lower(name)
            + "/",
          name,
        ))
        html.p(detail)
      })
    }
  })
})
