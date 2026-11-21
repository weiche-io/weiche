#import "page.typ": page
#import "meta.typ": site
#show: page.with(
  title: site.name + " — A Kubernetes gateway, built in Rust",
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
