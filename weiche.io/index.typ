#import "page.typ": page
#import "meta.typ": site
#show: page.with(body-class: "home")

#html.section(class: "hero hero-layout wrap", aria-labelledby: "title", {
  html.header(class: "hero-copy", {
    html.h1(id: "title")[#site.name]
    html.p(class: "lede")[An experimental Kubernetes gateway built in Rust with io_uring.]
    html.nav(class: "actions", aria-label: "Project resources", {
      html.a(class: "button primary", href: "guide/")[Get started]
      html.a(class: "button secondary", href: site.repository)[View source]
    })
  })
  html.figure(class: "flow", role: "img",
    aria-label: "Weiche routes incoming HTTP, TCP and UDP traffic to ready backend endpoints.", {
    html.span(class: "ports", {
      for proto in ("HTTP", "TCP", "UDP") { html.span(proto) }
    })
    html.span(class: "wire")
    html.span(class: "chip", html.img(src: "weiche.svg", alt: "", width: 72, height: 72))
    html.span(class: "wire")
    html.span(class: "endpoints")[Endpoints]
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
        html.h3(html.a(href: "https://gateway-api.sigs.k8s.io/reference/api-types/" + lower(name) + "/", name))
        html.p(detail)
      })
    }
  })
})
