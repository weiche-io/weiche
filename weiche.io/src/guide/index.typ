#import "/page.typ": page
#import "/meta.typ": site
#show: page.with(
  title: "Get started — Weiche",
  description: "Build Weiche and run the Gateway API conformance suite on kind.",
  path: "guide/",
  root: "../",
  body-class: "guide",
)

#html.header(class: "page-intro", {
  html.h1()[Test Weiche on kind]
  html.p(class: "lede")[Run the same Gateway API conformance tests used on pull requests.]
})

#html.aside(class: "toc", {
  html.nav(aria-label: "On this page", {
    html.a(href: "#prerequisites")[Prerequisites]
    html.a(href: "#run")[Run the suite]
    html.a(href: "#report")[Read the report]
    html.a(href: "#current-scope")[Current scope]
  })
})

#html.article(class: "prose", {
  html.section(aria-labelledby: "prerequisites", {
    html.h2(id: "prerequisites")[Prerequisites]
    [Use a Linux host with Docker running, GNU Make, Bash, Git, Go, jq and kubectl.
      The build downloads kind and the Go toolchain selected by the upstream suite.]
  })
  html.section(aria-labelledby: "run", {
    html.h2(id: "run")[Run the suite]
    raw(
      "git clone --branch " + site.source-ref + " " + site.repository + ".git\n"
        + "cd weiche\nmake conformance",
      lang: "sh",
      block: true,
    )
    [Make builds the Weiche image and the test runner, creates a temporary kind
      cluster, and installs the bundled Gateway API definitions and the
      #html.a(href: site.repository + "/blob/" + site.source-ref + "/test/weiche.yaml")[Weiche controller].
      The upstream suite creates its own Gateways, routes and backends.
      The runner executes inside the kind node to reach their cluster IPs.]
  })
  html.section(aria-labelledby: "report", {
    html.h2(id: "report")[Read the report]
    [The suite prints each test result. Make copies its YAML report to
      #raw("target/conformance/report.yaml") and removes the temporary cluster.
      On a pull request, the same report is available as the
      #raw("gateway-api-conformance") workflow artifact.]
  })
  html.section(aria-labelledby: "current-scope", {
    html.h2(id: "current-scope")[Current scope]
    [Weiche runs the #raw("GATEWAY-HTTP"), #raw("GATEWAY-TCP") and
      #raw("GATEWAY-UDP") Core profiles. The suite version follows the
      #html.a(href: site.repository + "/blob/" + site.source-ref + "/weiche-gtwapi/manifests/source.json")[bundled CRDs].
      The #html.a(href: site.repository + "/tree/" + site.source-ref + "/weiche/src/control")[control plane]
      watches Gateway API resources, Services and EndpointSlices and compiles
      them into routes for the data plane.]
  })
})
