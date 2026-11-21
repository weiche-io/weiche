#import "/page.typ": article
#import "/meta.typ": site
#import "meta.typ"

#show: article.with(meta)

= Prerequisites <prerequisites>

Use a Linux host with Docker running, GNU Make, Bash, Git, Go, jq and kubectl.
The build downloads kind and the Go toolchain selected by the upstream suite.

= Run the suite <run>

#let clone = "git clone --branch " + site.source-ref + " " + site.repository + ".git"
#raw(
  (clone, "cd weiche", "make conformance").join("\n"),
  lang: "sh",
  block: true,
)

#let blob = site.repository + "/blob/" + site.source-ref + "/"
#let tree = site.repository + "/tree/" + site.source-ref + "/"

Make builds the Weiche image and the test runner, creates a temporary kind
cluster, and installs the bundled Gateway API definitions and the
#link(blob + "test/weiche.yaml")[Weiche controller].
The upstream suite creates its own Gateways, routes and backends.
The runner executes inside the kind node to reach their cluster IPs.

= Read the report <report>

The suite prints each test result. Make copies its YAML report to
`target/conformance/report.yaml` and removes the temporary cluster.
On a pull request, the same report is available as the
`gateway-api-conformance` workflow artifact.

= Current scope <current-scope>

Weiche runs the `GATEWAY-HTTP`, `GATEWAY-TCP` and `GATEWAY-UDP` Core profiles.
The suite version follows the
#link(blob + "weiche-gtwapi/manifests/source.json")[bundled CRDs].
The #link(tree + "weiche/src/control")[control plane] watches Gateway API
resources, Services and EndpointSlices and compiles them into routes for the
data plane.
