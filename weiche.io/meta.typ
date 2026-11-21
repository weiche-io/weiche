// Site-wide metadata. Keep deployment URLs and repository links here.
#let site = (
  name: "Weiche",
  url: "https://weiche.io",
  description: "An experimental Kubernetes gateway, built in Rust.",
  repository: "https://github.com/weiche-io/weiche",
  source-ref: "codex",
)

#let head(title: site.name, description: site.description, path: "", root: "./") = {
  html.meta(charset: "utf-8")
  html.meta(name: "viewport", content: "width=device-width, initial-scale=1")
  html.meta(name: "color-scheme", content: "light dark")
  html.title()[#title]
  html.meta(name: "description", content: description)
  html.link(rel: "canonical", href: site.url + "/" + path)
  for (property, value) in (
    "og:type": "website",
    "og:site_name": site.name,
    "og:title": title,
    "og:description": description,
    "og:url": site.url + "/" + path,
  ) {
    html.elem("meta", attrs: (property: property, content: value))
  }
  html.link(rel: "icon", type: "image/svg+xml", href: root + "weiche.svg")
  html.link(rel: "stylesheet", href: root + "common.css")
}
