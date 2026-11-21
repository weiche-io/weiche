#import "meta.typ": head, site

#let nav-link(href, body, current: false) = html.elem(
  "a",
  attrs: (href: href, ..if current { ("aria-current": "page") } else { (:) }),
  body,
)

#let page(
  title: site.name + " — A Kubernetes gateway, built in Rust",
  description: site.description,
  path: "",
  root: "./",
  body-class: "",
  body,
) = {
  html.html(lang: "en", {
    html.head(head(
      title: title,
      description: description,
      path: path,
      root: root,
    ))
    html.body(class: body-class, {
      html.a(href: "#main", class: "skip-link")[Skip to content]
      html.header(class: "site-header wrap", {
        html.a(href: root, class: "brand", aria-label: "Weiche home", {
          html.img(
            src: root + "weiche.svg#dark",
            width: 48,
            height: 48,
            alt: "",
          )
          [weiche]
        })
        html.nav(aria-label: "Main navigation", {
          nav-link(root + "guide/", [Guide], current: path == "guide/")
          nav-link(site.repository, [GitHub])
        })
      })
      html.main(id: "main", class: "wrap", body)
      html.footer(class: "site-footer wrap", {
        html.p()[#html.strong()[Weiche]]
        if body-class != "home" {
          html.nav(aria-label: "Project links", {
            html.a(href: root + "weiche.svg")[Logo (SVG)]
            html.a(href: root + "guide/#current-scope")[Current scope]
            html.a(href: site.repository + "/issues")[Issues]
          })
        }
      })
    })
  })
}
