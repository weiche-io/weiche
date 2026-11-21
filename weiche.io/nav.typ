#import "meta.typ": site

#let banner(root, path) = {
  html.a(href: "#main", class: "skip-link")[Skip to content]
  html.header(class: "site-header wrap", {
    html.a(href: root, class: "brand", aria-label: site.name + " home", {
      html.img(src: root + "weiche.svg", width: 34, height: 34, alt: "")
      site.name
    })
    html.nav(aria-label: "Main navigation", {
      let current = if path == "guide/" { (aria-current: "page") } else { (:) }
      html.a(href: root + "guide/", ..current)[Guide]
      html.a(href: site.repository)[GitHub]
    })
  })
}

#let footer(root) = html.footer(class: "site-footer wrap", {
  html.a(class: "footer-brand", href: root, site.name)
  html.nav(aria-label: "Project links", {
    html.a(href: root + "weiche.svg")[Project logo]
    html.a(href: site.repository + "/issues")[Issues]
  })
})

#let breadcrumbs(root, current) = html.nav(
  class: "crumbs",
  aria-label: "Breadcrumb",
  {
    html.a(href: root, site.name)
    html.span(current)
  },
)

#let contents() = context {
  let sections = query(heading.where(level: 1))
  if sections.len() > 0 {
    html.aside(class: "toc", {
      html.p(id: "contents")[On this page]
      html.nav(aria-labelledby: "contents", {
        for section in sections { link(section.location(), section.body) }
      })
    })
  }
}
