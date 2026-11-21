#import "meta.typ": head, site
#import "/build/nav.typ": items as nav-items

#let source-path = sys.inputs.at("src", default: none)
#let derived-path = if (
  source-path != none
    and source-path.starts-with("src/")
    and source-path.ends-with("index.typ")
) {
  // Strip the leading "src/" (4 chars) and trailing "index.typ" (9 chars).
  source-path.slice(4, source-path.len() - 9)
} else {
  ""
}

#let root-for(path) = if path == "" { "./" } else {
  "../" * path.matches("/").len()
}

#let banner(root, path) = {
  html.a(href: "#main", class: "skip-link")[Skip to content]
  html.header(class: "site-header wrap", {
    html.a(href: root, class: "brand", aria-label: site.name + " home", {
      html.img(src: root + "weiche.svg", width: 34, height: 34, alt: "")
      site.name
    })
    html.nav(aria-label: "Main navigation", {
      for item in nav-items {
        let current = if path == item.path { (aria-current: "page") } else {
          (:)
        }
        html.a(href: root + item.path, ..current, item.label)
      }
      html.a(href: site.repository)[GitHub]
    })
  })
}

#let footer(root, source-path: none) = html.footer(class: "site-footer wrap", {
  html.a(class: "footer-brand", href: root, site.name)
  html.nav(aria-label: "Project links", {
    let source = if source-path == none {
      site.repository
    } else {
      site.repository + "/blob/" + site.source-ref + "/weiche.io/" + source-path
    }
    html.a(href: source)[Source]
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

#let canvas(it) = context {
  set align(center)
  if target() == "html" { html.frame(it) } else { it }
}

#let html-image(it) = {
  let alt = it.fields().at("alt", default: none)
  html.img(
    src: it.source,
    alt: if alt == none { "" } else { alt },
    loading: "lazy",
    decoding: "async",
  )
}

#let page(
  title: site.name,
  description: site.description,
  path: derived-path,
  main-class: "",
  body,
) = {
  let root = root-for(path)
  show image: html-image
  html.html(lang: "en", {
    html.head(head(
      title: title,
      description: description,
      path: path,
      root: root,
    ))
    html.body({
      banner(root, path)
      html.main(id: "main", class: main-class, body)
      footer(root, source-path: source-path)
    })
  })
}

#let article(meta, body) = {
  let path = if derived-path != "" {
    derived-path
  } else {
    dictionary(meta).at("path", default: "")
  }
  page(
    title: meta.title + " — " + site.name,
    description: meta.description,
    path: path,
    main-class: "wrap reader",
    {
      html.header(class: "page-intro", {
        breadcrumbs(root-for(path), meta.breadcrumb)
        html.h1(meta.heading)
        html.p(class: "lede", meta.intro)
      })
      contents()
      html.article(class: "prose", body)
    },
  )
}
