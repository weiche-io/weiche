#import "meta.typ": head, site
#import "nav.typ": banner, breadcrumbs, contents, footer

#let root-for(path) = if path == "" { "./" } else {
  "../" * path.matches("/").len()
}

#let page(
  title: site.name,
  description: site.description,
  path: "",
  main-class: "",
  body,
) = {
  let root = root-for(path)
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
      footer(root)
    })
  })
}

#let article(meta, body) = page(
  title: meta.title + " — " + site.name,
  description: meta.description,
  path: meta.path,
  main-class: "wrap reader",
  {
    html.header(class: "page-intro", {
      breadcrumbs(root-for(meta.path), meta.breadcrumb)
      html.h1(meta.heading)
      html.p(class: "lede", meta.intro)
    })
    contents()
    html.article(class: "prose", body)
  },
)
