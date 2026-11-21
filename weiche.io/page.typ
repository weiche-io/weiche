#import "meta.typ": head, site
#import "nav.typ": banner, breadcrumbs, contents, footer

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
