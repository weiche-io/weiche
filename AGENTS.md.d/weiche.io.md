# website design and engineering principles

## html and css

- no javascript (hopefully).
- no `<div>` tag; prioritize semantic html elements (`header`, `nav`, `main`,
  `article`, `section`, `aside`, `footer`).
- centralize styling in `weiche.io/assets/common.css` for the common part.
- always attribute adapted third-party styles, palettes, or assets with their
  upstream repository, commit/version, and license header in the file.

## build layout and makefiles

- keep the website self-contained under `weiche.io/` with its own `Makefile` and
  modular `mk/{log,assets,typst}.mk`; keep root `mk/site.mk` as a thin
  forwarding layer.
- do not create shell script directories (`bin/` or `scripts/`); keep build steps declarative in GNU Make.

## typst templates and pages

- keep only shared templates at `weiche.io/` root (`meta.typ` for site-wide
  metadata, and `page.typ` for the html shell, header/footer chrome, TOC, and
  helpers `page`, `article`, `canvas`, lazy `html-image`).
- put all page content under `weiche.io/src/` — `src/index.typ` for the landing
  page and `src/<section>/{index.typ, meta.typ}` alongside page-local media for
  subpages — importing templates via root paths (`"/page.typ"`, `"/meta.typ"`)
  and using Typst `#image(...)` so `html-image` emits lazy `<img>` tags.
