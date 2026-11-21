# website design and engineering principles

## html and css

- no javascript (hopefully).
- no `<div>` tag; prioritize semantic html elements (`header`, `nav`, `main`,
  `article`, `section`, `aside`, `footer`).
- centralize styling in `weiche.io/assets/common.css` using css custom
  properties, `prefers-color-scheme`, and `@media print`; avoid inline styles.
- keep accessibility intact: skip link, focus-visible outlines, `aria-current`,
  and meaningful `alt` text on images.

## fonts and binary assets

- never commit font files or large third-party binaries into git; keep
  `weiche.io/assets/fonts/` ignored in `.gitignore`.
- fetch pinned upstream font archives (such as `@fontsource` tarballs) on demand
  in `weiche.io/mk/assets.mk` with atomic `.tmp` -> `mv` writes, and ship their
  license files (`OFL-*.txt`) beside the served `.woff2` files in `build/fonts/`.
- prefer pre-subsetted `.woff2` fonts; if large or CJK fonts are ever added,
  subset them at build time (e.g. via `pyftsubset`) rather than serving full
  fonts.

## build layout and makefiles

- keep the website self-contained under `weiche.io/` with its own `Makefile` and
  modular `mk/{log,assets,typst}.mk`; keep root `mk/site.mk` as a thin
  forwarding layer.
- do not create shell script directories (`bin/` or `scripts/`); keep build steps
  declarative in GNU Make and compatible with GNU Make 4.3 on CI (avoid
  4.4-only syntax such as `.WAIT`).
- stage each `src/<page>/` directory into `build/<page>/` before compiling so
  sibling assets (images, svgs, data) resolve identically in Typst and HTML,
  then strip intermediate `*.typ`, `*.bib`, and `.mode-*` files in `clean` /
  `full` so `build/` only holds deployable output.
- support `V=y` for verbose recipes and `MINIFY=y` for production minification,
  tracking the output mode so switching `MINIFY` invalidates stale html/css.

## typst templates and pages

- keep site-wide metadata in `meta.typ`, shared chrome and `context query` TOC
  in `nav.typ`, and the shared html shell plus helpers (`page`, `article`,
  `canvas`, lazy `html-image`) in `page.typ`.
- pass `--input src=...` from `mk/typst.mk` so `page.typ` derives page URL
  paths, relative root links (`root-for`), canonical/og URLs, and GitHub source
  links automatically instead of repeating `path` in every page's `meta.typ`.
- place each subpage under `src/<section>/{index.typ, meta.typ}` alongside any
  page-local media, and use Typst `#image(...)` so `html-image` emits lazy
  `<img>` tags.
