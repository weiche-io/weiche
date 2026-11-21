SITE     := weiche.io
SITE_OUT := $(SITE)/build
TYPST    ?= typst
PORT     ?= 8000
MINIFY   ?= n

MINIFY_HTML := minify --html-keep-quotes --html-keep-end-tags --html-keep-document-tags
HTML_MODE := $(if $(filter y,$(MINIFY)),minified,plain)
HTML_STAMP := $(SITE_OUT)/.html-$(HTML_MODE)

PAGES  := $(SITE_OUT)/index.html $(patsubst $(SITE)/src/%.typ,$(SITE_OUT)/%.html,$(shell find $(SITE)/src -name index.typ -type f))
ASSETS := $(patsubst $(SITE)/assets/%,$(SITE_OUT)/%,$(shell find $(SITE)/assets -type f))
# Each article can keep its metadata beside index.typ.
ARTICLE_META := $(shell find $(SITE)/src -type f -name meta.typ)
META_PAGES := $(patsubst $(SITE)/src/%/meta.typ,$(SITE_OUT)/%/index.html,$(ARTICLE_META))

vpath %.typ $(SITE) $(SITE)/src

.PHONY: weiche.io docs build serve clean
weiche.io: $(PAGES) $(ASSETS)
docs build: weiche.io

$(META_PAGES): $(SITE_OUT)/%/index.html: $(SITE)/src/%/meta.typ

ifeq ($(V),y)
site_run = $(1)
else
site_run = diagnostics=$$($(1) 2>&1) || { printf '%s\n' "$$diagnostics" >&2; exit 1; }
endif

# Switching modes invalidates the HTML, even when no Typst source changed.
$(HTML_STAMP):
	$(call log,HTML,$(HTML_MODE))
	mkdir -p "$(@D)"
	rm -f "$(@D)/.html-plain" "$(@D)/.html-minified"
	touch "$@"

$(SITE_OUT)/%.html: %.typ $(addprefix $(SITE)/,page.typ meta.typ nav.typ) mk/site.mk $(HTML_STAMP)
	$(call log,TYP,$<)
	mkdir -p "$(@D)"
	$(call site_run,$(TYPST) compile --root "$(SITE)" --features html "$<" "$@")
ifeq ($(HTML_MODE),minified)
	$(call log,MINI,$@)
	$(call site_run,$(MINIFY_HTML) -o "$@" "$@")
endif

$(SITE_OUT)/%: $(SITE)/assets/%
	$(call log,ASSET,$@)
	install -D -m 0644 "$<" "$@"

serve: weiche.io
	$(call log,SERVE,http://[::1]:$(PORT)/)
	python3 -m http.server -b ::1 $(PORT) --directory "$(SITE_OUT)"

clean:
	$(call log,RM,$(SITE_OUT))
	$(RM) -r "$(SITE_OUT)"
