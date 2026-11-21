SITE := weiche.io
SITE_OUT := $(SITE)/build
TYPST ?= typst
PORT ?= 8000

PAGES := $(SITE_OUT)/index.html $(patsubst $(SITE)/src/%.typ,$(SITE_OUT)/%.html,$(shell find $(SITE)/src -name index.typ -type f))
ASSETS := $(patsubst $(SITE)/assets/%,$(SITE_OUT)/%,$(shell find $(SITE)/assets -type f))
vpath %.typ $(SITE) $(SITE)/src

.PHONY: weiche.io docs build serve clean
weiche.io: $(PAGES) $(ASSETS)
docs build: weiche.io

ifeq ($(V),y)
run_typst = $(1)
else
site_log = @printf '%-6s %s\n' "$(1)" "$(2)"
run_typst = diagnostics=$$($(1) 2>&1) || { printf '%s\n' "$$diagnostics" >&2; exit 1; }
endif

$(SITE_OUT)/%.html: %.typ $(SITE)/page.typ $(SITE)/meta.typ mk/site.mk
	$(call site_log,TYP,$<)
	mkdir -p "$(@D)"
	$(call run_typst,$(TYPST) compile --root "$(SITE)" --features html "$<" "$@")

$(SITE_OUT)/%: $(SITE)/assets/%
	$(call site_log,ASSET,$@)
	install -D -m 0644 "$<" "$@"

serve: weiche.io
	$(call site_log,SERVE,http://[::1]:$(PORT)/)
	python3 -m http.server -b ::1 $(PORT) --directory "$(SITE_OUT)"

clean:
	$(call site_log,RM,$(SITE_OUT))
	$(RM) -r "$(SITE_OUT)"
