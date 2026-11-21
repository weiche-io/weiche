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

$(SITE_OUT)/%.html: %.typ $(SITE)/page.typ $(SITE)/meta.typ mk/site.mk
	@mkdir -p "$(@D)"
	$(TYPST) compile --root $(SITE) --features html "$<" "$@"

$(SITE_OUT)/%: $(SITE)/assets/%
	install -D -m 0644 "$<" "$@"

serve: weiche.io
	python3 -m http.server -b ::1 $(PORT) --directory $(SITE_OUT)

clean:
	$(RM) -r $(SITE_OUT)
