MAKEFLAGS   += --no-builtin-rules
SHELL       := bash
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:

.PHONY: web
.DEFAULT_GOAL := web

include mk/log.mk

MINIFY ?= n

MINIFY_CMD := minify --html-keep-quotes --html-keep-end-tags --html-keep-document-tags

INSTALL := install -D -m 0644
MKDIR_P := mkdir -p
CP_R    := cp -r

SRC_PAGES    := $(shell find src -mindepth 2 -type f -name index.typ | sort)
SRC_SECTIONS := $(patsubst src/%/index.typ,%,$(SRC_PAGES))
SRC_META     := $(patsubst %,src/%/meta.typ,$(SRC_SECTIONS))
STAGED_PAGES := $(addprefix build/,$(addsuffix /index.typ,$(SRC_SECTIONS)))
TARGET_PAGES := $(addprefix build/,$(addsuffix /index.html,$(SRC_SECTIONS)))

include mk/assets.mk
include mk/typst.mk

.SECONDARY: $(STAGED_PAGES)

web: build/index.html $(TARGET_PAGES) $(addprefix build/,$(ASSET_CSS) $(ASSET_STATIC) $(ASSET_FONTS))

build/nav.typ: $(SRC_META) src
	$(call log,NAV,$@)
	$(Q)$(MKDIR_P) $(@D)
	$(Q){ \
		printf '#let items = (\n'; \
		for meta in $(SRC_META); do \
			section=$${meta#src/}; \
			section=$${section%/meta.typ}; \
			printf '  {\n'; \
			printf '    import "/%s" as m\n' "$$meta"; \
			printf '    let fields = dictionary(m)\n'; \
			printf '    if fields.at("nav", default: true) {\n'; \
			printf '      ((order: fields.at("order", default: 100), path: "%s/", label: fields.at("breadcrumb", default: m.title)),)\n' "$$section"; \
			printf '    } else {\n'; \
			printf '      ()\n'; \
			printf '    }\n'; \
			printf '  },\n'; \
		done; \
		printf ').flatten().sorted(key: it => it.order)\n'; \
	} > "$@.tmp" && mv "$@.tmp" "$@" || { rm -f "$@.tmp"; exit 1; }

build/%/index.typ: src/%/index.typ src/%/meta.typ src/%
	$(call log,COPY,$(@D))
	$(Q)$(MKDIR_P) $(@D)
	$(Q)$(CP_R) $(<D)/. $(@D)/
	$(Q)touch $@
