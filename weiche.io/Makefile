MAKEFLAGS   += --no-builtin-rules
SHELL       := bash
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:

.PHONY: build full assets css fonts serve clean distclean
.DEFAULT_GOAL := build

include mk/log.mk

MINIFY ?= n
PORT   ?= 8000

MINIFY_CMD := minify --html-keep-quotes --html-keep-end-tags --html-keep-document-tags
BUILD_MODE := $(if $(filter y,$(MINIFY)),minified,plain)
MODE_STAMP := build/.mode-$(BUILD_MODE)

INSTALL := install -D -m 0644
MKDIR_P := mkdir -p
CP_R    := cp -r
RM_RF   := rm -rf
FIND    := find

SRC_PAGES    := $(shell find src -mindepth 2 -type f -name index.typ | sort)
SRC_SECTIONS := $(patsubst src/%/index.typ,%,$(SRC_PAGES))
STAGED_PAGES := $(addprefix build/,$(addsuffix /index.typ,$(SRC_SECTIONS)))
TARGET_PAGES := $(addprefix build/,$(addsuffix /index.html,$(SRC_SECTIONS)))
TRASH_COUNT  = $(words $(shell [ -d build ] && find build \( -name '*.bib' -o -name '*.typ' -o -name '.mode-*' \) 2>/dev/null))

include mk/assets.mk
include mk/typst.mk

.SECONDARY: $(STAGED_PAGES)

build: assets fonts build/index.html $(TARGET_PAGES)

full: build
	$(Q)$(MAKE) --no-print-directory clean

assets: css $(addprefix build/,$(ASSET_STATIC))
css: $(addprefix build/,$(ASSET_CSS))
fonts: $(addprefix build/,$(ASSET_FONTS))

$(MODE_STAMP):
	$(call log,MODE,$(BUILD_MODE))
	$(Q)$(MKDIR_P) $(@D)
	$(Q)rm -f build/.mode-plain build/.mode-minified
	$(Q)touch $@

build/%/index.typ: src/%/index.typ src/%/meta.typ src/%
	$(call log,COPY,$(@D))
	$(Q)$(MKDIR_P) $(@D)
	$(Q)$(CP_R) $(<D)/. $(@D)/
	$(Q)touch $@

serve: build
	$(call log,SERVE,http://[::1]:$(PORT)/)
	$(Q)python3 -m http.server -b ::1 $(PORT) --directory build

clean:
	$(call log,RM,$(TRASH_COUNT) intermediates)
	$(Q)if [ -d build ]; then $(FIND) build \( -name '*.bib' -o -name '*.typ' -o -name '.mode-*' \) -delete; fi

distclean:
	$(call log,RM,build)
	$(Q)$(RM_RF) build
