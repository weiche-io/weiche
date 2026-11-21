ASSET_CSS    := $(notdir $(wildcard assets/*.css))
ASSET_STATIC := $(patsubst assets/%,%,$(shell find assets -type f ! -name '*.css' | sort))

ASSET_FONT_FILES := \
	fonts/plex-sans.woff2 \
	fonts/chakra-petch.woff2
ASSET_FONT_LICENSES := \
	fonts/OFL-plex-sans.txt \
	fonts/OFL-chakra-petch.txt
ASSET_FONTS := $(ASSET_FONT_FILES) $(ASSET_FONT_LICENSES)

CURL ?= curl

PLEX_SANS_TARBALL    := https://registry.npmjs.org/@fontsource-variable/ibm-plex-sans/-/ibm-plex-sans-5.3.0.tgz
CHAKRA_PETCH_TARBALL := https://registry.npmjs.org/@fontsource/chakra-petch/-/chakra-petch-5.3.0.tgz

build/%.css: assets/%.css
	$(call log,ASSET,$@)
	$(Q)$(INSTALL) $< $@
ifeq ($(MINIFY),y)
	$(call log,MINI,$@)
	$(Q)$(call run,$(MINIFY_CMD) -o $@ $@)
endif

build/%: assets/%
	$(call log,ASSET,$@)
	$(Q)$(INSTALL) $< $@

build/fonts/plex-sans.woff2:      FONT_TARBALL := $(PLEX_SANS_TARBALL)
build/fonts/plex-sans.woff2:      FONT_MEMBER  := package/files/ibm-plex-sans-latin-wght-normal.woff2
build/fonts/OFL-plex-sans.txt:    FONT_TARBALL := $(PLEX_SANS_TARBALL)
build/fonts/OFL-plex-sans.txt:    FONT_MEMBER  := package/LICENSE
build/fonts/chakra-petch.woff2:   FONT_TARBALL := $(CHAKRA_PETCH_TARBALL)
build/fonts/chakra-petch.woff2:   FONT_MEMBER  := package/files/chakra-petch-latin-700-normal.woff2
build/fonts/OFL-chakra-petch.txt: FONT_TARBALL := $(CHAKRA_PETCH_TARBALL)
build/fonts/OFL-chakra-petch.txt: FONT_MEMBER  := package/LICENSE

$(addprefix build/,$(ASSET_FONTS)):
	$(call log,FONT,$@)
	$(Q)$(MKDIR_P) $(@D)
	$(Q)rm -f "$@.tmp"
	$(Q)$(CURL) -fsSL "$(FONT_TARBALL)" | tar -xzO "$(FONT_MEMBER)" > "$@.tmp" && mv "$@.tmp" "$@" || { rm -f "$@.tmp"; exit 1; }
