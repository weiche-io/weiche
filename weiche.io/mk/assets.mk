ASSET_CSS    := $(notdir $(wildcard assets/*.css))
ASSET_STATIC := $(patsubst assets/%,%,$(shell find assets -path assets/fonts -prune -o -type f ! -name '*.css' -print | sort))

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

FONT_ARCHIVES := \
	assets/fonts/ibm-plex-sans.tgz \
	assets/fonts/chakra-petch.tgz

.PRECIOUS: $(FONT_ARCHIVES) $(addprefix assets/,$(ASSET_FONTS))

build/%.css: assets/%.css $(MODE_STAMP)
	$(call log,ASSET,$@)
	$(Q)$(INSTALL) $< $@
ifeq ($(MINIFY),y)
	$(call log,MINI,$@)
	$(Q)$(call run,$(MINIFY_CMD) -o $@ $@)
endif

build/fonts/%: assets/fonts/%
	$(call log,ASSET,$@)
	$(Q)$(INSTALL) $< $@

build/%: assets/%
	$(call log,ASSET,$@)
	$(Q)$(INSTALL) $< $@

assets/fonts/ibm-plex-sans.tgz: FONT_URL := $(PLEX_SANS_TARBALL)
assets/fonts/chakra-petch.tgz:  FONT_URL := $(CHAKRA_PETCH_TARBALL)

assets/fonts/%.tgz:
	$(call log,FETCH,$@)
	$(Q)$(MKDIR_P) $(@D)
	$(Q)rm -f "$@.tmp"
	$(Q)$(CURL) -fsSL "$(FONT_URL)" > "$@.tmp" && mv "$@.tmp" "$@" || { rm -f "$@.tmp"; exit 1; }

assets/fonts/plex-sans.woff2:      assets/fonts/ibm-plex-sans.tgz
assets/fonts/plex-sans.woff2:      FONT_MEMBER := package/files/ibm-plex-sans-latin-wght-normal.woff2
assets/fonts/OFL-plex-sans.txt:    assets/fonts/ibm-plex-sans.tgz
assets/fonts/OFL-plex-sans.txt:    FONT_MEMBER := package/LICENSE
assets/fonts/chakra-petch.woff2:   assets/fonts/chakra-petch.tgz
assets/fonts/chakra-petch.woff2:   FONT_MEMBER := package/files/chakra-petch-latin-700-normal.woff2
assets/fonts/OFL-chakra-petch.txt: assets/fonts/chakra-petch.tgz
assets/fonts/OFL-chakra-petch.txt: FONT_MEMBER := package/LICENSE

$(addprefix assets/,$(ASSET_FONTS)):
	$(call log,FONT,$@)
	$(Q)rm -f "$@.tmp"
	$(Q)tar -xzOf "$<" "$(FONT_MEMBER)" > "$@.tmp" && mv "$@.tmp" "$@" || { rm -f "$@.tmp"; exit 1; }
