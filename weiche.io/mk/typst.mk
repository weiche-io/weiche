TYPST     ?= typst
TYPST_CMD := $(TYPST) c --root . --features html

SITE_TMPLS := page.typ meta.typ build/nav.typ

build/index.html: src/index.typ $(SITE_TMPLS) mk/typst.mk $(MODE_STAMP)
	$(call log,TYP,$<)
	$(Q)$(MKDIR_P) $(@D)
	$(Q)$(call run,$(TYPST_CMD) --input src=$< $< $@)
ifeq ($(MINIFY),y)
	$(call log,MINI,$@)
	$(Q)$(call run,$(MINIFY_CMD) -o $@ $@)
endif

build/%/index.html: build/%/index.typ $(SITE_TMPLS) mk/typst.mk $(MODE_STAMP)
	$(call log,TYP,$<)
	$(Q)$(call run,$(TYPST_CMD) --input src=src/$*/index.typ $< $@)
ifeq ($(MINIFY),y)
	$(call log,MINI,$@)
	$(Q)$(call run,$(MINIFY_CMD) -o $@ $@)
endif
