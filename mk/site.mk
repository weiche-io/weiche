SITE_MAKE := $(MAKE) $(if $(filter y,$(V)),,--no-print-directory) -C weiche.io

.PHONY: weiche.io docs build full serve clean distclean
weiche.io:
	$(if $(filter y,$(V)),,@)$(SITE_MAKE) build

docs build: weiche.io

full:
	$(if $(filter y,$(V)),,@)$(SITE_MAKE) full

serve:
	$(if $(filter y,$(V)),,@)$(SITE_MAKE) serve

clean:
	$(if $(filter y,$(V)),,@)$(SITE_MAKE) distclean

distclean: clean
