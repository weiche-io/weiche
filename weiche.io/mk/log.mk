ifeq ($(V),y)
	Q :=
	run = $(1)
else
	Q := @
	run = diagnostics=$$($(1) 2>&1) || { printf '%s\n' "$$diagnostics" >&2; exit 1; }
endif

define log
	@printf '%-6s %s\n' "$(1)" "$(2)"
endef
