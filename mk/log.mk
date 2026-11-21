ifneq ($(V),y)
log = @printf '%-6s %s\n' "$(1)" "$(2)"
quiet := --quiet
endif
