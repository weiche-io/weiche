MAKEFLAGS += --no-builtin-rules
SHELL     := bash
.SHELLFLAGS := -eu -o pipefail -c
.ONESHELL:
.DELETE_ON_ERROR:
.DEFAULT_GOAL := web

include mk/log.mk
include mk/site.mk
include mk/conformance.mk
