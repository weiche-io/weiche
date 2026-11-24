FAMILY ?= IPv4

.PHONY: k8s
k8s: $(KIND)
ifeq ($(IMAGE),)
	docker build -t localhost/weiche:spec .
endif
	cd test/k8s
	KIND="$(abspath $(KIND))" NODE_IMAGE="$(NODE_IMAGE)" FAMILY="$(FAMILY)" \
	    IMAGE="$(or $(IMAGE),localhost/weiche:spec)" go test -v -count=1 -timeout=25m . $(K8S_TEST_FLAGS)
