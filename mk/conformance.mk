SPEC              ?= target/conformance
SUITE             := $(SPEC)/gateway-api
KIND_VERSION      := v0.31.0
KIND_SHA256_amd64 := eb244cbafcc157dff60cf68693c14c9a75c4e6e6fedaf9cd71c58117cb93e3fa
KIND_SHA256_arm64 := 8e1014e87c34901cc422a1445866835d1e666f2a61301c27e722bdeab5a1f7e4
KIND              := $(SPEC)/kind-$(KIND_VERSION)/kind
NODE_IMAGE        := docker.io/kindest/node:v1.35.0
PROFILES          := GATEWAY-HTTP,GATEWAY-TCP,GATEWAY-UDP

.PHONY: conformance-source conformance-tools conformance
conformance-source: $(SUITE)/go.work
conformance-tools: $(SPEC)/conformance.test $(KIND)

$(SUITE)/go.work: weiche-gtwapi/manifests/source.json
	$(call log,FETCH,Gateway API)
	mkdir -p "$(@D)"
	git -C "$(@D)" init $(quiet)
	git -C "$(@D)" fetch $(quiet) --depth=1 https://github.com/kubernetes-sigs/gateway-api.git "refs/tags/$$(jq -er .release "$<")"
	git -C "$(@D)" checkout $(quiet) --detach FETCH_HEAD
	touch "$@"

$(SPEC)/conformance.test: weiche-gtwapi/manifests/source.json
	$(if $(filter y,$(V)),,@)$(MAKE) $(if $(filter y,$(V)),,-s) conformance-source
	$(call log,BUILD,$@)
	CGO_ENABLED=0 $(MAKE) $(quiet) -C "$(SUITE)" conformance-bin
	mv "$(SUITE)/conformance.test" "$@"

$(KIND):
	$(call log,FETCH,$@)
	arch=$$(uname -m | sed -e 's/^x86_64$$/amd64/' -e 's/^aarch64$$/arm64/')
	case "$$arch" in
	amd64) sha="$(KIND_SHA256_amd64)" ;;
	arm64) sha="$(KIND_SHA256_arm64)" ;;
	*) printf 'unsupported architecture: %s\n' "$$arch" >&2; exit 1 ;;
	esac
	mkdir -p "$(@D)"
	curl -fsSL --retry 3 -o "$@" "https://github.com/kubernetes-sigs/kind/releases/download/$(KIND_VERSION)/kind-linux-$$arch"
	printf '%s  %s\n' "$$sha" "$@" | sha256sum -c - $(quiet)
	chmod 755 "$@"

conformance: $(SPEC)/conformance.test $(KIND)
	$(call log,IMAGE,$(or $(IMAGE),localhost/weiche:spec))
	image="$(or $(IMAGE),localhost/weiche:spec)"
ifeq ($(IMAGE),)
	docker build -t "$$image" .
endif
	run=$$(mktemp -d "$(abspath $(SPEC))/run.XXXXXXXX")
	cluster=weiche-spec-$${run##*.}
	cluster=$${cluster,,}
	node="$$cluster-control-plane"
	export KUBECONFIG="$$run/kubeconfig"
	trap '"$(KIND)" delete cluster --name "$$cluster"; rm -rf "$$run"' EXIT
	$(call log,KIND,$$cluster)
	"$(KIND)" create cluster --name "$$cluster" --image $(NODE_IMAGE) --wait 2m
	"$(KIND)" load docker-image --name "$$cluster" "$$image"
	$(call log,APPLY,Gateway API CRDs)
	kubectl apply --server-side -f weiche-gtwapi/manifests/standard-install.yaml
	kubectl wait --for=condition=Established crd --all --timeout=2m
	$(call log,APPLY,test/weiche.yaml)
	kubectl create namespace gateway-conformance-infra
	helm install weiche charts/weiche --namespace weiche-spec --create-namespace -f test/weiche.yaml \
	    --set "image.repository=$${image%:*}" --set "image.tag=$${image##*:}"
	kubectl -n weiche-spec rollout status deployment/weiche --timeout=2m
	kubectl wait --for=condition=Accepted gatewayclass/weiche --timeout=2m
	# keep docker cp on the node's root filesystem because kind mounts /tmp separately.
	docker cp "$<" "$$node:/conformance.test"
	status=0
	$(call log,TEST,$(PROFILES))
	docker exec -e KUBECONFIG=/etc/kubernetes/admin.conf "$$node" \
	    /conformance.test -test.v -test.parallel=4 -test.run '^TestConformance$$' -test.timeout=35m \
	    -gateway-class weiche -conformance-profiles $(PROFILES) \
	    -organization weiche-io -project weiche -url https://github.com/weiche-io/weiche \
	    -contact https://github.com/weiche-io/weiche/issues -version "$$(git rev-parse HEAD)" \
	    -mode dedicated -report-output /report.yaml || status=$$?
	if [ "$$status" -ne 0 ] && ! docker exec "$$node" test -f /report.yaml; then
	    exit "$$status"
	fi
	$(call log,REPORT,$(SPEC)/report.yaml)
	docker cp "$$node:/report.yaml" "$$run/report.yaml"
	mv "$$run/report.yaml" "$(SPEC)/report.yaml"
	exit "$$status"
