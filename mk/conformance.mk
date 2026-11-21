SPEC         := target/conformance
SUITE        := $(SPEC)/gateway-api
KIND_VERSION := v0.31.0
KIND         := $(SPEC)/kind-$(KIND_VERSION)/kind
NODE_IMAGE   := docker.io/kindest/node:v1.35.0
PROFILES     := GATEWAY-HTTP,GATEWAY-TCP,GATEWAY-UDP

.PHONY: conformance-source conformance
conformance-source: $(SUITE)/go.work

$(SUITE)/go.work: weiche-gtwapi/manifests/source.json
	$(call log,FETCH,Gateway API)
	mkdir -p "$(@D)"
	git -C "$(@D)" init $(quiet)
	git -C "$(@D)" fetch $(quiet) --depth=1 https://github.com/kubernetes-sigs/gateway-api.git "refs/tags/$$(jq -er .release "$<")"
	git -C "$(@D)" checkout $(quiet) --detach FETCH_HEAD
	touch "$@"

$(SPEC)/conformance.test: $(SUITE)/go.work
	$(call log,BUILD,$@)
	CGO_ENABLED=0 $(MAKE) $(quiet) -C "$(SUITE)" conformance-bin
	mv "$(SUITE)/conformance.test" "$@"

$(KIND):
	$(call log,BUILD,$@)
	mkdir -p "$(@D)"
	GOBIN="$(abspath $(@D))" go install sigs.k8s.io/kind@$(KIND_VERSION)

conformance: $(SPEC)/conformance.test $(KIND)
	$(call log,IMAGE,localhost/weiche:spec)
	docker build -t localhost/weiche:spec .
	run=$$(mktemp -d "$(abspath $(SPEC))/run.XXXXXXXX")
	cluster=weiche-spec-$${run##*.}
	cluster=$${cluster,,}
	node="$$cluster-control-plane"
	export KUBECONFIG="$$run/kubeconfig"
	trap '"$(KIND)" delete cluster --name "$$cluster"; rm -rf "$$run"' EXIT
	$(call log,KIND,$$cluster)
	"$(KIND)" create cluster --name "$$cluster" --image $(NODE_IMAGE) --wait 2m
	"$(KIND)" load docker-image --name "$$cluster" localhost/weiche:spec
	$(call log,APPLY,Gateway API CRDs)
	kubectl apply --server-side -f weiche-gtwapi/manifests/standard-install.yaml
	kubectl wait --for=condition=Established crd --all --timeout=2m
	$(call log,APPLY,test/weiche.yaml)
	kubectl create namespace gateway-conformance-infra
	helm install weiche charts/weiche --namespace weiche-spec --create-namespace -f test/weiche.yaml
	kubectl -n weiche-spec rollout status deployment/weiche --timeout=2m
	kubectl wait --for=condition=Accepted gatewayclass/weiche --timeout=2m
	docker cp "$<" "$$node:/tmp/conformance.test"
	status=0
	$(call log,TEST,$(PROFILES))
	docker exec -e KUBECONFIG=/etc/kubernetes/admin.conf "$$node" \
	    /tmp/conformance.test -test.v -test.parallel=4 -test.run '^TestConformance$$' -test.timeout=35m \
	    -gateway-class weiche -conformance-profiles $(PROFILES) \
	    -organization weiche-io -project weiche -url https://github.com/weiche-io/weiche \
	    -contact https://github.com/weiche-io/weiche/issues -version "$$(git rev-parse HEAD)" \
	    -mode dedicated -report-output /tmp/report.yaml || status=$$?
	$(call log,REPORT,$(SPEC)/report.yaml)
	docker cp "$$node:/tmp/report.yaml" "$$run/report.yaml"
	mv "$$run/report.yaml" "$(SPEC)/report.yaml"
	exit "$$status"
