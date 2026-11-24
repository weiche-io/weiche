export FAMILY ?= IPv4

.PHONY: k8s
k8s: $(KIND)
	cd test/k8s
	case "$$FAMILY" in
	IPv4) pool=198.51.100.240/31; FIRST_IP=198.51.100.240; LAST_IP=198.51.100.241; node_field=IPAddress ;;
	IPv6) pool=2001:db8:100::10/127; FIRST_IP=2001:db8:100::10; LAST_IP=2001:db8:100::11; node_field=GlobalIPv6Address ;;
	*) echo "unsupported IP family: $$FAMILY" >&2; exit 1 ;;
	esac
	export pool FIRST_IP LAST_IP
	image="$(or $(IMAGE),localhost/weiche:spec)"
ifeq ($(IMAGE),)
	docker build -t "$$image" ../..
endif
	run=$$(mktemp -d)
	cluster=weiche-k8s-$${run##*.}
	cluster=$${cluster,,}
	export KUBECONFIG="$$run/kubeconfig"
	kind="$(abspath $(KIND))"
	routed=false
	cleanup() {
	    local status=$$?
	    if [[ $$status != 0 && -s $$KUBECONFIG ]]; then
	        kubectl get nodes,pods,svc,deploy -A -o wide || true
	        kubectl get lease/weiche-controller -o yaml || true
	        kubectl logs -l app=weiche,app.kubernetes.io/instance=weiche --prefix --tail=80 || true
	        kubectl get gateway,httproute,ciliumloadbalancerippool -A -o yaml || true
	        kubectl get events -A --sort-by=.lastTimestamp | tail -60 || true
	    fi
	    if $$routed; then sudo ip route del "$$pool" via "$$node" || status=1; fi
	    "$$kind" delete cluster --name "$$cluster" || status=1
	    rm -r -- "$$run" || status=1
	    exit "$$status"
	}
	trap cleanup EXIT
	"$$kind" create cluster --name "$$cluster" --image "$(NODE_IMAGE)" --config kind.yaml
	helm install cilium https://helm.cilium.io/cilium-1.19.3.tgz -n kube-system -f cilium.yaml --wait --timeout 5m
	kubectl wait --for=condition=Ready node --all --timeout=2m
	envsubst < pool.yaml.in | kubectl apply -f -
	# route the pool from the test host through the kind node.
	node=$$(docker inspect "$$cluster-control-plane" | jq -er --arg field "$$node_field" '.[0].NetworkSettings.Networks.kind[$$field] | select(length > 0)')
	sudo ip route add "$$pool" via "$$node"
	routed=true
	"$$kind" load docker-image --name "$$cluster" "$$image"
	kubectl apply --server-side -f ../../weiche-gtwapi/manifests/standard-install.yaml
	kubectl wait --for=condition=Established crd --all --timeout=2m
	helm install weiche ../../charts/weiche -f ../weiche.yaml -f weiche.yaml \
	    --set "image.repository=$${image%:*}" --set "image.tag=$${image##*:}" \
	    --set "proxy.service.ipFamilies[0]=$$FAMILY" --wait --timeout 3m
	kubectl wait --for=condition=Accepted gatewayclass/weiche --timeout=2m
	kubectl apply -f echo.yaml
	kubectl rollout status deployment/echo --timeout=2m
	# claim the fixed address first so the automatic allocation has only one choice.
	for gateway in fixed automatic; do
	    ip=$$FIRST_IP
	    addresses='[]'
	    if [[ $$gateway == fixed ]]; then
	        ip=$$LAST_IP
	        addresses="[{type: IPAddress, value: '$$ip'}]"
	    fi
	    gateway="$$gateway" addresses="$$addresses" envsubst < gateway.yaml.in | kubectl apply -f -
	    kubectl wait --for=condition=Programmed "gateway/$$gateway" --timeout=3m
	    service=$$(kubectl get svc -l "gateway.networking.k8s.io/gateway-name=$$gateway" -o name)
	    kubectl get "$$service" -o json | jq -e --arg ip "$$ip" --arg mode "$$gateway" '
	        .spec.type == "LoadBalancer" and
	        .spec.loadBalancerIP == (if $$mode == "fixed" then $$ip else null end) and
	        .status.loadBalancer.ingress[0].ip == $$ip'
	    kubectl get "gateway/$$gateway" -o json | jq -e --arg ip "$$ip" '.status.addresses == [{type:"IPAddress",value:$$ip}]'
	done
	go test -v -count=1 -timeout=12m controller_test.go
