package k8s

import (
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func apply(t *testing.T, file string, values map[string]string) {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), "kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(os.Expand(string(data), func(key string) string { return values[key] }))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("apply %s: %v\n%s", file, err, out)
	}
}

func cleanup(t *testing.T, name string, args ...string) {
	t.Helper()
	t.Cleanup(func() {
		if out, err := exec.Command(name, args...).CombinedOutput(); err != nil {
			t.Errorf("cleanup %s %v: %v\n%s", name, args, err, out)
		}
	})
}

func jsonpath(t *testing.T, resource, path string) string {
	t.Helper()
	return string(command(t, "kubectl", "get", resource, "-o=jsonpath="+path))
}

func TestKubernetes(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("KUBECONFIG", filepath.Join(dir, "kubeconfig"))
	cluster := fmt.Sprintf("weiche-k8s-%d", os.Getpid())
	kind := os.Getenv("KIND")
	cleanup(t, kind, "delete", "cluster", "--name", cluster)
	command(t, kind, "create", "cluster", "--name", cluster, "--image", os.Getenv("NODE_IMAGE"), "--config", "kind.yaml")
	command(t, "helm", "install", "cilium", "https://helm.cilium.io/cilium-1.19.3.tgz", "-n", "kube-system", "-f", "cilium.yaml", "--wait", "--timeout=5m")
	command(t, "kubectl", "wait", "--for=condition=Ready", "node", "--all", "--timeout=2m")

	family := os.Getenv("FAMILY")
	cidr, field := "198.51.100.240/31", "IPAddress"
	switch family {
	case "IPv4":
	case "IPv6":
		cidr, field = "2001:db8:100::10/127", "GlobalIPv6Address"
	default:
		t.Fatalf("unknown family %q", family)
	}
	first := netip.MustParsePrefix(cidr).Addr()
	apply(t, "pool.yaml.in", map[string]string{"pool": cidr})
	node := strings.TrimSpace(string(command(t, "docker", "inspect", "-f", "{{(index .NetworkSettings.Networks \"kind\")."+field+"}}", cluster+"-control-plane")))
	command(t, "sudo", "ip", "route", "add", cidr, "via", node)
	cleanup(t, "sudo", "ip", "route", "del", cidr, "via", node)

	image := os.Getenv("IMAGE")
	command(t, kind, "load", "docker-image", "--name", cluster, image)
	command(t, "kubectl", "apply", "--server-side", "-f", "../../weiche-gtwapi/manifests/standard-install.yaml")
	command(t, "kubectl", "wait", "--for=condition=Established", "crd", "--all", "--timeout=2m")
	repository, tag := image, "latest"
	if colon := strings.LastIndex(image, ":"); colon > strings.LastIndex(image, "/") {
		repository, tag = image[:colon], image[colon+1:]
	}
	command(t, "helm", "install", "weiche", "../../charts/weiche", "-f", "../weiche.yaml", "-f", "weiche.yaml",
		"--set-string", "image.repository="+repository, "--set-string", "image.tag="+tag,
		"--set", "proxy.service.ipFamilies[0]="+family, "--wait", "--timeout=3m")
	command(t, "kubectl", "wait", "--for=condition=Accepted", "gatewayclass/weiche", "--timeout=2m")
	command(t, "kubectl", "apply", "-f", "echo.yaml")
	command(t, "kubectl", "rollout", "status", "deployment/echo", "--timeout=2m")

	// Claim the fixed VIP first, leaving exactly one IP for Cilium to assign.
	fixed := endpoint{first.Next().String(), "fixed.example.test"}
	automatic := endpoint{first.String(), "automatic.example.test"}
	for _, e := range []endpoint{fixed, automatic} {
		name, _, _ := strings.Cut(e.host, ".")
		addresses, request := "[]", ""
		if e == fixed {
			addresses, request = "[{type: IPAddress, value: '"+e.ip+"'}]", e.ip
		}
		apply(t, "gateway.yaml.in", map[string]string{"gateway": name, "addresses": addresses})
		command(t, "kubectl", "wait", "--for=condition=Programmed", "gateway/"+name, "--timeout=3m")
		svc := strings.TrimSpace(string(command(t, "kubectl", "get", "svc", "-l", "gateway.networking.k8s.io/gateway-name="+name, "-o=name")))
		got := jsonpath(t, svc, "{.spec.type}/{.spec.loadBalancerIP}/{.status.loadBalancer.ingress[0].ip}")
		if got != "LoadBalancer/"+request+"/"+e.ip || jsonpath(t, "gateway/"+name, "{.status.addresses[*].type}/{.status.addresses[*].value}") != "IPAddress/"+e.ip {
			t.Fatalf("expected %s VIP %s, service reported %s", name, e.ip, got)
		}
	}
	if t.Run("ControllerScaling", func(t *testing.T) { controllerScaling(t, fixed, automatic) }) {
		t.Run("ProxyPerformance", func(t *testing.T) { compareProxies(t, dir, family, fixed.ip, automatic.ip) })
	}
}
