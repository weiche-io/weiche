package k8s

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
)

const benchHost = "bench.example.test"
const benchPayload = "weiche-traefik-echo"

// Both frontends terminate the same TLS certificate and forward HTTP/1.1 to
// the same ready Pod IP. The client reaches both via a routed LoadBalancer VIP.
func compareProxies(t *testing.T, dir, family, weicheIP, traefikIP string) {
	t.Helper()
	k := func(args ...string) string { return strings.TrimSpace(string(command(t, "kubectl", args...))) }
	cert, key := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	command(t, "openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-days", "1",
		"-subj", "/CN="+benchHost, "-addext", "subjectAltName=DNS:"+benchHost, "-keyout", key, "-out", cert)
	k("create", "secret", "tls", "bench-tls", "--cert="+cert, "--key="+key)
	pem, err := os.ReadFile(cert)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		t.Fatal("cannot load benchmark certificate")
	}

	// Release the automatic allocation before assigning the second VIP to Traefik.
	svc := k("get", "svc", "-l", "gateway.networking.k8s.io/gateway-name=automatic", "-o=name")
	k("delete", "gateway/automatic", "--cascade=foreground", "--timeout=2m")
	k("wait", "--for=delete", svc, "--timeout=2m")
	k("patch", "gateway/fixed", "--type=json", "--patch-file=https-listener.json")
	k("patch", "httproute/fixed", "--type=merge", "-p", `{"spec":{"parentRefs":[{"name":"fixed","sectionName":"websecure"}],"hostnames":["bench.example.test"]}}`)
	// The echo Service is single-stack and has exactly one replica.
	ip := k("get", "endpointslice", "-l", "kubernetes.io/service-name=echo", "-o=jsonpath={.items[0].endpoints[0].addresses[0]}")
	apply(t, "traefik.yaml.in", map[string]string{"backend": net.JoinHostPort(ip, "8080"), "vip": traefikIP, "family": family})
	k("rollout", "status", "deployment/traefik-bench", "--timeout=3m")
	k("wait", "service/traefik-bench", "--for=jsonpath={.status.loadBalancer.ingress[0].ip}="+traefikIP, "--timeout=2m")
	runComparison(t, roots, []string{net.JoinHostPort(weicheIP, "8443"), net.JoinHostPort(traefikIP, "8443")})
}

var alpn = [...]string{"", "http/1.1", "h2", "h3"}

type benchClient struct {
	*http.Client
	close func()
	major int
	url   string
}

func newBenchClient(roots *x509.CertPool, addr string, major int) benchClient {
	config := &tls.Config{RootCAs: roots, ServerName: benchHost, MinVersion: tls.VersionTLS13, NextProtos: []string{alpn[major]}}
	client := &http.Client{Timeout: 4 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var closeTransport func()
	if major == 3 {
		tr := &http3.Transport{TLSClientConfig: config, DisableCompression: true}
		client.Transport, closeTransport = tr, func() { _ = tr.Close() }
	} else {
		p := new(http.Protocols)
		p.SetHTTP1(major == 1)
		p.SetHTTP2(major == 2)
		tr := &http.Transport{TLSClientConfig: config, Protocols: p, DisableCompression: true, MaxConnsPerHost: 1}
		client.Transport, closeTransport = tr, tr.CloseIdleConnections
	}
	return benchClient{client, closeTransport, major, "https://" + addr + "/echo?msg=" + benchPayload}
}

func (c benchClient) request() error {
	req, err := http.NewRequest("GET", c.url, nil)
	if err != nil {
		return err
	}
	req.Host = benchHost
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return err
	}
	negotiated := ""
	if resp.TLS != nil {
		negotiated = resp.TLS.NegotiatedProtocol
	}
	if negotiated != alpn[c.major] || resp.ProtoMajor != c.major || resp.StatusCode != 200 || string(body) != benchPayload {
		return fmt.Errorf("%s: expected %s, received %s (ALPN %q), %s, body %q", c.url, alpn[c.major], resp.Proto, negotiated, resp.Status, body)
	}
	return nil
}

type sample struct {
	requestsPerSecond float64
	p50, p95, p99     time.Duration
}

const benchConnections = 8

// One independent transport per connection. Share it among four streams for
// multiplexed protocols; HTTP/1.1 permits one outstanding request per connection.
func measure(t *testing.T, roots *x509.CertPool, addr string, major int, duration time.Duration) sample {
	t.Helper()
	streams := 4
	if major == 1 {
		streams = 1
	}
	clients := make([]benchClient, benchConnections)
	for i := range clients {
		clients[i] = newBenchClient(roots, addr, major)
		defer clients[i].close()
		if err := clients[i].request(); err != nil {
			t.Fatal(err)
		}
	}
	type lane struct {
		times []time.Duration
		err   error
	}
	lanes := make([]lane, len(clients)*streams)
	var ready, workers sync.WaitGroup
	ready.Add(len(lanes))
	start := make(chan struct{})
	var warmUntil, end time.Time
	for i := range lanes {
		workers.Go(func() {
			client := clients[i/streams]
			ready.Done()
			<-start
			for {
				before := time.Now()
				if !before.Before(end) {
					return
				}
				if err := client.request(); err != nil {
					lanes[i].err = err
					return
				}
				if !before.Before(warmUntil) {
					lanes[i].times = append(lanes[i].times, time.Since(before))
				}
			}
		})
	}
	ready.Wait()
	warmUntil = time.Now().Add(2 * time.Second)
	end = warmUntil.Add(duration)
	close(start)
	workers.Wait()
	elapsed := time.Since(warmUntil)
	var timings []time.Duration
	for _, l := range lanes {
		if l.err != nil {
			t.Fatal(l.err)
		}
		if len(l.times) == 0 {
			t.Fatal("load client completed no measured requests")
		}
		timings = append(timings, l.times...)
	}
	slices.Sort(timings)
	percentile := func(p int) time.Duration { return timings[(len(timings)*p+99)/100-1] }
	return sample{float64(len(timings)) / elapsed.Seconds(), percentile(50), percentile(95), percentile(99)}
}

// Alternate who goes first on each paired round to balance order effects on
// a shared runner. Report every round so variation is visible.
func runComparison(t *testing.T, roots *x509.CertPool, addrs []string) {
	t.Helper()
	var summary strings.Builder
	fmt.Fprintf(&summary, "### Proxy comparison (%s)\n\n1 Pod/proxy, 2 vCPU limit, 256 MiB, TLS 1.3, 8 persistent connections; H1: 1 in-flight request/connection, H2/H3: 4 concurrent streams/connection. 2 s warm-up + 5 s measured per sample.\n\n", os.Getenv("FAMILY"))
	fmt.Fprintln(&summary, "| Protocol | Round | Proxy | req/s | p50 ms | p95 ms | p99 ms |\n|---|---:|---|---:|---:|---:|---:|")
	for major := 1; major <= 3; major++ {
		// Configuration propagation is outside the timed workload.
		for _, addr := range addrs {
			c := newBenchClient(roots, addr, major)
			deadline := time.Now().Add(time.Minute)
			for {
				err := c.request()
				if err == nil {
					break
				}
				if time.Now().After(deadline) {
					c.close()
					t.Fatal(err)
				}
				time.Sleep(250 * time.Millisecond)
			}
			c.close()
		}
		for round := range 4 {
			for order := range 2 {
				i := (round + order) % 2
				name := []string{"Weiche", "Traefik"}[i]
				s := measure(t, roots, addrs[i], major, 5*time.Second)
				row := fmt.Sprintf("| H%d | %d | %s | %.0f | %.2f | %.2f | %.2f |", major, round+1, name, s.requestsPerSecond, s.p50.Seconds()*1e3, s.p95.Seconds()*1e3, s.p99.Seconds()*1e3)
				t.Log(row)
				fmt.Fprintln(&summary, row)
			}
		}
	}
	if path := os.Getenv("GITHUB_STEP_SUMMARY"); path != "" {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := io.WriteString(f, summary.String()); err != nil {
			t.Fatal(err)
		}
	}
}
