package k8s

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
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
	k("patch", "httproute/fixed", "--type=json", "-p", `[
		{"op":"add","path":"/spec/parentRefs/0/sectionName","value":"websecure"},
		{"op":"replace","path":"/spec/hostnames","value":["bench.example.test"]},
		{"op":"replace","path":"/spec/rules/0/filters/0/responseHeaderModifier/set/0/value","value":"bench"}]`)
	// The echo Service is single-stack and has exactly one replica.
	ip := k("get", "endpointslice", "-l", "kubernetes.io/service-name=echo", "-o=jsonpath={.items[0].endpoints[0].addresses[0]}")
	apply(t, "traefik.yaml.in", map[string]string{"backend": net.JoinHostPort(ip, "8080"), "vip": traefikIP, "family": family})
	k("rollout", "status", "deployment/traefik-bench", "--timeout=3m")
	k("wait", "service/traefik-bench", "--for=jsonpath={.status.loadBalancer.ingress[0].ip}="+traefikIP, "--timeout=2m")
	addrs := []string{net.JoinHostPort(weicheIP, "8443"), net.JoinHostPort(traefikIP, "8443")}
	setup := newMetricTable("Proxy configuration", "Proxy", "Deployment", "Ready pods", "CPU limit/pod (cores)", "Memory limit/pod", "Proxy → backend", "Backend ready pods")
	backendPods := jsonpath(t, "deployment/echo", "{.status.readyReplicas}")
	for _, proxy := range []struct{ name, deploy string }{{"Weiche", "fixed"}, {"Traefik", "traefik-bench"}} {
		config := jsonpath(t, "deployment/"+proxy.deploy, `{.status.readyReplicas}{" | "}{.spec.template.spec.containers[0].resources.limits.cpu}{" | "}{.spec.template.spec.containers[0].resources.limits.memory}`)
		setup.row("%s | %s | %s | HTTP/1.1 · plaintext/TCP | %s", proxy.name, proxy.deploy, config, backendPods)
	}
	setup.publish(t)
	t.Run("PersistentConnections", func(t *testing.T) { runComparison(t, roots, addrs) })
	short := newMetricTable("Fresh connections", "Proxy", "Client → proxy (ALPN)", "Target (conn/s)", "Window (s)", "Max in flight", "Issued requests", "Missed slots", "OK", "Failed", "Incorrect responses", "Request errors (%)", "Error limit (%)", "Success p99 (ms)", "p99 limit (ms)", "Verdict")
	defer short.publish(t)
	for i, name := range []string{"Weiche", "Traefik"} {
		t.Run("ShortConnections/"+name, func(t *testing.T) { shortConnections(t, roots, addrs[i], name, short) })
	}
}

var frontend = [...]string{"", "HTTP/1.1 · TLS 1.3/TCP (`http/1.1`)", "HTTP/2 · TLS 1.3/TCP (`h2`)", "HTTP/3 · TLS 1.3/QUIC/UDP (`h3`)"}

var alpn = [...]string{"", "http/1.1", "h2", "h3"}

// A bad protocol or incorrect successful payload always violates routing correctness.
var errWrongResponse = errors.New("wrong proxy response")

type benchClient struct {
	*http.Client
	close func()
	major int
	url   string
}

func newBenchClient(roots *x509.CertPool, addr string, major int) benchClient {
	config := &tls.Config{RootCAs: roots, ServerName: benchHost, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, NextProtos: []string{alpn[major]}}
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
	return benchClient{client, closeTransport, major, "https://" + addr + "/echo?msg="}
}

func (c benchClient) request() error { return c.echo(benchPayload) }

func (c benchClient) echo(payload string) error {
	req, err := http.NewRequest("GET", c.url+url.QueryEscape(payload), nil)
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
	route, backend := resp.Header.Get("X-Bench-Route"), resp.Header.Get("X-Bench-Backend")
	if negotiated != alpn[c.major] || resp.ProtoMajor != c.major || (resp.StatusCode == 200 && (route != "bench" || backend != "echo" || string(body) != payload)) {
		return fmt.Errorf("%w: expected %s and %q, got %s (ALPN %q), %s, route %q, backend %q, body %q", errWrongResponse, alpn[c.major], payload, resp.Proto, negotiated, resp.Status, route, backend, body)
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("%s: %s", c.url, resp.Status)
	}
	return nil
}

type sample struct {
	requests      int
	elapsed       time.Duration
	p50, p95, p99 time.Duration
}

const (
	benchConnections = 8
	benchWarmup      = 2 * time.Second
	benchWindow      = 5 * time.Second
)

// One independent transport per connection. Share it among four streams for
// multiplexed protocols; HTTP/1.1 permits one outstanding request per connection.
func measure(t *testing.T, roots *x509.CertPool, addr string, major, streams int, duration time.Duration) sample {
	t.Helper()
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
	warmUntil = time.Now().Add(benchWarmup)
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
	return sample{len(timings), elapsed, percentile(50), percentile(95), percentile(99)}
}

// Alternate who goes first on each paired round to balance order effects on
// a shared runner. Report every round so variation is visible.
func runComparison(t *testing.T, roots *x509.CertPool, addrs []string) {
	t.Helper()
	table := newMetricTable("Persistent connections", "Client → proxy (ALPN)", "Proxy", "Round", "Order", "Connection slots", "Max in flight/conn", "Warm-up (s)", "Target window (s)", "Elapsed incl. drain (s)", "OK", "OK req/s", "p50 (ms)", "p95 (ms)", "p99 (ms)")
	defer table.publish(t)
	for major := 1; major <= 3; major++ {
		streams := 4
		if major == 1 {
			streams = 1
		}
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
				s := measure(t, roots, addrs[i], major, streams, benchWindow)
				row := fmt.Sprintf("%s | %s | %d | %d | %d | %d | %g | %g | %.4f | %d | %.1f | %.3f | %.3f | %.3f", frontend[major], name, round+1, order+1, benchConnections, streams, benchWarmup.Seconds(), benchWindow.Seconds(), s.elapsed.Seconds(), s.requests, float64(s.requests)/s.elapsed.Seconds(), s.p50.Seconds()*1e3, s.p95.Seconds()*1e3, s.p99.Seconds()*1e3)
				t.Log(row)
				table.row("%s", row)
			}
		}
	}
}

// These are CI load targets, adjustable independently of the persistent-connection benchmark.
var (
	capacityRate         = flag.Int("capacity-rate", 200, "target fresh TLS connections per second per proxy")
	capacityWindow       = flag.Duration("capacity-duration", 30*time.Second, "short-connection measurement window")
	capacityErrorPercent = flag.Float64("capacity-error-percent", 0.1, "maximum percent of issued requests that may fail")
	capacityP99          = flag.Duration("capacity-p99", 250*time.Millisecond, "maximum p99 latency of successful short connections")
)

func shortConnections(t *testing.T, roots *x509.CertPool, addr, name string, table *metricTable) {
	t.Helper()
	if *capacityRate <= 0 || *capacityRate > int(time.Second) || *capacityWindow < time.Second ||
		*capacityErrorPercent < 0 || *capacityErrorPercent > 100 || math.IsNaN(*capacityErrorPercent) || *capacityP99 <= 0 {
		t.Fatal("invalid short-connection load targets")
	}
	c := newBenchClient(roots, addr, 1)
	c.Transport.(*http.Transport).DisableKeepAlives = true
	defer c.close()
	var mu sync.Mutex
	var latencies []time.Duration
	var failed, wrong int
	var firstError, firstWrong error
	const concurrency = 64
	missed := fixedRate(t.Context(), *capacityRate, *capacityWindow, concurrency, func(slot int) error {
		return c.echo(fmt.Sprintf("%s-%d", benchPayload, slot))
	}, func(latency time.Duration, err error) {
		mu.Lock()
		defer mu.Unlock()
		if err == nil {
			latencies = append(latencies, latency)
			return
		}
		failed++
		if firstError == nil {
			firstError = err
		}
		if errors.Is(err, errWrongResponse) {
			wrong++
			if firstWrong == nil {
				firstWrong = err
			}
		}
	})
	issued := len(latencies) + failed
	t.Logf("target=%d fresh TLS connections/s for %s, issued=%d missed=%d, ok=%d failed=%d wrong=%d", *capacityRate, *capacityWindow, issued, missed, len(latencies), failed, wrong)
	if wrong > 0 {
		t.Errorf("%d incorrect responses; first: %v", wrong, firstWrong)
	}
	if missed > 0 {
		t.Errorf("inconclusive capacity: %d scheduled requests were never issued", missed)
	}
	errValue, p99Value := "—", "—"
	if issued > 0 {
		errPct := 100 * float64(failed) / float64(issued)
		errValue = fmt.Sprintf("%.4f", errPct)
		if missed == 0 && errPct > *capacityErrorPercent {
			t.Errorf("failure rate %.4f%% exceeds %.4f%%; first: %v", errPct, *capacityErrorPercent, firstError)
		}
	} else {
		t.Error("no requests issued")
	}
	if len(latencies) > 0 {
		slices.Sort(latencies)
		p99 := latencies[(len(latencies)*99+99)/100-1]
		p99Value = fmt.Sprintf("%.3f", p99.Seconds()*1e3)
		if missed == 0 && p99 > *capacityP99 {
			t.Errorf("success p99 %s exceeds %s", p99, *capacityP99)
		}
	} else {
		t.Error("no successful request to measure latency")
	}
	verdict := "PASS"
	if missed > 0 && wrong == 0 {
		verdict = "INCONCLUSIVE"
	} else if t.Failed() {
		verdict = "FAIL"
	}
	row := fmt.Sprintf("%s | %s | %d | %g | %d | %d | %d | %d | %d | %d | %s | %.4f | %s | %g | %s",
		name, frontend[1], *capacityRate, capacityWindow.Seconds(), concurrency, issued, missed, len(latencies), failed, wrong, errValue, *capacityErrorPercent, p99Value, capacityP99.Seconds()*1e3, verdict)
	t.Log(row)
	table.row("%s", row)
}
