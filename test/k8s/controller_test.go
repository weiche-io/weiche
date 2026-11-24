package k8s

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func command(t *testing.T, name string, args ...string) []byte {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), name, args...)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return out
}

func getResource(t *testing.T, dest any, args ...string) {
	t.Helper()
	args = append([]string{"get", "--request-timeout=5s", "-o=json"}, args...)
	if err := json.Unmarshal(command(t, "kubectl", args...), dest); err != nil {
		t.Fatal(err)
	}
}

type leader struct{ uid, pod string }

// A surviving proxy can serve old routes with no controller. Observe two renewals
// from the same ready Pod after the requested number of replicas has settled.
func awaitLeader(t *testing.T, replicas int, retired string) leader {
	t.Helper()
	var previous leader
	var renewed time.Time
	var state string
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(time.Second) {
		var pods struct {
			Items []struct {
				Metadata struct {
					Name, UID         string
					DeletionTimestamp *time.Time
				}
				Status struct {
					Conditions []struct{ Type, Status string }
				}
			}
		}
		var lease struct {
			Spec struct {
				HolderIdentity       string
				RenewTime            time.Time
				LeaseDurationSeconds int
			}
		}
		getResource(t, &pods, "pods", "-l", "app=weiche,app.kubernetes.io/instance=weiche")
		getResource(t, &lease, "lease/weiche-controller")
		ready := 0
		var current leader
		for _, pod := range pods.Items {
			if pod.Metadata.DeletionTimestamp != nil {
				continue
			}
			for _, c := range pod.Status.Conditions {
				if c.Type == "Ready" && c.Status == "True" {
					ready++
					if pod.Metadata.UID == lease.Spec.HolderIdentity {
						current = leader{pod.Metadata.UID, pod.Metadata.Name}
					}
				}
			}
		}
		state = fmt.Sprintf("%d pods, %d ready, holder %q, renewed %s", len(pods.Items), ready, lease.Spec.HolderIdentity, lease.Spec.RenewTime)
		expires := lease.Spec.RenewTime.Add(time.Duration(lease.Spec.LeaseDurationSeconds) * time.Second)
		if len(pods.Items) == replicas && ready == replicas && current.uid != "" && current.uid != retired && time.Now().Before(expires) {
			if current == previous && lease.Spec.RenewTime.After(renewed) {
				t.Logf("%d replicas: %s renewed the lease (%s)", replicas, current.pod, current.uid)
				return current
			}
			previous, renewed = current, lease.Spec.RenewTime
		} else {
			previous = leader{}
		}
	}
	t.Fatalf("waiting for %d controllers and an active lease (retired %q): %s", replicas, retired, state)
	return leader{}
}

type endpoint struct{ ip, host string }

var client = http.Client{
	Timeout:       3 * time.Second,
	Transport:     &http.Transport{DisableKeepAlives: true},
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

func (e endpoint) probe() error {
	msg := e.host + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	req, err := http.NewRequest("GET", "http://"+net.JoinHostPort(e.ip, "80")+"/echo?msg="+msg, nil)
	if err != nil {
		return err
	}
	req.Host = e.host
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK || string(body) != msg {
		return fmt.Errorf("%s via %s: %s, body %q", e.host, e.ip, resp.Status, body)
	}
	return nil
}

// Follow Gateway API conformance's requirement for consecutive matching responses
// while a newly configured HTTPRoute propagates.
func (e endpoint) await(t *testing.T) {
	t.Helper()
	var last error
	for deadline, successes := time.Now().Add(time.Minute), 0; time.Now().Before(deadline); {
		if last = e.probe(); last != nil {
			successes = 0
			time.Sleep(time.Second)
			continue
		}
		successes++
		if successes == 3 {
			return
		}
	}
	t.Fatalf("route %s via %s did not converge: %v", e.host, e.ip, last)
}

func controllerScaling(t *testing.T, fixed, automatic endpoint) {
	for _, e := range []endpoint{fixed, automatic} {
		e.await(t)
	}

	// Keep eight clients per VIP for the entire run, including after a failure.
	// Report the first error per VIP and count every subsequent failure.
	stop := make(chan struct{})
	var workers sync.WaitGroup
	var probes [2]struct{ ok, failed atomic.Uint64 }
	started := time.Now()
	for i, e := range []endpoint{fixed, automatic} {
		for range 8 {
			workers.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}
					if err := e.probe(); err != nil {
						if probes[i].failed.Add(1) == 1 {
							t.Errorf("interrupted forwarding to %s via %s: %v", e.host, e.ip, err)
						}
						continue
					}
					probes[i].ok.Add(1)
				}
			})
		}
	}
	defer func() {
		close(stop)
		workers.Wait()
		t.Logf("8 clients per VIP over %s: fixed ok=%d failed=%d; automatic ok=%d failed=%d", time.Since(started),
			probes[0].ok.Load(), probes[0].failed.Load(), probes[1].ok.Load(), probes[1].failed.Load())
	}()

	var holder leader
	var lastOK, lastFailed [2]uint64
	for stage, replicas := range []int{1, 2, 3, 1} {
		var retired string
		if stage > 0 {
			if replicas == 1 {
				// Prefer retiring the incumbent on scale-down so a standby must take over.
				command(t, "kubectl", "annotate", "pod/"+holder.pod, "controller.kubernetes.io/pod-deletion-cost=-1000")
				retired = holder.uid
			}
			command(t, "helm", "upgrade", "weiche", "../../charts/weiche", "--reuse-values", "--set", fmt.Sprintf("controller.replicas=%d", replicas), "--wait", "--timeout=2m")
		}
		holder = awaitLeader(t, replicas, retired)
		// Only a working reconciler can serve this stage's new hostname.
		fresh := endpoint{fixed.ip, fmt.Sprintf("stage-%d.example.test", stage)}
		patch := fmt.Sprintf(`{"spec":{"hostnames":[%q,%q]}}`, fixed.host, fresh.host)
		generation := command(t, "kubectl", "patch", "httproute/fixed", "--type=merge", "-p", patch, "-o=jsonpath={.metadata.generation}")
		observed := `--for=jsonpath={.status.parents[0].conditions[?(@.type=="Accepted")].observedGeneration}=` + strings.TrimSpace(string(generation))
		command(t, "kubectl", "wait", "httproute/fixed", observed, "--timeout=1m")
		fresh.await(t)
		for i, e := range []endpoint{fixed, automatic} {
			ok, failed := probes[i].ok.Load(), probes[i].failed.Load()
			t.Logf("%d replicas, %s: ok=%d failed=%d", replicas, e.host, ok-lastOK[i], failed-lastFailed[i])
			lastOK[i], lastFailed[i] = ok, failed
		}
	}
}
