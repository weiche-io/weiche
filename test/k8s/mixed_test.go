package k8s

import (
	"crypto/x509"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

type mixedCounts struct {
	ok, failed, incorrect int
	firstErr              error
}

func (c *mixedCounts) add(other mixedCounts) {
	c.ok += other.ok
	c.failed += other.failed
	c.incorrect += other.incorrect
	if c.firstErr == nil {
		c.firstErr = other.firstErr
	}
}

func mixedLoad(t *testing.T, roots *x509.CertPool, addr, name string, window time.Duration, table *metricTable) {
	t.Helper()
	const inFlight = 64 // per protocol
	streamsPerConn := [...]int{1: 1, 2: 4, 3: 4}
	var lanes [len(streamsPerConn)][inFlight]mixedCounts
	var ready, workers sync.WaitGroup
	start := make(chan struct{})
	var end time.Time
	for major := 1; major <= 3; major++ {
		streams := streamsPerConn[major]
		for conn := range inFlight / streams {
			client := newBenchClient(roots, addr, major)
			defer client.close()
			for stream := range streams {
				lane := conn*streams + stream
				prefix := fmt.Sprintf("%s-%d-%d-", benchPayload, major, lane)
				ready.Add(1)
				workers.Go(func() {
					var counts mixedCounts
					ready.Done()
					<-start
					for seq := 0; time.Now().Before(end); seq++ {
						err := client.echo(prefix + strconv.Itoa(seq))
						if err == nil {
							counts.ok++
							continue
						}
						counts.failed++
						if counts.firstErr == nil {
							counts.firstErr = err
						}
						if errors.Is(err, errWrongResponse) {
							counts.incorrect++
						}
					}
					lanes[major][lane] = counts
				})
			}
		}
	}
	ready.Wait()
	begin := time.Now()
	end = begin.Add(window)
	close(start)
	workers.Wait()
	elapsed := time.Since(begin).Seconds()

	record := func(protocol string, slots, limit int, counts mixedCounts) {
		total := counts.ok + counts.failed
		errorRate := "—"
		if total > 0 {
			errorRate = fmt.Sprintf("%.4f", 100*float64(counts.failed)/float64(total))
		}
		row := fmt.Sprintf("%s | %s | %d | %d | %g | %.4f | %d | %d | %d | %d | %s | %.1f | %.1f",
			name, protocol, slots, limit, window.Seconds(), elapsed, total, counts.ok, counts.failed, counts.incorrect, errorRate, float64(total)/elapsed, float64(counts.ok)/elapsed)
		table.row("%s", row)
		t.Log(row)
	}
	var all mixedCounts
	totalSlots := 0
	for major := 1; major <= 3; major++ {
		var counts mixedCounts
		for _, lane := range lanes[major] {
			counts.add(lane)
		}
		slots := inFlight / streamsPerConn[major]
		record(frontend[major], slots, inFlight, counts)
		totalSlots += slots
		all.add(counts)
		if counts.firstErr != nil {
			t.Logf("%s %s sample error: %v", name, alpn[major], counts.firstErr)
		}
		// overload may cause request failures; each protocol must still prove correct forwarding.
		if counts.ok == 0 || counts.incorrect > 0 {
			t.Errorf("%s %s: %d correct responses, %d incorrect responses", name, alpn[major], counts.ok, counts.incorrect)
		}
	}
	record("All protocols", totalSlots, 3*inFlight, all)
}
