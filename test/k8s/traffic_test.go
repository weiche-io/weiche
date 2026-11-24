package k8s

import (
	"context"
	"sync"
	"time"
)

// Drive arrivals from the clock. If all workers are busy or the runner wakes
// late, count the unissued slots instead of queueing a burst after the stall.
// A zero duration keeps the same schedule until the caller ends the phase.
func fixedRate(ctx context.Context, rate int, duration time.Duration, concurrency int, request func(int) error, record func(time.Duration, error)) (missed int) {
	start := time.Now()
	period := time.Second / time.Duration(rate)
	active := make(chan struct{}, concurrency)
	var workers sync.WaitGroup
	defer workers.Wait()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for slot := 0; ; {
		select {
		case <-ctx.Done():
			return missed
		case <-timer.C:
		}
		elapsed := time.Since(start)
		if duration > 0 && elapsed >= duration {
			return missed + int((duration-1)/period) + 1 - slot
		}
		due := int(elapsed / period)
		if due >= slot {
			missed += due - slot
			select {
			case active <- struct{}{}:
				workers.Go(func() {
					defer func() { <-active }()
					before := time.Now()
					err := request(due)
					record(time.Since(before), err)
				})
			default:
				missed++
			}
			slot = due + 1
		}
		timer.Reset(time.Until(start.Add(time.Duration(slot) * period)))
	}
}
