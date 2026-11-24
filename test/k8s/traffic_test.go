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
	// Keep whole seconds and the fractional remainder separate. Rounding the
	// period down (e.g. at 15/s) would schedule an extra slot at the window end.
	slotAt := func(d time.Duration) int {
		return int(d/time.Second)*rate + int((d%time.Second)*time.Duration(rate)/time.Second)
	}
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
			return missed + slotAt(duration-time.Nanosecond) + 1 - slot
		}
		due := slotAt(elapsed)
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
		next := time.Duration(slot/rate)*time.Second + (time.Duration(slot%rate)*time.Second+time.Duration(rate)-1)/time.Duration(rate)
		timer.Reset(time.Until(start.Add(next)))
	}
}
