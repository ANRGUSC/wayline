package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStatusPassesNeverOverlapAndCoalesce(t *testing.T) {
	defer func() { statusPassFn = statusPass }()
	statusRunners.Delete("ns/run")
	defer statusRunners.Delete("ns/run")

	var inFlight, maxInFlight, passes int32
	var mu sync.Mutex
	var seen []string
	release := make(chan struct{})
	started := make(chan struct{}, 10)
	statusPassFn = func(a statusArgs) {
		n := atomic.AddInt32(&inFlight, 1)
		for {
			m := atomic.LoadInt32(&maxInFlight)
			if n <= m || atomic.CompareAndSwapInt32(&maxInFlight, m, n) {
				break
			}
		}
		mu.Lock()
		seen = append(seen, a.tasks[0].Name)
		mu.Unlock()
		started <- struct{}{}
		<-release
		atomic.AddInt32(&inFlight, -1)
		atomic.AddInt32(&passes, 1)
	}
	req := func(tag string) {
		requestStatus(statusArgs{namespace: "ns", odagName: "run", tasks: []taskSpec{{Name: tag}}})
	}

	req("first")
	<-started // first pass is running
	// Many requests while it runs: they coalesce into one more pass, with
	// the latest arguments.
	for _, tag := range []string{"a", "b", "c", "latest"} {
		req(tag)
	}
	release <- struct{}{} // finish the first pass
	<-started             // the coalesced pass starts
	release <- struct{}{}
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&passes) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond) // no third pass may start
	if got := atomic.LoadInt32(&passes); got != 2 {
		t.Fatalf("passes = %d, want 2 (one in flight + one coalesced)", got)
	}
	if maxInFlight != 1 {
		t.Fatalf("%d passes ran at once, want 1", maxInFlight)
	}
	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "first" || got[1] != "latest" {
		t.Fatalf("passes saw %v, want [first latest]", got)
	}

	// Idle again: the next request starts a pass at once.
	req("again")
	select {
	case <-started:
		release <- struct{}{}
	case <-time.After(time.Second):
		t.Fatal("no pass started after the runner went idle")
	}
}
