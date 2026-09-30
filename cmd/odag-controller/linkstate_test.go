package main

import (
	"testing"
	"time"
)

func resetLinks() {
	linkLive.Range(func(k, _ any) bool { linkLive.Delete(k); return true })
}

func TestObserveTransferJudgesAgainstThePlan(t *testing.T) {
	resetLinks()
	defer resetLinks()
	now := time.Now()
	// Within 2x its prediction plus 2 s: not evidence of degradation, even
	// though raw throughput is low (it was planned to share the link).
	if observeTransfer("a", "b", 30e6, 4, 9.9, false, now) || linkDegraded("a", "b", now) {
		t.Fatal("judged degraded within its prediction")
	}
	if !observeTransfer("a", "b", 30e6, 4, 10.1, false, now) || !linkDegraded("a", "b", now) {
		t.Fatal("not judged degraded past 2x its prediction + 2 s")
	}
	if observeTransfer("a", "b", 30e6, 4, 12, false, now) {
		t.Fatal("reported newly degraded twice")
	}
	bw := liveBandwidth(func(string, string) float64 { return 100e6 })
	if got := bw("a", "b"); got != 30e6/12 {
		t.Fatalf("live bandwidth %v, want the evidence's %v", got, 30e6/12.0)
	}
	if got := bw("b", "a"); got != 100e6 {
		t.Fatalf("reverse direction %v, want the model's", got)
	}
	// A completed transfer within its prediction clears it.
	observeTransfer("a", "b", 30e6, 4, 4.5, true, now)
	if linkDegraded("a", "b", now) {
		t.Fatal("still degraded after a transfer within its prediction")
	}
	// Small transfers are no evidence.
	if observeTransfer("a", "c", 1000, 0.01, 30, true, now) || linkDegraded("a", "c", now) {
		t.Fatal("a tiny transfer counted as evidence")
	}
	// Evidence expires.
	observeTransfer("a", "d", 30e6, 1, 60, false, now)
	if linkDegraded("a", "d", now.Add(linkObsTTL)) {
		t.Fatal("evidence did not expire")
	}
}

// p finished on a, its 30 MB output is crawling to c's node b over a
// degraded link. c has not started: it moves to a, where the input is.
func TestLinkReplanMovesWaitingConsumerToItsInput(t *testing.T) {
	resetLinks()
	defer resetLinks()
	tasks := []taskSpec{
		{Name: "p", Runtime: 12, DataSize: "30MB", CPU: "1"},
		{Name: "c", Runtime: 20, RuntimeProfile: map[string]float64{"a": 24, "b": 20, "x": 30},
			Dependencies: []string{"p"}, CPU: "1"},
	}
	nodes := testNodes("a", "b", "x")
	m := testModel(tasks, map[string]string{"p": "a", "c": "b"}, nodes, nil)
	now := time.Now()
	observeTransfer("a", "b", 30e6, 0.3, 12, false, now) // 12 s in, planned 0.3 s
	live := *m
	live.bw = liveBandwidth(m.bw)
	slow := func(src, dst string) bool { return linkDegraded(src, dst, now) }
	in := recoveryInput{
		m: &live, lost: map[string]bool{},
		assign: map[string]string{"p": "a", "c": "b"}, done: map[string]bool{"p": true},
		running: map[string]bool{}, onLost: map[string]bool{},
		holders: map[string][]string{"p": {"a"}}, now: 24,
		startup: func(string) float64 { return 3 },
		movable: map[string]bool{"c": true}, slow: slow,
	}
	plan := planRecovery(in)
	if len(plan.Moves) != 1 || plan.Moves[0].Task != "c" || plan.Moves[0].To != "a" {
		t.Fatalf("moves = %+v, want c -> a", plan.Moves)
	}
	if len(plan.Copies) != 0 || len(plan.Rerun) != 0 {
		t.Fatalf("plan = %+v, want only the move", plan)
	}

	// Early on (1 s late), staying still looks better: no move.
	resetLinks()
	observeTransfer("a", "b", 30e6, 0.3, 2.7, false, now)
	if plan := planRecovery(in); len(plan.Moves) != 0 {
		t.Fatalf("moved too early: %+v", plan.Moves)
	}
}

// The output also has a copy on x with a healthy link to b: the waiting
// consumer stays and takes it from there.
func TestLinkReplanSourcesFromFasterCopy(t *testing.T) {
	resetLinks()
	defer resetLinks()
	tasks := []taskSpec{
		{Name: "p", Runtime: 12, DataSize: "30MB", CPU: "1"},
		{Name: "c", Runtime: 20, RuntimeProfile: map[string]float64{"a": 40, "b": 20, "x": 40},
			Dependencies: []string{"p"}, CPU: "1"},
	}
	nodes := testNodes("a", "b", "x")
	m := testModel(tasks, map[string]string{"p": "a", "c": "b"}, nodes, nil)
	now := time.Now()
	observeTransfer("a", "b", 30e6, 0.3, 12, false, now)
	live := *m
	live.bw = liveBandwidth(m.bw)
	plan := planRecovery(recoveryInput{
		m: &live, lost: map[string]bool{},
		assign: map[string]string{"p": "a", "c": "b"}, done: map[string]bool{"p": true},
		running: map[string]bool{}, onLost: map[string]bool{},
		holders: map[string][]string{"p": {"a", "x"}}, now: 24,
		startup: func(string) float64 { return 3 },
		movable: map[string]bool{"c": true},
		slow:    func(src, dst string) bool { return linkDegraded(src, dst, now) },
	})
	if len(plan.Moves) != 0 {
		t.Fatalf("moves = %+v, want c to stay", plan.Moves)
	}
	if len(plan.Copies) != 1 || plan.Copies[0] != (copyOrder{Object: "p", To: "b"}) {
		t.Fatalf("copies = %+v, want p re-sourced to b", plan.Copies)
	}
}
