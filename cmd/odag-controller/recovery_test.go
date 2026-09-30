package main

import (
	"testing"
	"time"
)

func TestObserveNodeGraceAndRestore(t *testing.T) {
	healthMu.Lock()
	healthOf = map[string]*nodeHealth{}
	healthMu.Unlock()
	t0 := time.Unix(1000, 0)

	if lost, _ := observeNode("n", false, true, t0); lost {
		t.Fatal("lost on the first failed probe")
	}
	if lost, _ := observeNode("n", false, true, t0.Add(nodeLossGrace-time.Millisecond)); lost {
		t.Fatal("lost before the grace period")
	}
	if lost, _ := observeNode("n", false, true, t0.Add(nodeLossGrace)); !lost || !nodeLost("n") {
		t.Fatal("not lost after the grace period")
	}
	if lost, _ := observeNode("n", false, true, t0.Add(2*nodeLossGrace)); lost {
		t.Fatal("reported lost twice")
	}
	// Back only when the agent answers AND Kubernetes says Ready.
	if _, back := observeNode("n", true, false, t0.Add(3*nodeLossGrace)); back || !nodeLost("n") {
		t.Fatal("restored while NotReady")
	}
	if _, back := observeNode("n", true, true, t0.Add(4*nodeLossGrace)); !back || nodeLost("n") {
		t.Fatal("not restored")
	}
	// NotReady is lost at once; a blip of the agent alone is not.
	if lost, _ := observeNode("m", true, false, t0); !lost {
		t.Fatal("NotReady node not lost at once")
	}
	observeNode("k", false, true, t0)
	observeNode("k", true, true, t0.Add(nodeLossGrace/2))
	if lost, _ := observeNode("k", false, true, t0.Add(nodeLossGrace)); lost {
		t.Fatal("an answer in between must restart the grace period")
	}
	healthMu.Lock()
	healthOf = map[string]*nodeHealth{}
	healthMu.Unlock()
}

func TestPickSourcePrefersServingCopyThenFastestLink(t *testing.T) {
	ready := func(n string) bool { return n != "dead" }
	bw := linkTable([2]string{"slow", "t"})
	nodes := []string{"dead", "fast", "slow", "t"}

	if got := pickSource(realizationEntry{ServingCopy: "slow"}, "t", ready, nodes, bw); got != "slow" {
		t.Fatalf("servingCopy ignored: %s", got)
	}
	if got := pickSource(realizationEntry{}, "t", ready, nodes, bw); got != "fast" {
		t.Fatalf("got %s, want the holder with the fastest link", got)
	}
	if got := pickSource(realizationEntry{ServingCopy: "dead"}, "t", ready, nodes, bw); got != "fast" {
		t.Fatalf("got %s, want a fallback when the serving copy is not ready", got)
	}
	none := func(string) bool { return false }
	if got := pickSource(realizationEntry{}, "t", none, nodes, bw); got != "" {
		t.Fatalf("got %s with nothing ready", got)
	}
}

// chain: p (12 s, 8 MB) -> c (2 s), both planned on node a; b and c are the
// other nodes, b holding whatever copies the test gives it.
func chainInput(holders map[string][]string) recoveryInput {
	tasks := []taskSpec{
		{Name: "p", Runtime: 12, DataSize: "8MB", CPU: "1"},
		{Name: "c", Runtime: 2, Dependencies: []string{"p"}, CPU: "1"},
	}
	nodes := testNodes("a", "b", "x")
	m := testModel(tasks, map[string]string{"p": "a", "c": "a"}, nodes, linkTable([2]string{"a", "x"}, [2]string{"b", "x"}))
	return recoveryInput{
		m: m, lost: map[string]bool{"a": true},
		assign:  map[string]string{"p": "a", "c": "a"},
		done:    map[string]bool{"p": true},
		running: map[string]bool{}, onLost: map[string]bool{},
		holders: holders, now: 12,
		startup: func(string) float64 { return 3 },
	}
}

func TestRecoveryUsesSurvivingCopy(t *testing.T) {
	in := chainInput(map[string][]string{"p": {"b"}})
	plan := planRecovery(in)
	if len(plan.Rerun) != 0 || len(plan.Copies) != 0 || len(plan.Stuck) != 0 {
		t.Fatalf("plan = %+v: want no rerun and no copy", plan)
	}
	if len(plan.Moves) != 1 || plan.Moves[0].Task != "c" || plan.Moves[0].To != "b" {
		t.Fatalf("moves = %+v, want c -> b (where the copy is)", plan.Moves)
	}
	// Starts after dispatch (3 s) at 12 s, runs 2 s.
	if plan.Moves[0].EFT != 17 {
		t.Fatalf("estimated finish %v, want 17", plan.Moves[0].EFT)
	}
}

func TestRecoveryRerunsProducerWithoutCopy(t *testing.T) {
	in := chainInput(map[string][]string{})
	plan := planRecovery(in)
	if len(plan.Rerun) != 1 || plan.Rerun[0] != "p" {
		t.Fatalf("rerun = %v, want [p]", plan.Rerun)
	}
	if len(plan.Moves) != 2 || plan.Moves[0].Task != "p" || plan.Moves[1].Task != "c" {
		t.Fatalf("moves = %+v, want p then c", plan.Moves)
	}
	if plan.Moves[0].To != plan.Moves[1].To {
		t.Fatalf("c should follow p (no transfer): %+v", plan.Moves)
	}
	if len(plan.Copies) != 0 {
		t.Fatalf("copies = %+v: a re-run producer pushes itself", plan.Copies)
	}
}

func TestRecoveryRestartsTaskDispatchedOnLostNode(t *testing.T) {
	in := chainInput(map[string][]string{"p": {"b"}})
	in.onLost["c"] = true
	plan := planRecovery(in)
	if len(plan.Restart) != 1 || plan.Restart[0] != "c" {
		t.Fatalf("restart = %v, want [c]", plan.Restart)
	}
}

func TestRecoveryCopiesInputForMovedConsumerOfRunningProducer(t *testing.T) {
	// p runs on b (survives); its consumer c was planned on lost node a.
	in := chainInput(map[string][]string{})
	in.assign = map[string]string{"p": "b", "c": "a"}
	in.done = map[string]bool{}
	in.running = map[string]bool{"p": true}
	in.m.assign = map[string]string{"p": "b", "c": "a"}
	plan := planRecovery(in)
	if len(plan.Moves) != 1 || plan.Moves[0].Task != "c" {
		t.Fatalf("moves = %+v", plan.Moves)
	}
	if plan.Moves[0].To == "b" {
		if len(plan.Copies) != 0 {
			t.Fatalf("c moved next to p; no copy needed: %+v", plan.Copies)
		}
		return
	}
	if len(plan.Copies) != 1 || plan.Copies[0] != (copyOrder{Object: "p", To: plan.Moves[0].To}) {
		t.Fatalf("copies = %+v: p pushes to c's old node, so c's new node needs a copy", plan.Copies)
	}
}

func TestRecoveryLeavesUnaffectedRunAlone(t *testing.T) {
	in := chainInput(map[string][]string{"p": {"b"}})
	in.lost = map[string]bool{"x": true}
	in.assign = map[string]string{"p": "b", "c": "b"}
	in.m.assign = in.assign
	if plan := planRecovery(in); !plan.empty() {
		t.Fatalf("plan = %+v, want nothing to do", plan)
	}
}

func TestRecoveryStuckOnConstrainedTask(t *testing.T) {
	in := chainInput(map[string][]string{"p": {"b"}})
	in.m.byName["c"].Constraints = []string{"a"}
	plan := planRecovery(in)
	if len(plan.Stuck) != 1 || plan.Stuck[0] != "c" {
		t.Fatalf("stuck = %v, want [c]", plan.Stuck)
	}
}

func TestRecoveryLineageThroughTwoLostOutputs(t *testing.T) {
	// a -> b -> c all on node n1 (lost); a and b finished, c pending, and
	// nothing survives: both producers run again, in order.
	tasks := []taskSpec{
		{Name: "a", Runtime: 1, DataSize: "1MB", CPU: "1"},
		{Name: "b", Runtime: 1, DataSize: "1MB", Dependencies: []string{"a"}, CPU: "1"},
		{Name: "c", Runtime: 1, Dependencies: []string{"b"}, CPU: "1"},
	}
	nodes := testNodes("n1", "n2")
	m := testModel(tasks, map[string]string{"a": "n1", "b": "n1", "c": "n1"}, nodes, nil)
	plan := planRecovery(recoveryInput{
		m: m, lost: map[string]bool{"n1": true},
		assign:  map[string]string{"a": "n1", "b": "n1", "c": "n1"},
		done:    map[string]bool{"a": true, "b": true},
		running: map[string]bool{}, onLost: map[string]bool{},
		holders: map[string][]string{}, now: 2,
		startup: func(string) float64 { return 0 },
	})
	if len(plan.Rerun) != 2 || plan.Rerun[0] != "a" || plan.Rerun[1] != "b" {
		t.Fatalf("rerun = %v, want [a b]", plan.Rerun)
	}
	if len(plan.Moves) != 3 {
		t.Fatalf("moves = %+v, want all three on n2", plan.Moves)
	}
}

func TestRecoveryCopiesInputWhenConsumerMovesAwayFromProducer(t *testing.T) {
	in := chainInput(map[string][]string{})
	in.assign = map[string]string{"p": "b", "c": "a"}
	in.done = map[string]bool{}
	in.running = map[string]bool{"p": true}
	in.m.assign = map[string]string{"p": "b", "c": "a"}
	in.m.byName["c"].Constraints = []string{"a", "x"}
	plan := planRecovery(in)
	if len(plan.Moves) != 1 || plan.Moves[0].To != "x" {
		t.Fatalf("moves = %+v, want c -> x", plan.Moves)
	}
	if len(plan.Copies) != 1 || plan.Copies[0] != (copyOrder{Object: "p", To: "x"}) {
		t.Fatalf("copies = %+v: p pushes to c's old node, so x needs a copy", plan.Copies)
	}
}

func TestRecoveryReportsRootOfStuckChain(t *testing.T) {
	in := chainInput(map[string][]string{})
	in.m.byName["p"].Constraints = []string{"a"}
	plan := planRecovery(in)
	if len(plan.Stuck) != 1 || plan.Stuck[0] != "p" || len(plan.Blocked) != 1 || plan.Blocked[0] != "c" {
		t.Fatalf("stuck %v blocked %v, want [p] and [c]", plan.Stuck, plan.Blocked)
	}
}
