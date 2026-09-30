package main

import (
	"math"
	"strings"
	"testing"
)

// Link classes from E13: B = 942 Mbit/s at 0.957 goodput, so B is about
// 113 MB/s and B/8 about 14.1 MB/s.
const (
	linkB  = 113e6
	linkB8 = 14.1e6
)

// linkTable returns a symmetric bandwidth function; unlisted pairs are fast.
func linkTable(slow ...[2]string) func(src, dst string) float64 {
	return func(src, dst string) float64 {
		if src == dst {
			return 0
		}
		for _, p := range slow {
			if (p[0] == src && p[1] == dst) || (p[0] == dst && p[1] == src) {
				return linkB8
			}
		}
		return linkB
	}
}

func decisionFor(ds []copyDecision, obj, consumer string) (copyDecision, bool) {
	for _, d := range ds {
		if d.Object == obj && d.Consumer == consumer {
			return d, true
		}
	}
	return copyDecision{}, false
}

// An expensive pod producer co-located with its only consumer leaves one
// copy; losing that node forces a recompute (about startup + runtime), so a
// copy on the consumer's fallback pays for itself many times over.
func TestPlanAwareCopiesExpensiveCoLocatedObject(t *testing.T) {
	tasks := []taskSpec{
		{Name: "embed", Runtime: 12, DataSize: "8.3MB", CPU: "1"},
		{Name: "fuse", Runtime: 2, Dependencies: []string{"embed"}, CPU: "1"},
	}
	nodes := testNodes("a", "b", "c")
	m := testModel(tasks, map[string]string{"embed": "a", "fuse": "a"}, nodes,
		linkTable([2]string{"a", "b"}))

	entries, ds := planAwareCopies(m, defaultRealizationPolicy(), nil, policyContext{})
	d, ok := decisionFor(ds, "embed", "fuse")
	if !ok || !d.Copy || !d.Recompute {
		t.Fatalf("decision = %+v, want a copy that avoids a recompute", d)
	}
	// Recompute: 3 s pod startup + 12 s runtime on the fallback.
	if math.Abs(d.DelayS-15) > 1e-6 {
		t.Fatalf("delay without copy = %v, want 15", d.DelayS)
	}
	if len(entries) != 1 || entries[0].Object != "embed" || entries[0].Origin != originPlan ||
		len(entries[0].Copies) != 1 || entries[0].Copies[0] != d.Fallback {
		t.Fatalf("entries = %+v, want one plan copy of embed on %s", entries, d.Fallback)
	}
}

// A cheap warm producer of a large object: recomputing costs a fraction of a
// second, the copy over a B/8 link costs more than 3 s. No copy.
func TestPlanAwareCopiesCheapLargeObjectSkipped(t *testing.T) {
	tasks := []taskSpec{
		{Name: "prep", Runtime: 0.2, DataSize: "46.7MB", Runner: "wl-runner", CPU: "1"},
		{Name: "embed", Runtime: 12, Dependencies: []string{"prep"}, CPU: "1"},
	}
	nodes := testNodes("a", "b")
	m := testModel(tasks, map[string]string{"prep": "a", "embed": "a"}, nodes,
		linkTable([2]string{"a", "b"}))

	entries, ds := planAwareCopies(m, defaultRealizationPolicy(), nil, policyContext{})
	d, _ := decisionFor(ds, "prep", "embed")
	if d.Copy || len(entries) != 0 {
		t.Fatalf("decision = %+v entries = %+v, want no copy", d, entries)
	}
	if d.CopyS < 3 || d.DelayS > 0.5 {
		t.Fatalf("delay %v copy %v: want recompute under 0.5 s, copy over 3 s", d.DelayS, d.CopyS)
	}
}

// Producer and consumer on different nodes: losing the consumer's node
// leaves the producer's copy, and a copy made in advance saves only the
// transfer it replaces. No copy.
func TestPlanAwareCopiesSurvivingCopyNotDuplicated(t *testing.T) {
	tasks := []taskSpec{
		{Name: "p", Runtime: 12, DataSize: "8.3MB", CPU: "1"},
		{Name: "c", Runtime: 2, Dependencies: []string{"p"}, CPU: "1"},
	}
	nodes := testNodes("a", "b", "c")
	m := testModel(tasks, map[string]string{"p": "a", "c": "b"}, nodes, nil)
	entries, ds := planAwareCopies(m, defaultRealizationPolicy(), nil, policyContext{})
	d, _ := decisionFor(ds, "p", "c")
	if d.Copy || d.Recompute || len(entries) != 0 {
		t.Fatalf("decision = %+v, want no copy and no recompute", d)
	}
}

// A consumer with enough slack absorbs the recompute: no copy.
func TestPlanAwareCopiesSlackAbsorbsLoss(t *testing.T) {
	tasks := []taskSpec{
		{Name: "src", Runtime: 1, DataSize: "1MB", CPU: "1"},
		{Name: "side", Runtime: 1, DataSize: "8MB", Dependencies: []string{"src"}, CPU: "1"},
		{Name: "sideuse", Runtime: 1, Dependencies: []string{"side"}, CPU: "1"},
		{Name: "long", Runtime: 60, Dependencies: []string{"src"}, CPU: "1"},
		{Name: "join", Runtime: 1, Dependencies: []string{"sideuse", "long"}, CPU: "1"},
	}
	nodes := testNodes("a", "b", "c")
	m := testModel(tasks, map[string]string{"src": "b", "side": "a", "sideuse": "a", "long": "b", "join": "b"},
		nodes, nil)
	_, ds := planAwareCopies(m, defaultRealizationPolicy(), nil, policyContext{})
	d, _ := decisionFor(ds, "side", "sideuse")
	if d.Copy || d.SlackS < 50 {
		t.Fatalf("decision = %+v, want no copy (slack about 57 s)", d)
	}
}

func TestPlanAwareCopiesConstrainedConsumerAndUserEntries(t *testing.T) {
	tasks := []taskSpec{
		{Name: "p", Runtime: 12, DataSize: "8MB", CPU: "1"},
		{Name: "pinned", Runtime: 2, Dependencies: []string{"p"}, Constraints: []string{"a"}, CPU: "1"},
		{Name: "q", Runtime: 12, DataSize: "8MB", CPU: "1"},
		{Name: "free", Runtime: 2, Dependencies: []string{"q"}, CPU: "1"},
	}
	nodes := testNodes("a", "b")
	m := testModel(tasks, map[string]string{"p": "a", "pinned": "a", "q": "b", "free": "b"}, nodes, nil)
	user := []realizationEntry{{Object: "q", Copies: []string{"b"}}}
	entries, ds := planAwareCopies(m, defaultRealizationPolicy(), user, policyContext{})
	if d, _ := decisionFor(ds, "p", "pinned"); d.Copy || !strings.Contains(d.Reason, "no fallback") {
		t.Fatalf("pinned consumer: %+v", d)
	}
	if d, _ := decisionFor(ds, "q", "free"); d.Copy || !strings.Contains(d.Reason, "user") {
		t.Fatalf("user-governed object: %+v", d)
	}
	if len(entries) != 0 {
		t.Fatalf("entries = %+v, want none", entries)
	}
}

func TestPlanAwareCopiesBudget(t *testing.T) {
	tasks := []taskSpec{
		{Name: "p1", Runtime: 12, DataSize: "8MB", CPU: "1"},
		{Name: "c1", Runtime: 2, Dependencies: []string{"p1"}, CPU: "1"},
		{Name: "p2", Runtime: 12, DataSize: "8MB", CPU: "1"},
		{Name: "c2", Runtime: 2, Dependencies: []string{"p2"}, CPU: "1"},
	}
	nodes := testNodes("a", "b", "c")
	m := testModel(tasks, map[string]string{"p1": "a", "c1": "a", "p2": "b", "c2": "b"}, nodes, nil)
	pol := defaultRealizationPolicy()
	pol.MaxCopyBytes = 10_000_000
	entries, ds := planAwareCopies(m, pol, nil, policyContext{})
	if countCopies(entries) != 1 {
		t.Fatalf("entries = %+v, want exactly one copy under a 10 MB budget", entries)
	}
	budgeted := 0
	for _, d := range ds {
		if strings.Contains(d.Reason, "budget") {
			budgeted++
		}
	}
	if budgeted != 1 {
		t.Fatalf("want one decision refused for budget, got %d: %+v", budgeted, ds)
	}
}

func TestMergeRealization(t *testing.T) {
	existing := []realizationEntry{
		{Object: "u", Copies: []string{"x"}},
		{Object: "old", Copies: []string{"y"}, Origin: originPlan},
		{Object: "shared", Copies: []string{"x"}, Evict: []string{"z"}},
	}
	plan := mergeRealization(existing, []realizationEntry{
		{Object: "new", Copies: []string{"y"}, Origin: originPlan},
		{Object: "u", Copies: []string{"z"}, Origin: originPlan},
	}, originPlan)
	if len(plan) != 3 || plan[0].Object != "u" || len(plan[0].Copies) != 1 ||
		plan[2].Object != "new" {
		t.Fatalf("plan merge = %+v: want old plan entry replaced, user entry kept", plan)
	}
	rec := mergeRealization(existing, []realizationEntry{
		{Object: "shared", Copies: []string{"z"}, Origin: originRecovery},
	}, originRecovery)
	for _, e := range rec {
		if e.Object == "shared" {
			if len(e.Copies) != 2 || len(e.Evict) != 0 {
				t.Fatalf("recovery merge = %+v: want z added to copies and dropped from evict", e)
			}
		}
	}
}

// A sensor's capture is pinned to its node and co-located with its
// consumer: without a copy, losing the node loses the capture for good.
// The policy copies it; the consumer's own (cheap to recompute) output is
// then recoverable from that copy and is not copied too.
func TestPlanAwareCopiesPinnedSourceAndNoCascade(t *testing.T) {
	tasks := []taskSpec{
		{Name: "source", Runtime: 0.3, DataSize: "1.6MB", Constraints: []string{"s"}, CPU: "1"},
		{Name: "prep", Runtime: 0.4, DataSize: "46.7MB", Runner: "glue", Dependencies: []string{"source"}, CPU: "1"},
		{Name: "embed", Runtime: 10, DataSize: "8.3MB", Dependencies: []string{"prep"}, CPU: "2"},
	}
	nodes := testNodes("s", "a", "b")
	m := testModel(tasks, map[string]string{"source": "s", "prep": "s", "embed": "s"}, nodes,
		linkTable([2]string{"s", "a"}, [2]string{"s", "b"}))
	entries, ds := planAwareCopies(m, defaultRealizationPolicy(), nil, policyContext{})

	src, _ := decisionFor(ds, "source", "prep")
	if !src.Copy || src.DelayS != -1 || src.Fallback == "" {
		t.Fatalf("source decision = %+v, want a copy of an unrecoverable input", src)
	}
	if d, _ := decisionFor(ds, "prep", "embed"); d.Copy {
		t.Fatalf("prep decision = %+v: recomputable from the source copy, should not be copied", d)
	}
	if len(entries) != 1 || entries[0].Object != "source" {
		t.Fatalf("entries = %+v, want only the source copied", entries)
	}
}

// The static rule copies every consumed object off its producer's node, by
// hash, whatever the plan: the same nodes for the same object every time.
func TestStaticCopiesArePlanBlindAndStable(t *testing.T) {
	tasks := []taskSpec{
		{Name: "p", Runtime: 1, DataSize: "8MB", CPU: "1"},
		{Name: "c", Runtime: 1, Dependencies: []string{"p"}, CPU: "1"},
		{Name: "d", Runtime: 1, Dependencies: []string{"c"}, CPU: "1"},
	}
	nodes := testNodes("a", "b", "x", "y")
	pol := defaultRealizationPolicy()
	pol.Mode, pol.Replicas = policyStatic, 3
	m1 := testModel(tasks, map[string]string{"p": "a", "c": "a", "d": "b"}, nodes, nil)
	m2 := testModel(tasks, map[string]string{"p": "a", "c": "x", "d": "y"}, nodes, nil)
	e1, _ := staticCopies(m1, pol, nil)
	e2, _ := staticCopies(m2, pol, nil)
	if len(e1) != 2 || e1[0].Origin != originStatic {
		t.Fatalf("entries = %+v, want both consumed objects copied", e1)
	}
	for _, e := range e1 {
		if len(e.Copies) != 2 || contains(e.Copies, m1.assign[producerOf(e.Object)]) {
			t.Fatalf("%s: copies %v, want 2 off the producer's node", e.Object, e.Copies)
		}
	}
	// p's placement is the same in both plans, so its copies are too,
	// although its consumer moved.
	if e1[0].Object != "p" || e2[0].Object != "p" || strings.Join(e1[0].Copies, ",") != strings.Join(e2[0].Copies, ",") {
		t.Fatalf("static copies of p depend on the plan: %v vs %v", e1[0].Copies, e2[0].Copies)
	}
}

func TestReapplicationSkipsStartedConsumersAndKeepsEarlierCopies(t *testing.T) {
	tasks := []taskSpec{
		{Name: "embed", Runtime: 12, DataSize: "8.3MB", CPU: "1"},
		{Name: "fuse", Runtime: 2, Dependencies: []string{"embed"}, CPU: "1"},
	}
	nodes := testNodes("a", "b", "c")
	m := testModel(tasks, map[string]string{"embed": "a", "fuse": "a"}, nodes, nil)
	ctx := policyContext{skip: map[string]bool{"fuse": true}}
	if entries, _ := planAwareCopies(m, defaultRealizationPolicy(), nil, ctx); len(entries) != 0 {
		t.Fatalf("entries = %+v: a started consumer needs no protection", entries)
	}
	earlier := []realizationEntry{{Object: "embed", Copies: []string{"c"}, Origin: originPlan}}
	got := keepEarlierCopies(earlier, []realizationEntry{{Object: "embed", Copies: []string{"b"}, Origin: originPlan}}, originPlan)
	if len(got) != 1 || strings.Join(got[0].Copies, ",") != "b,c" {
		t.Fatalf("got %+v, want the earlier copy kept alongside the new one", got)
	}
}

// Two pinned captures whose consumers would recover equally well on c or d:
// their copies go to different nodes, not both to the first.
func TestPlanAwareCopiesSpreadAcrossEqualFallbacks(t *testing.T) {
	tasks := []taskSpec{
		{Name: "s1", Runtime: 1, DataSize: "100MB", Constraints: []string{"a"}, CPU: "1"},
		{Name: "u1", Runtime: 10, Dependencies: []string{"s1"}, CPU: "1"},
		{Name: "s2", Runtime: 1, DataSize: "100MB", Constraints: []string{"b"}, CPU: "1"},
		{Name: "u2", Runtime: 10, Dependencies: []string{"s2"}, CPU: "1"},
	}
	nodes := testNodes("a", "b", "c", "d")
	// a and b reach each other slowly, so neither is the other's fallback.
	m := testModel(tasks, map[string]string{"s1": "a", "u1": "a", "s2": "b", "u2": "b"}, nodes,
		linkTable([2]string{"a", "b"}))
	entries, _ := planAwareCopies(m, defaultRealizationPolicy(), nil, policyContext{})
	got := map[string]string{}
	for _, e := range entries {
		if len(e.Copies) != 1 {
			t.Fatalf("%s: copies %v, want one", e.Object, e.Copies)
		}
		got[e.Object] = e.Copies[0]
	}
	if len(got) != 2 || got["s1"] == got["s2"] {
		t.Fatalf("copies %v: want the two captures on different fallbacks", got)
	}
}

// A copy protects only once it lands. 100 MB over a B/8 link takes about
// 7 s of a 20 s exposure: a 12 s producer is not worth it, a 30 s one is.
func TestPlanAwareCopiesWeighTheLandingWindow(t *testing.T) {
	for _, tc := range []struct {
		runtime float64
		copy    bool
	}{{12, false}, {30, true}} {
		tasks := []taskSpec{
			{Name: "p", Runtime: tc.runtime, RuntimeProfile: map[string]float64{"b": tc.runtime * 4 / 3, "x": tc.runtime * 4 / 3},
				DataSize: "100MB", CPU: "1"},
			{Name: "c", Runtime: 20, Dependencies: []string{"p"}, CPU: "1"},
		}
		nodes := testNodes("a", "b", "x")
		m := testModel(tasks, map[string]string{"p": "a", "c": "a"}, nodes,
			linkTable([2]string{"a", "b"}, [2]string{"a", "x"}))
		_, ds := planAwareCopies(m, defaultRealizationPolicy(), nil, policyContext{})
		d, _ := decisionFor(ds, "p", "c")
		if d.Copy != tc.copy {
			t.Fatalf("runtime %v: decision %+v, want copy=%v", tc.runtime, d, tc.copy)
		}
		if d.Protected < 0.6 || d.Protected > 0.7 {
			t.Fatalf("runtime %v: protected share %v, want about (20-7.1)/20", tc.runtime, d.Protected)
		}
	}
}

// After a repair moved c next to the only surviving copy of p's output (p's
// node lost), re-protection copies it again; the lost node is neither a
// holder nor a fallback.
func TestReprotectAfterRepairIgnoresTheLostNode(t *testing.T) {
	tasks := []taskSpec{
		{Name: "p", Runtime: 12, DataSize: "8MB", CPU: "1"},
		{Name: "c", Runtime: 20, Dependencies: []string{"p"}, CPU: "1"},
	}
	nodes := testNodes("a", "f", "g")
	planned := testModel(tasks, map[string]string{"p": "a", "c": "a"}, nodes, nil)
	planned.policy = defaultRealizationPolicy()
	next := map[string]nodeInfo{"p": nodes["a"], "c": nodes["f"]}
	m := repairedModel(tasks, planned, next, map[string]bool{"a": true})
	if _, ok := m.nodes["a"]; ok {
		t.Fatal("the lost node is back in the repaired model")
	}
	entries, _ := planAwareCopies(m, defaultRealizationPolicy(), nil, policyContext{
		holders: map[string][]string{"p": {"f"}}, skip: map[string]bool{"p": true}})
	if len(entries) != 1 || len(entries[0].Copies) != 1 || entries[0].Copies[0] != "g" {
		t.Fatalf("entries = %+v, want p re-copied to g", entries)
	}
}
