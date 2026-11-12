package main

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

// A policy rebuilding the plan from its published form must decide with the
// same numbers the controller planned with.
func TestPublishedPlanRoundTrip(t *testing.T) {
	nodes := testNodes("a", "b", "c")
	tasks := []taskSpec{
		{Name: "p", Runtime: 10, DataSize: "8MB", RuntimeProfile: map[string]float64{"a": 4, "b": 12}},
		{Name: "q", Runtime: 6, DataSize: "2MB", Dependencies: []string{"p"}},
		{Name: "r", Runtime: 3, Dependencies: []string{"p", "q"}},
	}
	bw := func(src, dst string) float64 {
		if src == dst {
			return 0
		}
		if src == "a" && dst == "c" {
			return 10e6
		}
		return 100e6
	}
	m := testModel(tasks, map[string]string{"p": "a", "q": "b", "r": "c"}, nodes, bw)
	m.deployed = time.Unix(1700000000, 250000000)

	pub := buildPublishedPlan("ns/run", "uid-1", m)
	raw, err := json.Marshal(pub)
	if err != nil {
		t.Fatal(err)
	}
	var back publishedPlan
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if got := back.RuntimeS["q"]; len(got) != 1 || got["*"] != 6 {
		t.Fatalf("uniform runtime should collapse to *: %v", got)
	}
	r := modelFromPublished(&back, tasks, nodes)

	for _, tk := range []string{"p", "q", "r"} {
		for _, n := range []string{"a", "b", "c"} {
			if math.Abs(r.runtime(tk, n)-m.runtime(tk, n)) > 1e-3 {
				t.Errorf("runtime(%s,%s) = %v, planned %v", tk, n, r.runtime(tk, n), m.runtime(tk, n))
			}
			if r.dataSize(tk, n) != m.dataSize(tk, n) {
				t.Errorf("dataSize(%s,%s) = %v, planned %v", tk, n, r.dataSize(tk, n), m.dataSize(tk, n))
			}
		}
		if r.assign[tk] != m.assign[tk] || r.eft[tk] != m.eft[tk] || r.lst[tk] != m.lst[tk] {
			t.Errorf("%s: assign/eft/lst %s %v %v, planned %s %v %v", tk,
				r.assign[tk], r.eft[tk], r.lst[tk], m.assign[tk], m.eft[tk], m.lst[tk])
		}
	}
	for _, s := range []string{"a", "b", "c"} {
		for _, d := range []string{"a", "b", "c"} {
			if s != d && r.bw(s, d) != m.bw(s, d) {
				t.Errorf("bw(%s,%s) = %v, planned %v", s, d, r.bw(s, d), m.bw(s, d))
			}
		}
	}
	if d := r.deployed.Sub(m.deployed); d > time.Millisecond || d < -time.Millisecond {
		t.Errorf("deployed %v, planned %v", r.deployed, m.deployed)
	}
	if len(back.Bandwidth.Links) != 1 || back.Bandwidth.Links["a>c"] != 10e6 {
		t.Errorf("only the differing link should be listed: %v", back.Bandwidth.Links)
	}
}

// Observations read by the policy process drive the same lost-node and
// link-state functions the controller uses.
func TestApplyObservations(t *testing.T) {
	healthMu.Lock()
	healthOf = map[string]*nodeHealth{"x": {lost: true}}
	healthMu.Unlock()
	now := time.Now()
	applyObservations(observations{
		LostNodes: map[string]float64{"y": unixSeconds(now)},
		Links:     []observedLink{{Src: "a", Dst: "b", Degraded: true, Bps: 1e6, AtUnix: unixSeconds(now)}},
	})
	if nodeLost("x") || !nodeLost("y") {
		t.Fatalf("lost set not replaced: x=%v y=%v", nodeLost("x"), nodeLost("y"))
	}
	if !linkDegraded("a", "b", now) || linkDegraded("b", "a", now) {
		t.Fatalf("link state not applied")
	}
	if got := liveBandwidth(func(string, string) float64 { return 100e6 })("a", "b"); got != 1e6 {
		t.Fatalf("live bandwidth %v, want the observed 1e6", got)
	}
	applyObservations(observations{LostNodes: map[string]float64{}})
	if nodeLost("y") || linkDegraded("a", "b", now) {
		t.Fatalf("observations not cleared")
	}
}
