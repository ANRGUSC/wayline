package main

import (
	"math"
	"testing"
)

// testNodes builds nodes with 4 CPUs and 8 GiB each.
func testNodes(names ...string) map[string]nodeInfo {
	out := map[string]nodeInfo{}
	for i, n := range names {
		out[n] = nodeInfo{name: n, ip: "10.0.0." + string(rune('1'+i)), cpuMillis: 4000, memBytes: 8 << 30}
	}
	return out
}

// testModel places tasks as given and predicts the schedule with a uniform
// bandwidth unless bw is set.
func testModel(tasks []taskSpec, place map[string]string, nodes map[string]nodeInfo,
	bw func(src, dst string) float64) *planModel {

	assign := map[string]nodeInfo{}
	for t, n := range place {
		assign[t] = nodes[n]
	}
	var res bandwidthResolver
	if bw != nil {
		res = bw
	} else {
		res = func(src, dst string) float64 {
			if src == dst {
				return 0
			}
			return 100e6
		}
	}
	rt := func(task, node string) float64 {
		for _, t := range tasks {
			if t.Name == task {
				if v, ok := t.RuntimeProfile[node]; ok {
					return v
				}
				return t.Runtime
			}
		}
		return 0
	}
	predicted, flows := computePredictedSchedule(tasks, assign, rt, nil, res)
	return newPlanModel(tasks, assign, nodes, rt, nil, res, predicted, flows)
}

func TestPlanModelObjectsAndSizes(t *testing.T) {
	tasks := []taskSpec{
		{Name: "a", Runtime: 1, DataSize: "10MB", Outputs: []outputSpec{{Name: "x", DataSize: "4MB"}, {Name: "y", DataSize: "6MB"}}},
		{Name: "b", Runtime: 1, Dependencies: []string{"a"}, Inputs: []inputSpec{{Producer: "a", Object: "x"}}},
		{Name: "c", Runtime: 1, Dependencies: []string{"a"}, Inputs: []inputSpec{{Producer: "a", Object: "y"}}},
		{Name: "d", Runtime: 1, DataSize: "2MB", Dependencies: []string{"b", "c"}},
	}
	nodes := testNodes("n1", "n2")
	m := testModel(tasks, map[string]string{"a": "n1", "b": "n1", "c": "n2", "d": "n2"}, nodes, nil)

	if got := m.objects(); len(got) != 4 || got[0] != "a.x" && got[0] != "a.y" {
		t.Fatalf("objects = %v", got)
	}
	if got := m.consumersOf("a.y"); len(got) != 1 || got[0] != "c" {
		t.Fatalf("consumersOf(a.y) = %v", got)
	}
	if got := m.objectBytes("a.x"); got != 4_000_000 {
		t.Fatalf("objectBytes(a.x) = %d", got)
	}
	if got := m.plannedHolders("a.y"); len(got) != 2 {
		t.Fatalf("plannedHolders(a.y) = %v, want producer and consumer nodes", got)
	}
	if got := m.plannedHolders("a.x"); len(got) != 1 || got[0] != "n1" {
		t.Fatalf("plannedHolders(a.x) = %v, want only n1 (co-located)", got)
	}
}

func TestPlanModelSlack(t *testing.T) {
	// a -> {long, short} -> join on one node: short has slack long-short.
	tasks := []taskSpec{
		{Name: "a", Runtime: 1},
		{Name: "long", Runtime: 10, Dependencies: []string{"a"}},
		{Name: "short", Runtime: 3, Dependencies: []string{"a"}},
		{Name: "join", Runtime: 1, Dependencies: []string{"long", "short"}},
	}
	nodes := testNodes("n1")
	m := testModel(tasks, map[string]string{"a": "n1", "long": "n1", "short": "n1", "join": "n1"}, nodes, nil)
	if s := m.slack("long"); math.Abs(s) > 1e-9 {
		t.Fatalf("slack(long) = %v, want 0 (critical path)", s)
	}
	if s := m.slack("short"); math.Abs(s-7) > 1e-9 {
		t.Fatalf("slack(short) = %v, want 7", s)
	}
	if m.makespan != 12 {
		t.Fatalf("makespan = %v, want 12", m.makespan)
	}
}

func TestBestNodeRespectsConstraintsAndExclusions(t *testing.T) {
	tasks := []taskSpec{
		{Name: "a", Runtime: 1, RuntimeProfile: map[string]float64{"n1": 5, "n2": 1, "n3": 2}},
		{Name: "pinned", Runtime: 1, Constraints: []string{"n1"}},
	}
	nodes := testNodes("n1", "n2", "n3")
	m := testModel(tasks, map[string]string{"a": "n1", "pinned": "n1"}, nodes, nil)
	tl := m.timelines(map[string]bool{"a": true})
	none := func(string, string) float64 { return 0 }

	if n, _ := m.bestNode("a", nil, tl, 0, none); n != "n2" {
		t.Fatalf("bestNode(a) = %s, want n2 (fastest)", n)
	}
	if n, _ := m.bestNode("a", map[string]bool{"n2": true}, tl, 0, none); n != "n3" {
		t.Fatalf("bestNode(a) without n2 = %s, want n3", n)
	}
	if n, eft := m.bestNode("pinned", map[string]bool{"n1": true}, tl, 0, none); n != "" || !math.IsInf(eft, 1) {
		t.Fatalf("pinned task off its only node: got %s, %v", n, eft)
	}
}

func TestStartOnUnknownCapacityAdmits(t *testing.T) {
	if got := startOn(&nodeTimeline{}, 1000, 1<<20, 5, 3); got != 3 {
		t.Fatalf("startOn on a node of unknown capacity = %v, want 3", got)
	}
	tl := &nodeTimeline{totalCPU: 1000, totalMem: 1 << 30}
	tl.commit("x", 0, 10, 1000, 0)
	if got := startOn(tl, 2000, 0, 1, 0); got != 10 {
		t.Fatalf("oversized task starts at %v, want 10 (alone, after x)", got)
	}
}
