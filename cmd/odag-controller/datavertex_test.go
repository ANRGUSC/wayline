package main

import "testing"

func toy() []taskSpec {
	return []taskSpec{
		{Name: "a", Dependencies: nil},
		{Name: "store-a", Type: "data", Dependencies: []string{"a"}},
		{Name: "b", Dependencies: []string{"store-a"}},
		{Name: "c", Dependencies: []string{"a"}},
	}
}

func TestSuccessorCounts(t *testing.T) {
	got := successorCounts(toy())
	want := map[string]int{"a": 2, "store-a": 1}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("succs[%s] = %d, want %d", k, got[k], v)
		}
	}
	if got["b"] != 0 || got["c"] != 0 {
		t.Errorf("sinks must have 0 successors, got b=%d c=%d", got["b"], got["c"])
	}
}

func TestIsDataVertex(t *testing.T) {
	tasks := toy()
	succs := successorCounts(tasks)
	if !isDataVertex(tasks[1], succs["store-a"]) {
		t.Error("well-formed data vertex not recognized")
	}
	if isDataVertex(tasks[0], succs["a"]) {
		t.Error("compute task classified as data vertex")
	}
	// Malformed: data vertex with no successors falls back to pod.
	sink := taskSpec{Name: "s", Type: "data", Dependencies: []string{"a"}}
	if isDataVertex(sink, 0) {
		t.Error("sink data vertex must fall back to pod execution")
	}
	// Malformed: two dependencies.
	multi := taskSpec{Name: "m", Type: "data", Dependencies: []string{"a", "b"}}
	if isDataVertex(multi, 1) {
		t.Error("multi-dep data vertex must fall back to pod execution")
	}
}
