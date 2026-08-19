package main

import "testing"

func TestConstraintOnly_RespectsNodeNames(t *testing.T) {
	tasks := []taskSpec{
		{Name: "pinned", Constraints: []string{"n3"}},
		{Name: "tier", Constraints: []string{"n2", "n3"}},
		{Name: "free"},
	}
	nodes := makeNodes("n1", "n2", "n3")
	m := constraintOnlyAssign(tasks, nodes)

	if m["pinned"].name != "n3" {
		t.Errorf("pinned task on %s, want n3", m["pinned"].name)
	}
	if n := m["tier"].name; n != "n2" && n != "n3" {
		t.Errorf("tier task on %s, want n2 or n3", n)
	}
	if _, ok := m["free"]; !ok {
		t.Error("unconstrained task left unplaced")
	}
}

func TestConstraintOnly_IsDeterministic(t *testing.T) {
	tasks := []taskSpec{{Name: "a"}, {Name: "b"}, {Name: "c"}, {Name: "d"}}
	nodes := makeNodes("n1", "n2", "n3")
	first := constraintOnlyAssign(tasks, nodes)
	for i := 0; i < 20; i++ {
		again := constraintOnlyAssign(tasks, nodes)
		for name, ni := range first {
			if again[name].name != ni.name {
				t.Fatalf("run %d: task %s moved %s -> %s", i, name, ni.name, again[name].name)
			}
		}
	}
}

func TestConstraintOnly_SpreadsRatherThanPacks(t *testing.T) {
	// Four unconstrained tasks over three nodes must not all land on one:
	// the neutral placement should not accidentally serialize a parallel tier.
	tasks := []taskSpec{{Name: "a"}, {Name: "b"}, {Name: "c"}, {Name: "d"}}
	m := constraintOnlyAssign(tasks, makeNodes("n1", "n2", "n3"))
	used := map[string]bool{}
	for _, ni := range m {
		used[ni.name] = true
	}
	if len(used) != 3 {
		t.Errorf("4 tasks spread over %d node(s), want 3", len(used))
	}
}

func TestConstraintOnly_UnschedulableConstraintFallsBack(t *testing.T) {
	// Constraint naming a node that is not in the cluster must not drop the task.
	tasks := []taskSpec{{Name: "orphan", Constraints: []string{"gone"}}}
	m := constraintOnlyAssign(tasks, makeNodes("n1", "n2"))
	if _, ok := m["orphan"]; !ok {
		t.Error("task with unschedulable constraint was dropped")
	}
}
