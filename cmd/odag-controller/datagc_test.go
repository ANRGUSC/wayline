package main

import "testing"

func TestTemplateOfRun(t *testing.T) {
	cases := map[string]string{
		"e11-heft-warm-run-twmlr": "e11-heft-warm",
		"demo-iot-run-h4r98":      "demo-iot",
		"a-run-b-run-c":           "a-run-b",
		"no-template-here":        "",
	}
	for in, want := range cases {
		if got := templateOfRun(in); got != want {
			t.Errorf("templateOfRun(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEdgeBytesUsesConsumedNamedOutputs(t *testing.T) {
	prod := &taskSpec{Name: "p", DataSize: "1MB",
		Outputs: []outputSpec{{Name: "big", DataSize: "150MB"}, {Name: "small", DataSize: "2MB"}}}
	both := &taskSpec{Name: "c", Inputs: []inputSpec{{Producer: "p", Object: "big"}, {Producer: "p", Object: "small"}}}
	plain := &taskSpec{Name: "d", Dependencies: []string{"p"}}
	fallback := func() int64 { return 7 }
	if got := edgeBytes(prod, both, fallback); got != 152_000_000 {
		t.Errorf("named outputs: got %d, want 152000000", got)
	}
	if got := edgeBytes(prod, plain, fallback); got != 7 {
		t.Errorf("default output: got %d, want the fallback 7", got)
	}
}

// With 150 MB named edges over a 10 MB/s link, the second consumer must stay
// on the producer's node and wait for CPU there rather than move 15 s of data;
// sized as 1 MB per edge, HEFT used to move it.
func TestHeftSizesEdgesByNamedOutputs(t *testing.T) {
	tasks := []taskSpec{
		{Name: "p", Runtime: 1, CPU: "2", Constraints: []string{"a"},
			Outputs: []outputSpec{{Name: "to-c1", DataSize: "150MB"}, {Name: "to-c2", DataSize: "150MB"}}},
		{Name: "c1", Runtime: 10, CPU: "2", Dependencies: []string{"p"}, Inputs: []inputSpec{{Producer: "p", Object: "to-c1"}}},
		{Name: "c2", Runtime: 10, CPU: "2", Dependencies: []string{"p"}, Inputs: []inputSpec{{Producer: "p", Object: "to-c2"}}},
	}
	nodes := map[string]nodeInfo{
		"a": {name: "a", cpuMillis: 2000, memBytes: 4 << 30},
		"b": {name: "b", cpuMillis: 2000, memBytes: 4 << 30},
	}
	r := heftAssignTasks(tasks, nodes, nil, nil, constBW(10e6), heftOptions{})
	if r.assignMap["c1"].name != "a" || r.assignMap["c2"].name != "a" {
		t.Fatalf("consumers should stay with their 150 MB inputs on a: c1=%s c2=%s",
			r.assignMap["c1"].name, r.assignMap["c2"].name)
	}
}
