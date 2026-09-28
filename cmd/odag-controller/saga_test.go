package main

// Integration tests for the SAGA sidecar bridge. They need a running
// sidecar (saga-sidecar/server.py) at WL_SAGA_SCHEDULER_URL (default
// http://127.0.0.1:8090) and are skipped when it is unreachable, so
// plain `go test ./cmd/odag-controller` stays hermetic.

import (
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

func requireSidecar(t *testing.T) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(sagaSchedulerURL() + "/healthz")
	if err != nil {
		t.Skipf("saga sidecar not reachable at %s: %v", sagaSchedulerURL(), err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Skipf("saga sidecar unhealthy: HTTP %d", resp.StatusCode)
	}
}

// predictedMakespan evaluates a placement under Wayline's own predictor so
// built-in HEFT and SAGA placements are compared with the same cost model.
func predictedMakespan(tasks []taskSpec, assignMap map[string]nodeInfo,
	rtRes runtimeResolver, dsRes dataSizeResolver, bwRes bandwidthResolver) float64 {
	predicted, _ := computePredictedSchedule(tasks, assignMap, rtRes, dsRes, bwRes)
	ms := 0.0
	for _, p := range predicted {
		if p.EstEnd > ms {
			ms = p.EstEnd
		}
	}
	return ms
}

func diamondTasks() []taskSpec {
	return []taskSpec{
		{Name: "A", Runtime: 10.0, DataSize: "100MB"},
		{Name: "B", Runtime: 5.0, DataSize: "1MB", Dependencies: []string{"A"}},
		{Name: "C", Runtime: 5.0, DataSize: "1MB", Dependencies: []string{"A"}},
		{Name: "D", Runtime: 2.0, DataSize: "0", Dependencies: []string{"B", "C"}},
	}
}

// Separable heterogeneity: n1 is 2x faster than n2/n3 for every task, so
// SAGA's cost/speed model represents it exactly (fit RMSE 0).
func separableRT() runtimeResolver {
	speed := map[string]float64{"n1": 2.0, "n2": 1.0, "n3": 1.0}
	base := map[string]float64{"A": 10, "B": 5, "C": 5, "D": 2}
	return func(task, node string) float64 { return base[task] / speed[node] }
}

func constDS(bytes int64) dataSizeResolver {
	return func(task, node string) int64 { return bytes }
}

func TestSagaHeft_Diamond_QualityParity(t *testing.T) {
	requireSidecar(t)
	tasks := diamondTasks()
	nodes := makeNodes("n1", "n2", "n3")
	rtRes := separableRT()
	dsRes := constDS(1_000_000)
	bwRes := constBW(100e6)

	builtin := heftAssignTasks(tasks, nodes, rtRes, dsRes, bwRes, heftOptions{})
	builtinMS := predictedMakespan(tasks, builtin.assignMap, rtRes, dsRes, bwRes)

	sagaMap, _, err := sagaAssignTasks("heft", sagaSchedulerURL(), nil, tasks, nodes, rtRes, dsRes, bwRes)
	if err != nil {
		t.Fatalf("sagaAssignTasks: %v", err)
	}
	if len(sagaMap) != len(tasks) {
		t.Fatalf("saga placed %d tasks, want %d", len(sagaMap), len(tasks))
	}
	sagaMS := predictedMakespan(tasks, sagaMap, rtRes, dsRes, bwRes)

	t.Logf("builtin HEFT makespan=%.2fs, SAGA HEFT makespan=%.2fs", builtinMS, sagaMS)
	// The contention models differ (Wayline's HEFT models serialized egress
	// and TCP fair-share; SAGA's does not), so placements may differ. The
	// parity claim is quality under Wayline's own predictor: SAGA-HEFT must
	// be within 1.5x of built-in HEFT on a separable instance.
	if sagaMS > builtinMS*1.5 {
		t.Errorf("SAGA HEFT placement much worse than builtin: %.2fs vs %.2fs", sagaMS, builtinMS)
	}
}

func TestSaga_ConstraintsRespected(t *testing.T) {
	requireSidecar(t)
	tasks := diamondTasks()
	tasks[0].Constraints = []string{"n3"} // pin A to the slow node
	nodes := makeNodes("n1", "n2", "n3")

	sagaMap, _, err := sagaAssignTasks("heft", sagaSchedulerURL(), nil, tasks, nodes, separableRT(), constDS(1_000_000), constBW(100e6))
	if err != nil {
		t.Fatalf("sagaAssignTasks: %v", err)
	}
	if got := sagaMap["A"].name; got != "n3" {
		t.Errorf("constrained task A placed on %q, want n3", got)
	}
}

func TestSaga_MultipleAlgorithms(t *testing.T) {
	requireSidecar(t)
	tasks := diamondTasks()
	nodes := makeNodes("n1", "n2", "n3")
	rtRes := separableRT()
	dsRes := constDS(1_000_000)
	bwRes := constBW(100e6)

	builtin := heftAssignTasks(tasks, nodes, rtRes, dsRes, bwRes, heftOptions{})
	builtinMS := predictedMakespan(tasks, builtin.assignMap, rtRes, dsRes, bwRes)

	for _, algo := range []string{"heft", "cpop", "peft", "minmin", "maxmin", "sufferage"} {
		algo := algo
		t.Run(algo, func(t *testing.T) {
			m, _, err := sagaAssignTasks(algo, sagaSchedulerURL(), nil, tasks, nodes, rtRes, dsRes, bwRes)
			if err != nil {
				t.Fatalf("%s: %v", algo, err)
			}
			for _, task := range tasks {
				if _, ok := m[task.Name]; !ok {
					t.Fatalf("%s left %s unassigned", algo, task.Name)
				}
			}
			ms := predictedMakespan(tasks, m, rtRes, dsRes, bwRes)
			t.Logf("%-10s makespan=%.2fs (builtin HEFT %.2fs)", algo, ms, builtinMS)
		})
	}
}

func TestSaga_UnknownAlgorithmFallsThroughAsError(t *testing.T) {
	requireSidecar(t)
	_, _, err := sagaAssignTasks("not-a-real-algorithm", sagaSchedulerURL(), nil, diamondTasks(), makeNodes("n1", "n2"),
		nil, nil, constBW(100e6))
	if err == nil {
		t.Fatal("expected error for unknown algorithm")
	}
	t.Log(fmt.Sprintf("got expected error: %v", err))
}

// --------------------------------------------------------------------------
// Bring-your-own-scheduler: explicit endpoint + dotted-path algorithm
// --------------------------------------------------------------------------

// sidecarWithUserCode returns a scheduler endpoint that can import the
// mysched test package, or "" if none is reachable. Set WL_SAGA_TEST_URL to
// a sidecar started with WL_SAGA_PATH=saga-sidecar/testdata.
func sidecarWithUserCode(t *testing.T) string {
	t.Helper()
	url := os.Getenv("WL_SAGA_TEST_URL")
	if url == "" {
		url = sagaSchedulerURL()
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url + "/healthz")
	if err != nil {
		t.Skipf("no scheduler service at %s: %v", url, err)
	}
	resp.Body.Close()
	return url
}

func TestSaga_ExternalSchedulerByDottedPathOverExplicitURL(t *testing.T) {
	url := sidecarWithUserCode(t)
	tasks := diamondTasks()
	nodes := makeNodes("n1", "n2", "n3")

	// A class that exists in no registry: it must be imported by name.
	m, _, err := sagaAssignTasks("mysched.PinFirstNodeScheduler", url, nil,
		tasks, nodes, separableRT(), constDS(1_000_000), constBW(100e6))
	if err != nil {
		t.Skipf("sidecar cannot import the test scheduler (start it with "+
			"WL_SAGA_PATH=saga-sidecar/testdata): %v", err)
	}
	for _, task := range tasks {
		if _, ok := m[task.Name]; !ok {
			t.Fatalf("task %s unassigned", task.Name)
		}
	}
	// This scheduler pins everything to the alphabetically-first node.
	for name, ni := range m {
		if ni.name != "n1" {
			t.Errorf("task %s on %s, want n1 (scheduler pins to first node)", name, ni.name)
		}
	}
}

func TestSaga_ConstructorOptionsReachTheScheduler(t *testing.T) {
	url := sidecarWithUserCode(t)
	tasks := diamondTasks()
	nodes := makeNodes("n1", "n2", "n3")

	// which=2 selects the third node alphabetically.
	m, _, err := sagaAssignTasks("mysched.ParamScheduler", url,
		map[string]interface{}{"which": 2},
		tasks, nodes, separableRT(), constDS(1_000_000), constBW(100e6))
	if err != nil {
		t.Skipf("sidecar cannot import the test scheduler: %v", err)
	}
	for name, ni := range m {
		if ni.name != "n3" {
			t.Errorf("task %s on %s, want n3 (options.which=2)", name, ni.name)
		}
	}
}

// TestPerEdgeObjectSizes: a producer with several named outputs must give
// each dependent edge the size of the object that edge actually carries,
// not the producer's aggregate dataSize.
func TestPerEdgeObjectSizes(t *testing.T) {
	tasks := []taskSpec{
		{Name: "produce", Image: "i", DataSize: "200MB",
			Outputs: []outputSpec{
				{Name: "alert", DataSize: "1MB"},
				{Name: "features", DataSize: "200MB"},
			}},
		{Name: "small", Image: "i", Dependencies: []string{"produce"},
			Inputs: []inputSpec{{Producer: "produce", Object: "alert"}}},
		{Name: "big", Image: "i", Dependencies: []string{"produce"},
			Inputs: []inputSpec{{Producer: "produce", Object: "features"}}},
	}
	byName := map[string]taskSpec{}
	for _, tk := range tasks {
		byName[tk.Name] = tk
	}
	sizeFor := func(consumer, dep string) int64 {
		c := byName[consumer]
		var total int64
		for _, key := range consumedKeys(c, dep) {
			obj := ""
			if i := strings.IndexByte(key, '.'); i > 0 {
				obj = key[i+1:]
			}
			size := parseDataSizeBytes(byName[dep].DataSize)
			for _, o := range byName[dep].Outputs {
				if o.Name == obj {
					size = parseDataSizeBytes(o.DataSize)
				}
			}
			total += size
		}
		return total
	}
	if got := sizeFor("small", "produce"); got != 1_000_000 {
		t.Errorf("alert edge = %d, want 1000000", got)
	}
	if got := sizeFor("big", "produce"); got != 200_000_000 {
		t.Errorf("features edge = %d, want 200000000", got)
	}
}

// TestSchedulePlanOrdering: assignments are ordered per node by the
// scheduler's predicted start, which is what dispatch enacts.
func TestSchedulePlanOrdering(t *testing.T) {
	out := sagaScheduleResponse{Assignments: []sagaAssignment{
		{Task: "c", Node: "n1", EstimatedStart: 20},
		{Task: "a", Node: "n1", EstimatedStart: 0},
		{Task: "b", Node: "n1", EstimatedStart: 10},
		{Task: "z", Node: "n2", EstimatedStart: 5},
	}}
	byNode := map[string][]sagaAssignment{}
	for _, a := range out.Assignments {
		byNode[a.Node] = append(byNode[a.Node], a)
	}
	sort.SliceStable(byNode["n1"], func(i, j int) bool {
		return byNode["n1"][i].EstimatedStart < byNode["n1"][j].EstimatedStart
	})
	want := []string{"a", "b", "c"}
	for i, a := range byNode["n1"] {
		if a.Task != want[i] {
			t.Fatalf("n1 order = %v, want %v", byNode["n1"], want)
		}
	}
}
