package main

// Integration tests for the SAGA sidecar bridge. They need a running
// sidecar (saga-sidecar/server.py) at WL_SAGA_SCHEDULER_URL (default
// http://127.0.0.1:8090) and are skipped when it is unreachable, so
// plain `go test ./cmd/odag-controller` stays hermetic.

import (
	"fmt"
	"net/http"
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

	sagaMap, err := sagaAssignTasks("heft", tasks, nodes, rtRes, dsRes, bwRes)
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

	sagaMap, err := sagaAssignTasks("heft", tasks, nodes, separableRT(), constDS(1_000_000), constBW(100e6))
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
			m, err := sagaAssignTasks(algo, tasks, nodes, rtRes, dsRes, bwRes)
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
	_, err := sagaAssignTasks("not-a-real-algorithm", diamondTasks(), makeNodes("n1", "n2"),
		nil, nil, constBW(100e6))
	if err == nil {
		t.Fatal("expected error for unknown algorithm")
	}
	t.Log(fmt.Sprintf("got expected error: %v", err))
}
