package main

import (
	"context"
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

type predictedTaskEntry struct {
	Name     string  `json:"name"`
	Node     string  `json:"node"`
	EstStart float64 `json:"estStart"` // seconds from t=0 (deployment time)
	EstEnd   float64 `json:"estEnd"`
}

type predictedFlowEntry struct {
	FromTask string  `json:"fromTask"`
	ToTask   string  `json:"toTask"`
	SrcNode  string  `json:"srcNode"`
	DstNode  string  `json:"dstNode"`
	Start    float64 `json:"start"`
	End      float64 `json:"end"`
	DataSize int64   `json:"dataSize"`
}

// computePredictedSchedule computes estimated start/end times for each task
// given a fixed node assignment. Uses resource-aware parallel execution:
// independent tasks on the same node can overlap if resources allow.
// Also returns naive per-edge flow timings (no contention model here —
// contention-aware flows only come from the HEFT scheduler).
func computePredictedSchedule(tasks []taskSpec, assignMap map[string]nodeInfo, rtResolver runtimeResolver, dsResolver dataSizeResolver, bwResolver bandwidthResolver) ([]predictedTaskEntry, []predictedFlowEntry) {
	taskByName := make(map[string]*taskSpec, len(tasks))
	for i := range tasks {
		taskByName[tasks[i].Name] = &tasks[i]
	}

	resolveRuntime := func(taskName, nodeName string) float64 {
		if rtResolver != nil {
			return rtResolver(taskName, nodeName)
		}
		return taskByName[taskName].Runtime
	}
	resolveDataSizeBytes := func(taskName, nodeName string) int64 {
		if dsResolver != nil {
			return dsResolver(taskName, nodeName)
		}
		return parseDataSizeBytes(taskByName[taskName].DataSize)
	}
	resolveBandwidth := func(src, dst string) float64 {
		if src == dst {
			return 0
		}
		if bwResolver != nil {
			return bwResolver(src, dst)
		}
		return heftDefaultBandwidth
	}

	// Build per-node resource timelines.
	timelines := make(map[string]*nodeTimeline)
	for _, ni := range assignMap {
		if _, ok := timelines[ni.name]; !ok {
			timelines[ni.name] = &nodeTimeline{
				totalCPU: ni.cpuMillis,
				totalMem: ni.memBytes,
			}
		}
	}

	taskFinish := make(map[string]float64)
	scheduled := make(map[string]bool)
	result := make([]predictedTaskEntry, 0, len(tasks))
	flows := make([]predictedFlowEntry, 0)

	// sourceQueueEnd[dep] is the end time of dep's last outgoing cross-node
	// transfer. The data-agent pushes to successors serially (one blocking
	// HTTP POST at a time), so sibling transfers from the same source task
	// are back-to-back, not parallel.
	sourceQueueEnd := make(map[string]float64)

	// Topological order via repeated passes.
	for len(result) < len(tasks) {
		progress := false
		for _, t := range tasks {
			if scheduled[t.Name] {
				continue
			}
			allDepsDone := true
			for _, dep := range t.Dependencies {
				if !scheduled[dep] {
					allDepsDone = false
					break
				}
			}
			if !allDepsDone {
				continue
			}

			nodeName := assignMap[t.Name].name
			tl := timelines[nodeName]

			// Compute depsReady: when all deps finish + comm cost. Cross-node
			// transfers from the same source task are serialized (the data-agent
			// pushes to successors one at a time), so each new transfer starts
			// at max(depFinish, end-of-dep's-previous-outgoing-transfer).
			depsReady := 0.0
			for _, dep := range t.Dependencies {
				depFinish := taskFinish[dep]
				depNode := assignMap[dep].name
				arrival := depFinish
				if depNode != nodeName {
					bytes := resolveDataSizeBytes(dep, depNode)
					bw := resolveBandwidth(depNode, nodeName)
					var commCost float64
					if bw > 0 {
						commCost = float64(bytes) / bw
					}
					if bytes > 0 {
						start := depFinish
						if qe := sourceQueueEnd[dep]; qe > start {
							start = qe
						}
						end := start + commCost
						sourceQueueEnd[dep] = end
						arrival = end
						flows = append(flows, predictedFlowEntry{
							FromTask: dep,
							ToTask:   t.Name,
							SrcNode:  depNode,
							DstNode:  nodeName,
							Start:    start,
							End:      end,
							DataSize: bytes,
						})
					}
				}
				if arrival > depsReady {
					depsReady = arrival
				}
			}

			// Find earliest start with available resources.
			taskCPU := parseTaskCPUMillis(t.CPU)
			taskMem := parseTaskMemBytes(t.Memory)
			runtime := resolveRuntime(t.Name, nodeName)
			est := tl.earliestStart(taskCPU, taskMem, runtime, depsReady)
			eft := est + runtime

			tl.commit(t.Name, est, eft, taskCPU, taskMem)
			taskFinish[t.Name] = eft
			scheduled[t.Name] = true
			progress = true
			result = append(result, predictedTaskEntry{
				Name:     t.Name,
				Node:     nodeName,
				EstStart: est,
				EstEnd:   eft,
			})
		}
		if !progress {
			break
		}
	}
	return result, flows
}

func writePredictedSchedule(dynClient dynamic.Interface, namespace, odagName string, predicted []predictedTaskEntry, flows []predictedFlowEntry, scheduling map[string]any) {
	if flows == nil {
		flows = []predictedFlowEntry{}
	}
	patch := map[string]any{
		"status": map[string]any{
			"predictedTasks":        predicted,
			"predictedNetworkFlows": flows,
			"scheduling":            scheduling,
		},
	}
	data, _ := json.Marshal(patch)
	_, _ = dynClient.Resource(odagGVR).Namespace(namespace).Patch(
		context.Background(), odagName, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	)
}
