package scheduler

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// DefaultBandwidthBytesPerSec is used when no bandwidth matrix is provided.
// 100 Mbps = 12.5 MB/s.
const DefaultBandwidthBytesPerSec float64 = 12_500_000

// Task is a node in the DAG.
type Task struct {
	Name         string
	Dependencies []string
	Runtime      float64  // expected computation time in seconds
	DataSize     int64    // output data size in bytes (used for comm cost)
	Constraints  []string // allowed node names; empty = any node
}

// Node represents a schedulable cluster node.
type Node struct {
	Name string
}

// Assignment maps a task to its scheduled node with estimated times.
type Assignment struct {
	Task            string
	Node            string
	EstimatedStart  float64
	EstimatedFinish float64
}

// Schedule is the result of the HEFT algorithm.
type Schedule struct {
	Assignments       []Assignment
	EstimatedMakespan float64
}

// Compute runs the HEFT (Heterogeneous Earliest Finish Time) algorithm.
//
// Reference: Topcuoglu, Hariri & Wu (2002).
//
// Tasks are sorted by descending upward rank and assigned to the node that
// minimises Earliest Finish Time (EFT), respecting any node constraints.
func Compute(tasks []Task, nodes []Node, bwBytesPerSec float64) Schedule {
	if bwBytesPerSec <= 0 {
		bwBytesPerSec = DefaultBandwidthBytesPerSec
	}
	if len(nodes) == 0 || len(tasks) == 0 {
		return Schedule{}
	}

	taskMap := make(map[string]*Task, len(tasks))
	for i := range tasks {
		taskMap[tasks[i].Name] = &tasks[i]
	}

	// Average computation cost per task (uniform across nodes for now;
	// extend with per-node compute weights if node heterogeneity is known).
	avgComp := make(map[string]float64, len(tasks))
	for _, t := range tasks {
		if t.Runtime > 0 {
			avgComp[t.Name] = t.Runtime
		} else {
			avgComp[t.Name] = 10.0 // default 10s if not specified
		}
	}

	// Upward rank: rank(t) = avgComp(t) + max over successors s of (avgComm(t→s) + rank(s))
	ranks := make(map[string]float64, len(tasks))
	var calcRank func(name string) float64
	calcRank = func(name string) float64 {
		if r, ok := ranks[name]; ok {
			return r
		}
		task := taskMap[name]
		maxSucc := 0.0
		for _, other := range tasks {
			for _, dep := range other.Dependencies {
				if dep == name {
					comm := avgCommCost(task, bwBytesPerSec)
					r := comm + calcRank(other.Name)
					if r > maxSucc {
						maxSucc = r
					}
				}
			}
		}
		ranks[name] = avgComp[name] + maxSucc
		return ranks[name]
	}
	for _, t := range tasks {
		calcRank(t.Name)
	}

	// Sort tasks by descending rank (HEFT priority list).
	order := make([]string, len(tasks))
	for i, t := range tasks {
		order[i] = t.Name
	}
	sort.Slice(order, func(i, j int) bool {
		return ranks[order[i]] > ranks[order[j]]
	})

	// Assign each task to the node with the lowest EFT.
	nodeAvail := make(map[string]float64, len(nodes))
	taskFinish := make(map[string]float64, len(tasks))
	taskStart := make(map[string]float64, len(tasks))
	assigned := make(map[string]string, len(tasks))

	for _, name := range order {
		task := taskMap[name]
		allowed := allowedNodes(*task, nodes)
		if len(allowed) == 0 {
			allowed = nodes
		}

		bestNode, bestSt, bestFt := "", math.MaxFloat64, math.MaxFloat64

		for _, node := range allowed {
			avail := nodeAvail[node.Name]

			// Data-ready time: max over all predecessors.
			dataReady := 0.0
			for _, dep := range task.Dependencies {
				depFinish := taskFinish[dep]
				comm := 0.0
				if assigned[dep] != node.Name {
					if depTask := taskMap[dep]; depTask.DataSize > 0 {
						comm = float64(depTask.DataSize) / bwBytesPerSec
					}
				}
				t := depFinish + comm
				if t > dataReady {
					dataReady = t
				}
			}

			st := math.Max(avail, dataReady)
			ft := st + avgComp[name]

			if ft < bestFt {
				bestNode, bestSt, bestFt = node.Name, st, ft
			}
		}

		assigned[name] = bestNode
		taskStart[name] = bestSt
		taskFinish[name] = bestFt
		nodeAvail[bestNode] = bestFt
	}

	makespan := 0.0
	result := make([]Assignment, 0, len(tasks))
	for _, t := range tasks {
		if taskFinish[t.Name] > makespan {
			makespan = taskFinish[t.Name]
		}
		result = append(result, Assignment{
			Task:            t.Name,
			Node:            assigned[t.Name],
			EstimatedStart:  taskStart[t.Name],
			EstimatedFinish: taskFinish[t.Name],
		})
	}

	return Schedule{Assignments: result, EstimatedMakespan: makespan}
}

// avgCommCost returns half the one-way communication cost for a task's output.
// (used in rank calculation as an average over all node pairs)
func avgCommCost(t *Task, bwBytesPerSec float64) float64 {
	if t.DataSize <= 0 {
		return 0
	}
	return float64(t.DataSize) / bwBytesPerSec * 0.5
}

// allowedNodes filters nodes by the task's constraint list.
func allowedNodes(t Task, nodes []Node) []Node {
	if len(t.Constraints) == 0 {
		return nodes
	}
	set := make(map[string]bool, len(t.Constraints))
	for _, n := range t.Constraints {
		set[n] = true
	}
	var result []Node
	for _, n := range nodes {
		if set[n.Name] {
			result = append(result, n)
		}
	}
	return result
}

// ParseDataSize parses strings like "100MB", "1GB", "512KB" to bytes.
func ParseDataSize(s string) int64 {
	s = strings.ToUpper(strings.TrimSpace(s))
	var n int64
	fmt.Sscanf(s, "%d", &n)
	switch {
	case strings.Contains(s, "GB"):
		return n * 1_000_000_000
	case strings.Contains(s, "MB"):
		return n * 1_000_000
	case strings.Contains(s, "KB"):
		return n * 1_000
	}
	return n
}
