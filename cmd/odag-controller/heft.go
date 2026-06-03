package main

import (
	"log"
	"math"
	"sort"
)

// heftAssignTasks implements resource-aware HEFT scheduling.
//
// Unlike classical HEFT which assumes sequential execution on each node,
// this version models parallel execution: independent tasks on the same
// node can overlap as long as the node has sufficient CPU and memory.
//
// Steps:
//  1. Compute upward rank using average bandwidth/runtime (same as classic HEFT).
//  2. Sort tasks by decreasing rank (critical path first).
//  3. For each task in priority order, pick the node that minimises EFT:
//     EST(t,p) = max(depsReady, earliestResourceSlot(p, t.cpu, t.mem))
//     EFT(t,p) = EST(t,p) + runtime(t)
//
// Tasks with no dependency chain between them CAN overlap on the same node
// if resources allow. Tasks with a dependency always wait for the predecessor.

// runtimeResolver returns the expected runtime (seconds) for a task on a given node.
type runtimeResolver func(taskName, nodeName string) float64

// dataSizeResolver returns the expected output data size (bytes) for a task on a given node.
type dataSizeResolver func(taskName, nodeName string) int64

// nodeTimeline tracks resource commitments on a single node over time.
// Each scheduled task occupies a time interval consuming some CPU and memory.
type nodeTimeline struct {
	totalCPU int64 // allocatable millicores
	totalMem int64 // allocatable bytes
	slots    []timeSlot
}

type timeSlot struct {
	start, end float64
	cpuMillis  int64
	memBytes   int64
	taskName   string
}

// earliestStart finds the earliest time >= minStart where the node has enough
// free CPU and memory to run a task of the given duration.
func (nt *nodeTimeline) earliestStart(cpuNeed, memNeed int64, duration, minStart float64) float64 {
	if cpuNeed <= 0 && memNeed <= 0 {
		return minStart // no resource constraints
	}

	// Check if we can start at minStart.
	t := minStart
	for {
		if nt.canFit(t, t+duration, cpuNeed, memNeed) {
			return t
		}
		// Advance to the next slot boundary after t.
		nextBoundary := nt.nextBoundaryAfter(t)
		if nextBoundary <= t {
			return t // no more boundaries — must be free
		}
		t = nextBoundary
	}
}

// canFit checks if a task with given resource needs can fit in [start, end]
// without exceeding the node's capacity at any point.
func (nt *nodeTimeline) canFit(start, end float64, cpuNeed, memNeed int64) bool {
	for _, s := range nt.slots {
		// Check overlap.
		if s.end <= start || s.start >= end {
			continue // no overlap
		}
		// This slot overlaps with [start, end]. Check resource capacity.
		// We need: existing usage + new task <= total capacity.
		// Simplification: check peak usage at the overlap region.
		usedCPU := nt.cpuUsedAt((max(s.start, start) + min(s.end, end)) / 2)
		usedMem := nt.memUsedAt((max(s.start, start) + min(s.end, end)) / 2)
		if usedCPU+cpuNeed > nt.totalCPU || usedMem+memNeed > nt.totalMem {
			return false
		}
	}
	// Also check that just our task alone fits.
	return cpuNeed <= nt.totalCPU && memNeed <= nt.totalMem
}

// cpuUsedAt returns total CPU committed at time t.
func (nt *nodeTimeline) cpuUsedAt(t float64) int64 {
	total := int64(0)
	for _, s := range nt.slots {
		if s.start <= t && t < s.end {
			total += s.cpuMillis
		}
	}
	return total
}

// memUsedAt returns total memory committed at time t.
func (nt *nodeTimeline) memUsedAt(t float64) int64 {
	total := int64(0)
	for _, s := range nt.slots {
		if s.start <= t && t < s.end {
			total += s.memBytes
		}
	}
	return total
}

// nextBoundaryAfter returns the next slot start or end time strictly after t.
func (nt *nodeTimeline) nextBoundaryAfter(t float64) float64 {
	best := t + 1e9 // sentinel
	for _, s := range nt.slots {
		if s.start > t && s.start < best {
			best = s.start
		}
		if s.end > t && s.end < best {
			best = s.end
		}
	}
	return best
}

// commit adds a task to the timeline.
func (nt *nodeTimeline) commit(taskName string, start, end float64, cpuMillis, memBytes int64) {
	nt.slots = append(nt.slots, timeSlot{
		start: start, end: end,
		cpuMillis: cpuMillis, memBytes: memBytes,
		taskName: taskName,
	})
}

// --------------------------------------------------------------------------
// Network flow timeline — TCP fair-sharing bandwidth contention model
// --------------------------------------------------------------------------
//
// When multiple data transfers share a node's egress or ingress NIC,
// TCP fair-sharing gives each flow approximately BW/N. This timeline
// tracks committed transfers and simulates new ones under contention.

// networkFlow represents a committed data transfer occupying bandwidth.
type networkFlow struct {
	srcNode  string
	dstNode  string
	start    float64 // seconds
	end      float64 // seconds
	taskName string  // source task (for logging)
}

// networkTimeline tracks committed data transfers for contention modelling.
type networkTimeline struct {
	flows []networkFlow
}

// commitFlow records a completed transfer in the timeline.
func (nt *networkTimeline) commitFlow(srcNode, dstNode, taskName string, start, end float64) {
	nt.flows = append(nt.flows, networkFlow{
		srcNode: srcNode, dstNode: dstNode,
		start: start, end: end, taskName: taskName,
	})
}

// pendingTransfer describes a data transfer to be simulated.
type pendingTransfer struct {
	srcNode  string
	dstNode  string
	start    float64 // earliest time the transfer can begin
	dataSize int64   // bytes
	taskName string
}

// transferResult holds the simulated timing for a pending transfer.
type transferResult struct {
	srcNode  string
	dstNode  string
	start    float64
	end      float64
	dataSize int64
	taskName string
}

// simulateTransfers computes the end time for each pending transfer,
// accounting for TCP fair-sharing of bandwidth with committed flows
// and among the pending transfers themselves.
//
// At any point in time, if N flows share a node's egress (or ingress),
// each flow gets linkBW/N. The simulation advances through time boundaries
// where the number of concurrent flows changes, recomputing effective
// bandwidth at each interval.
func (nt *networkTimeline) simulateTransfers(
	pending []pendingTransfer,
	bwFunc func(src, dst string) float64,
) []transferResult {
	if len(pending) == 0 {
		return nil
	}

	type xferState struct {
		p         pendingTransfer
		remaining float64
		linkBW    float64
		endTime   float64
		done      bool
	}

	states := make([]xferState, len(pending))
	for i, p := range pending {
		bw := bwFunc(p.srcNode, p.dstNode)
		states[i] = xferState{p: p, remaining: float64(p.dataSize), linkBW: bw}
		if p.dataSize <= 0 || bw <= 0 {
			states[i].done = true
			states[i].remaining = 0
			states[i].endTime = p.start
		}
	}

	// Find earliest start.
	t := math.MaxFloat64
	for _, s := range states {
		if !s.done && s.p.start < t {
			t = s.p.start
		}
	}
	if t == math.MaxFloat64 {
		// All zero-size or zero-bandwidth.
		results := make([]transferResult, len(states))
		for i, s := range states {
			results[i] = transferResult{
				srcNode: s.p.srcNode, dstNode: s.p.dstNode,
				start: s.p.start, end: s.p.start,
				dataSize: s.p.dataSize, taskName: s.p.taskName,
			}
		}
		return results
	}

	for iter := 0; iter < 10000; iter++ {
		// Check completion.
		allDone := true
		for _, s := range states {
			if !s.done {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}

		// Jump to next pending start if nothing is active yet.
		anyActive := false
		nextStart := math.MaxFloat64
		for _, s := range states {
			if s.done {
				continue
			}
			if t >= s.p.start {
				anyActive = true
			} else if s.p.start < nextStart {
				nextStart = s.p.start
			}
		}
		if !anyActive {
			t = nextStart
			continue
		}

		// Count concurrent flows per node at time t.
		egressN := make(map[string]int)
		ingressN := make(map[string]int)
		for _, f := range nt.flows {
			if f.start <= t && t < f.end {
				egressN[f.srcNode]++
				ingressN[f.dstNode]++
			}
		}
		for _, s := range states {
			if !s.done && t >= s.p.start {
				egressN[s.p.srcNode]++
				ingressN[s.p.dstNode]++
			}
		}

		// Next event: committed flow boundary, pending start, or pending finish.
		nextEvent := math.MaxFloat64
		for _, f := range nt.flows {
			if f.start > t && f.start < nextEvent {
				nextEvent = f.start
			}
			if f.end > t && f.end < nextEvent {
				nextEvent = f.end
			}
		}
		for _, s := range states {
			if !s.done && s.p.start > t && s.p.start < nextEvent {
				nextEvent = s.p.start
			}
		}
		for _, s := range states {
			if s.done || t < s.p.start {
				continue
			}
			nE := egressN[s.p.srcNode]
			nI := ingressN[s.p.dstNode]
			effBW := min(s.linkBW/float64(nE), s.linkBW/float64(nI))
			if effBW <= 0 {
				continue
			}
			if finish := t + s.remaining/effBW; finish < nextEvent {
				nextEvent = finish
			}
		}

		if nextEvent <= t {
			break // safety valve
		}

		// Advance time, deducting transferred bytes.
		dt := nextEvent - t
		for i := range states {
			if states[i].done || t < states[i].p.start {
				continue
			}
			nE := egressN[states[i].p.srcNode]
			nI := ingressN[states[i].p.dstNode]
			effBW := min(states[i].linkBW/float64(nE), states[i].linkBW/float64(nI))
			states[i].remaining -= effBW * dt
			if states[i].remaining < 1.0 { // < 1 byte → done
				states[i].remaining = 0
				states[i].done = true
				states[i].endTime = nextEvent
			}
		}
		t = nextEvent
	}

	// Safety: mark anything still running as finishing now.
	for i := range states {
		if !states[i].done {
			states[i].endTime = t
		}
	}

	results := make([]transferResult, len(states))
	for i, s := range states {
		results[i] = transferResult{
			srcNode: s.p.srcNode, dstNode: s.p.dstNode,
			start: s.p.start, end: s.endTime,
			dataSize: s.p.dataSize, taskName: s.p.taskName,
		}
	}
	return results
}

// parseTaskCPUMillis parses "500m" → 500, "2" → 2000, "" → 0.
func parseTaskCPUMillis(s string) int64 {
	if s == "" {
		return 0
	}
	q, err := parseResourceQuantity(s)
	if err != nil {
		return 0
	}
	return q.MilliValue()
}

// parseTaskMemBytes parses "256Mi" → bytes, "" → 0.
func parseTaskMemBytes(s string) int64 {
	if s == "" {
		return 0
	}
	q, err := parseResourceQuantity(s)
	if err != nil {
		return 0
	}
	return q.Value()
}

// heftScheduleEntry holds the scheduling decision for a single task.
type heftScheduleEntry struct {
	Node     string
	EstStart float64
	EstEnd   float64
}

// heftFlowEntry is an internal per-edge flow record produced by HEFT.
type heftFlowEntry struct {
	FromTask string
	ToTask   string
	SrcNode  string
	DstNode  string
	Start    float64
	End      float64
	DataSize int64
}

// heftResult holds both the node assignment map and the predicted schedule.
type heftResult struct {
	assignMap map[string]nodeInfo
	schedule  map[string]heftScheduleEntry
	flows     []heftFlowEntry
}

// heftOptions holds tunable knobs for the HEFT scheduler.
//
// SpreadEpsilon controls tie-breaking when multiple candidate nodes yield
// near-identical EFTs. When > 0, any candidate whose EFT is within
// SpreadEpsilon seconds of the minimum EFT is considered tied, and the
// least-loaded node (fewest committed tasks) wins. SpreadEpsilon=0 still
// breaks exact-EFT ties toward the least-loaded node (a strict improvement
// over iteration-order tie-breaking).
//
// Motivation: when profiler-learned runtimes are nearly equal across
// candidates, classical HEFT's strict EFT comparison concentrates all load
// on one node, hurting resilience to stragglers and network contention.
type heftOptions struct {
	SpreadEpsilon float64
}

func heftAssignTasks(tasks []taskSpec, nodeMap map[string]nodeInfo, rtResolver runtimeResolver, dsResolver dataSizeResolver, bwResolver bandwidthResolver, opts heftOptions) heftResult {
	if len(tasks) == 0 {
		return heftResult{assignMap: map[string]nodeInfo{}, schedule: map[string]heftScheduleEntry{}, flows: []heftFlowEntry{}}
	}

	taskByName := make(map[string]*taskSpec, len(tasks))
	for i := range tasks {
		taskByName[tasks[i].Name] = &tasks[i]
	}

	// Build successors map.
	successors := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		if _, ok := successors[t.Name]; !ok {
			successors[t.Name] = nil
		}
		for _, dep := range t.Dependencies {
			successors[dep] = append(successors[dep], t.Name)
		}
	}

	// Resolver closures.
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

	// Average bandwidth/runtime for rank estimation.
	avgBandwidth := computeAvgBandwidth(nodeMap, resolveBandwidth)

	avgRuntime := func(taskName string) float64 {
		if rtResolver == nil {
			return taskByName[taskName].Runtime
		}
		total := 0.0
		count := 0
		for nodeName := range nodeMap {
			total += resolveRuntime(taskName, nodeName)
			count++
		}
		if count == 0 {
			return taskByName[taskName].Runtime
		}
		return total / float64(count)
	}

	commCostEstimate := func(taskName string) float64 {
		var bytes int64
		if dsResolver != nil {
			total := int64(0)
			count := 0
			for nodeName := range nodeMap {
				total += resolveDataSizeBytes(taskName, nodeName)
				count++
			}
			if count > 0 {
				bytes = total / int64(count)
			}
		} else {
			bytes = parseDataSizeBytes(taskByName[taskName].DataSize)
		}
		if bytes == 0 {
			return 0
		}
		return float64(bytes) / avgBandwidth
	}

	// Compute upward rank (same as classic HEFT).
	rank := make(map[string]float64, len(tasks))
	var computeRank func(name string) float64
	computeRank = func(name string) float64 {
		if v, ok := rank[name]; ok {
			return v
		}
		maxSuccCost := 0.0
		for _, s := range successors[name] {
			cost := commCostEstimate(name) + computeRank(s)
			if cost > maxSuccCost {
				maxSuccCost = cost
			}
		}
		r := avgRuntime(name) + maxSuccCost
		rank[name] = r
		return r
	}
	for _, t := range tasks {
		computeRank(t.Name)
	}

	// Sort by decreasing rank.
	sorted := make([]string, 0, len(tasks))
	for _, t := range tasks {
		sorted = append(sorted, t.Name)
	}
	// Stable sort: rank-tied tasks stay in spec order, which matches the
	// data-agent's WL_SUCCESSORS iteration order (see main.go:buildEnvVars).
	sort.SliceStable(sorted, func(i, j int) bool {
		return rank[sorted[i]] > rank[sorted[j]]
	})

	// Initialize per-node resource timelines.
	timelines := make(map[string]*nodeTimeline, len(nodeMap))
	for name, ni := range nodeMap {
		timelines[name] = &nodeTimeline{
			totalCPU: ni.cpuMillis,
			totalMem: ni.memBytes,
		}
	}

	// Network flow timeline for bandwidth contention.
	netTimeline := &networkTimeline{}

	taskFinish := make(map[string]float64, len(tasks))
	taskAssigned := make(map[string]string, len(tasks))
	result := make(map[string]nodeInfo, len(tasks))
	schedule := make(map[string]heftScheduleEntry, len(tasks))
	flows := make([]heftFlowEntry, 0)

	// sourceQueueEnd[dep] is the end time of dep's last outgoing cross-node
	// transfer. The data-agent pushes to successors serially (one blocking
	// HTTP POST at a time), so a new transfer from dep can't start before
	// both dep has finished AND dep's prior outgoing transfer has completed.
	sourceQueueEnd := make(map[string]float64, len(tasks))

	for _, name := range sorted {
		t := taskByName[name]

		// Determine candidate nodes.
		var candidates []string
		if len(t.Constraints) > 0 {
			for _, c := range t.Constraints {
				if _, ok := nodeMap[c]; ok {
					candidates = append(candidates, c)
				}
			}
			if len(candidates) == 0 {
				log.Printf("[heft] task %s: no constraint nodes in cluster; using all", name)
				for n := range nodeMap {
					candidates = append(candidates, n)
				}
			}
		} else {
			for n := range nodeMap {
				candidates = append(candidates, n)
			}
		}

		// Task resource requirements.
		taskCPU := parseTaskCPUMillis(t.CPU)
		taskMem := parseTaskMemBytes(t.Memory)

		// Evaluate every candidate first; then select with ε-tie-breaking.
		// Pure HEFT picks strictly-minimum-EFT; with SpreadEpsilon > 0, any
		// candidate within ε of the minimum is tied, and we prefer the
		// least-loaded node to avoid concentration.
		type candEval struct {
			nodeName  string
			eft       float64
			est       float64
			load      int
			transfers []transferResult
		}
		cands := make([]candEval, 0, len(candidates))
		minEFT := math.MaxFloat64

		for _, nodeName := range candidates {
			tl := timelines[nodeName]

			// Build pending transfers from deps to this candidate node.
			var pending []pendingTransfer
			depsReady := 0.0
			for _, dep := range t.Dependencies {
				depFinish := taskFinish[dep]
				depNode := taskAssigned[dep]

				if depNode != "" && depNode == nodeName {
					// Same node — no network transfer.
					if depFinish > depsReady {
						depsReady = depFinish
					}
				} else {
					bytes := resolveDataSizeBytes(dep, depNode)
					if bytes > 0 {
						// Data-agent serializes pushes out of dep: this transfer
						// can only start after dep's previous outgoing push finishes.
						start := max(depFinish, sourceQueueEnd[dep])
						pending = append(pending, pendingTransfer{
							srcNode:  depNode,
							dstNode:  nodeName,
							start:    start,
							dataSize: bytes,
							taskName: dep,
						})
					} else {
						if depFinish > depsReady {
							depsReady = depFinish
						}
					}
				}
			}

			// Simulate transfers with bandwidth contention (TCP fair-sharing).
			var transfers []transferResult
			if len(pending) > 0 {
				transfers = netTimeline.simulateTransfers(pending, resolveBandwidth)
				for _, tr := range transfers {
					if tr.end > depsReady {
						depsReady = tr.end
					}
				}
			}

			// Find earliest start where node has enough resources.
			runtime := resolveRuntime(name, nodeName)
			est := tl.earliestStart(taskCPU, taskMem, runtime, depsReady)
			eft := est + runtime

			cands = append(cands, candEval{
				nodeName:  nodeName,
				eft:       eft,
				est:       est,
				load:      len(tl.slots),
				transfers: transfers,
			})
			if eft < minEFT {
				minEFT = eft
			}
		}

		// Select least-loaded candidate whose EFT is within ε of the minimum.
		// Iteration order is the constraint-list order; stable within-tie pick.
		bestIdx := -1
		for i, c := range cands {
			if c.eft > minEFT+opts.SpreadEpsilon {
				continue
			}
			if bestIdx == -1 || c.load < cands[bestIdx].load {
				bestIdx = i
			}
		}
		bestNode := cands[bestIdx].nodeName
		bestEFT := cands[bestIdx].eft
		bestTransfers := cands[bestIdx].transfers

		// Commit network transfers for the chosen node.
		for _, tr := range bestTransfers {
			if tr.end > tr.start {
				netTimeline.commitFlow(tr.srcNode, tr.dstNode, tr.taskName, tr.start, tr.end)
			}
			// Extend the source task's outgoing queue so the next sibling's
			// transfer waits for this one to finish.
			if tr.end > sourceQueueEnd[tr.taskName] {
				sourceQueueEnd[tr.taskName] = tr.end
			}
			flows = append(flows, heftFlowEntry{
				FromTask: tr.taskName,
				ToTask:   name,
				SrcNode:  tr.srcNode,
				DstNode:  tr.dstNode,
				Start:    tr.start,
				End:      tr.end,
				DataSize: tr.dataSize,
			})
		}
		// Log contention effects on transfers.
		for _, tr := range bestTransfers {
			linkBW := resolveBandwidth(tr.srcNode, tr.dstNode)
			if linkBW > 0 {
				naiveDur := float64(tr.dataSize) / linkBW
				actualDur := tr.end - tr.start
				if actualDur > naiveDur*1.01 {
					log.Printf("[heft]   xfer %s->%s (dep %s): %.1fMB naive=%.2fs actual=%.2fs (%.0f%% slower from contention)",
						tr.srcNode, tr.dstNode, tr.taskName,
						float64(tr.dataSize)/1e6, naiveDur, actualDur,
						(actualDur/naiveDur-1)*100)
				}
			}
		}

		// Commit resources on the chosen node.
		taskCPUFinal := parseTaskCPUMillis(t.CPU)
		taskMemFinal := parseTaskMemBytes(t.Memory)
		runtime := resolveRuntime(name, bestNode)
		estFinal := timelines[bestNode].earliestStart(taskCPUFinal, taskMemFinal, runtime, bestEFT-runtime)
		timelines[bestNode].commit(name, estFinal, bestEFT, taskCPUFinal, taskMemFinal)

		taskAssigned[name] = bestNode
		taskFinish[name] = bestEFT
		result[name] = nodeMap[bestNode]
		schedule[name] = heftScheduleEntry{Node: bestNode, EstStart: estFinal, EstEnd: bestEFT}

		log.Printf("[heft] %-20s rank=%.1f -> %-10s EST=%.1fs EFT=%.1fs (cpu=%dm mem=%dMi)",
			name, rank[name], bestNode, estFinal, bestEFT, taskCPU, taskMem/(1<<20))
	}

	return heftResult{assignMap: result, schedule: schedule, flows: flows}
}

// computeAvgBandwidth returns the mean bandwidth across all distinct node pairs.
func computeAvgBandwidth(nodeMap map[string]nodeInfo, resolveBandwidth func(string, string) float64) float64 {
	nodes := make([]string, 0, len(nodeMap))
	for n := range nodeMap {
		nodes = append(nodes, n)
	}
	if len(nodes) < 2 {
		return heftDefaultBandwidth
	}
	total := 0.0
	count := 0
	for i, a := range nodes {
		for _, b := range nodes[i+1:] {
			total += resolveBandwidth(a, b)
			count++
		}
	}
	return total / float64(count)
}
