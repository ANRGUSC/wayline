package main

// The plan a run was placed with, kept for the whole run.
//
// Placement prices every task on a node and every transfer on a link. The
// same numbers decide which outputs deserve an extra copy (copypolicy.go)
// and where a task goes when its node is lost (recovery.go), so both reuse
// the model the planner used instead of a second, inconsistent estimate.
//
// Times are seconds from the run's deployment (the predicted schedule's
// t=0). The model is read-only after construction; decisions that place
// tasks hypothetically work on a copy of the node timelines.

import (
	"math"
	"sort"
	"sync"
	"time"
)

type planModel struct {
	tasks  []taskSpec
	byName map[string]*taskSpec
	order  []string            // topological order of task names
	assign map[string]string   // task -> planned node
	nodes  map[string]nodeInfo // nodes the run was planned on
	// Cost model, as the planner saw it.
	runtime  func(task, node string) float64
	dataSize func(task, node string) int64
	bw       func(src, dst string) float64 // bytes/s between distinct nodes

	est, eft map[string]float64
	lst      map[string]float64 // latest start that keeps the predicted completion time
	makespan float64
	flows    []predictedFlowEntry
	deployed time.Time
	policy   realizationPolicy // the run's spec.realizationPolicy
}

// planModels: "ns/odag" -> *planModel, for runs placed by this controller.
var planModels sync.Map

func newPlanModel(tasks []taskSpec, assignMap map[string]nodeInfo, nodeMap map[string]nodeInfo,
	rt runtimeResolver, ds dataSizeResolver, bw bandwidthResolver,
	predicted []predictedTaskEntry, flows []predictedFlowEntry) *planModel {

	m := &planModel{
		tasks:  tasks,
		byName: make(map[string]*taskSpec, len(tasks)),
		assign: make(map[string]string, len(assignMap)),
		nodes:  make(map[string]nodeInfo, len(nodeMap)),
		est:    map[string]float64{},
		eft:    map[string]float64{},
		lst:    map[string]float64{},
		flows:  flows,
	}
	for i := range tasks {
		m.byName[tasks[i].Name] = &tasks[i]
	}
	for t, ni := range assignMap {
		m.assign[t] = ni.name
	}
	for n, ni := range nodeMap {
		m.nodes[n] = ni
	}
	for _, ni := range assignMap {
		if _, ok := m.nodes[ni.name]; !ok && ni.name != "" {
			m.nodes[ni.name] = ni
		}
	}
	m.runtime = func(task, node string) float64 {
		if rt != nil {
			return rt(task, node)
		}
		if t := m.byName[task]; t != nil {
			return t.Runtime
		}
		return 0
	}
	m.dataSize = func(task, node string) int64 {
		if ds != nil {
			return ds(task, node)
		}
		if t := m.byName[task]; t != nil {
			return parseDataSizeBytes(t.DataSize)
		}
		return 0
	}
	m.bw = func(src, dst string) float64 {
		b := 0.0
		if bw != nil {
			b = bw(src, dst)
		}
		if b <= 0 {
			b = heftDefaultBandwidth
		}
		return b
	}
	for _, p := range predicted {
		m.est[p.Name] = p.EstStart
		m.eft[p.Name] = p.EstEnd
		m.makespan = math.Max(m.makespan, p.EstEnd)
	}
	m.order = topoOrder(tasks)
	m.computeLatestStarts()
	return m
}

// topoOrder returns task names so that every task follows its dependencies
// (spec order among independent tasks).
func topoOrder(tasks []taskSpec) []string {
	done := make(map[string]bool, len(tasks))
	var out []string
	for len(out) < len(tasks) {
		progress := false
		for _, t := range tasks {
			if done[t.Name] {
				continue
			}
			ready := true
			for _, d := range t.Dependencies {
				if !done[d] {
					ready = false
					break
				}
			}
			if ready {
				done[t.Name] = true
				out = append(out, t.Name)
				progress = true
			}
		}
		if !progress {
			break // a cycle; extractTasks does not produce one
		}
	}
	return out
}

// edgeDelay is how long after its producer finishes an input reaches the
// consumer in the plan: the predicted transfer (which includes waiting for
// shared links), or zero on one node.
func (m *planModel) edgeDelay(prod, cons string) float64 {
	for _, f := range m.flows {
		if f.FromTask == prod && f.ToTask == cons {
			return math.Max(0, f.End-m.eft[prod])
		}
	}
	return 0
}

// computeLatestStarts is the backward pass: the latest each task can start
// without delaying the predicted completion time, through DAG edges and
// planned transfer times. Node sharing is not modeled, so slack is an upper
// bound when a node runs several tasks back to back.
func (m *planModel) computeLatestStarts() {
	succs := map[string][]string{}
	for _, t := range m.tasks {
		for _, d := range t.Dependencies {
			succs[d] = append(succs[d], t.Name)
		}
	}
	for i := len(m.order) - 1; i >= 0; i-- {
		name := m.order[i]
		lf := m.makespan
		for _, s := range succs[name] {
			if v := m.lst[s] - m.edgeDelay(name, s); v < lf {
				lf = v
			}
		}
		m.lst[name] = lf - (m.eft[name] - m.est[name])
	}
}

// slack is how long a task can be delayed without delaying the run.
func (m *planModel) slack(task string) float64 {
	return math.Max(0, m.lst[task]-m.est[task])
}

// producerOf returns the task that produces an object key.
func producerOf(obj string) string {
	for i := 0; i < len(obj); i++ {
		if obj[i] == '.' {
			return obj[:i]
		}
	}
	return obj
}

// inputObjects lists the object keys a task reads, with their producers.
func (m *planModel) inputObjects(task string) []string {
	t := m.byName[task]
	if t == nil {
		return nil
	}
	var out []string
	for _, d := range t.Dependencies {
		out = append(out, consumedKeys(*t, d)...)
	}
	return out
}

// objects returns every object some task consumes, in topological order of
// their producers.
func (m *planModel) objects() []string {
	seen := map[string]bool{}
	var out []string
	for _, name := range m.order {
		for _, o := range m.inputObjects(name) {
			if !seen[o] {
				seen[o] = true
				out = append(out, o)
			}
		}
	}
	pos := make(map[string]int, len(m.order))
	for i, n := range m.order {
		pos[n] = i
	}
	sort.SliceStable(out, func(i, j int) bool {
		return pos[producerOf(out[i])] < pos[producerOf(out[j])]
	})
	return out
}

// consumersOf lists the tasks that read an object.
func (m *planModel) consumersOf(obj string) []string {
	var out []string
	for _, name := range m.order {
		for _, o := range m.inputObjects(name) {
			if o == obj {
				out = append(out, name)
				break
			}
		}
	}
	return out
}

// objectBytes is the size of an object: a named output's declared size, or
// the producer's resolved data size for its default output.
func (m *planModel) objectBytes(obj string) int64 {
	prod := producerOf(obj)
	t := m.byName[prod]
	if t == nil {
		return 0
	}
	if obj != prod {
		name := obj[len(prod)+1:]
		for _, o := range t.Outputs {
			if o.Name == name {
				return parseDataSizeBytes(o.DataSize)
			}
		}
		return 0
	}
	return m.dataSize(prod, m.assign[prod])
}

// transferTime is the time to move an object between two nodes on an
// otherwise idle link.
func (m *planModel) transferTime(obj, src, dst string) float64 {
	if src == dst {
		return 0
	}
	return float64(m.objectBytes(obj)) / m.bw(src, dst)
}

// plannedHolders are the nodes the plan leaves a copy of obj on: the
// producer's node and every consumer's node (the producer pushes to each).
func (m *planModel) plannedHolders(obj string) []string {
	set := map[string]bool{m.assign[producerOf(obj)]: true}
	for _, c := range m.consumersOf(obj) {
		set[m.assign[c]] = true
	}
	return sortedKeys(set)
}

// candidates are the nodes a task may run on, minus excluded ones.
func (m *planModel) candidates(task string, exclude map[string]bool) []string {
	t := m.byName[task]
	var out []string
	if t != nil && len(t.Constraints) > 0 {
		for _, c := range t.Constraints {
			if _, ok := m.nodes[c]; ok && !exclude[c] {
				out = append(out, c)
			}
		}
		return out
	}
	for n := range m.nodes {
		if !exclude[n] {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

// timelines rebuilds each node's planned CPU and memory commitments from
// the predicted schedule, leaving out the tasks in `skip`.
func (m *planModel) timelines(skip map[string]bool) map[string]*nodeTimeline {
	tl := make(map[string]*nodeTimeline, len(m.nodes))
	for n, ni := range m.nodes {
		tl[n] = &nodeTimeline{totalCPU: ni.cpuMillis, totalMem: ni.memBytes}
	}
	for _, t := range m.tasks {
		if skip[t.Name] {
			continue
		}
		n := m.assign[t.Name]
		if tl[n] == nil {
			continue
		}
		tl[n].commit(t.Name, m.est[t.Name], m.eft[t.Name],
			parseTaskCPUMillis(t.CPU), parseTaskMemBytes(t.Memory))
	}
	return tl
}

// startOn is the earliest a task of the given shape can start on a node at
// or after `ready`. A node of unknown capacity admits anything.
func startOn(tl *nodeTimeline, cpu, mem int64, dur, ready float64) float64 {
	if tl == nil || tl.totalCPU <= 0 || tl.totalMem <= 0 {
		return ready
	}
	// A task larger than the whole node still runs, alone (capacity.go).
	cpu = min(cpu, tl.totalCPU)
	mem = min(mem, tl.totalMem)
	return tl.earliestStart(cpu, mem, dur, ready)
}

// nodeEFT is a candidate node and the task's finish time there.
type nodeEFT struct {
	node string
	eft  float64
}

// rankNodes lists the candidate nodes for `task` by the time it would
// finish there, given when its inputs can be on each node (arrive) and the
// earliest it may start anywhere (notBefore). Ties keep name order.
func (m *planModel) rankNodes(task string, exclude map[string]bool,
	tl map[string]*nodeTimeline, notBefore float64,
	arrive func(obj, node string) float64) []nodeEFT {

	t := m.byName[task]
	if t == nil {
		return nil
	}
	cpu, mem := parseTaskCPUMillis(t.CPU), parseTaskMemBytes(t.Memory)
	var out []nodeEFT
	for _, n := range m.candidates(task, exclude) {
		ready := notBefore
		for _, o := range m.inputObjects(task) {
			ready = math.Max(ready, arrive(o, n))
		}
		dur := m.runtime(task, n)
		out = append(out, nodeEFT{n, startOn(tl[n], cpu, mem, dur, ready) + dur})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].eft < out[j].eft-1e-9 })
	return out
}

// bestNode places `task` where it would finish earliest (see rankNodes).
// Returns "" when every candidate is excluded.
func (m *planModel) bestNode(task string, exclude map[string]bool,
	tl map[string]*nodeTimeline, notBefore float64,
	arrive func(obj, node string) float64) (string, float64) {

	r := m.rankNodes(task, exclude, tl, notBefore, arrive)
	if len(r) == 0 || math.IsInf(r[0].eft, 1) {
		return "", math.Inf(1)
	}
	return r[0].node, r[0].eft
}

func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k, ok := range set {
		if ok && k != "" {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
