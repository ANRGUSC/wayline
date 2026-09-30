package main

// Node-loss recovery: re-planning compute together with its data.
//
// When a node is lost (nodehealth.go), every task of an active run that has
// not finished and was placed there moves to the surviving node where it
// would finish earliest, under the run's plan model (planmodel.go). The
// search counts where each input already has a copy: an input on a node
// arrives there now, one elsewhere after its transfer, one whose producer
// must run again after that run. A copy the plan-aware policy placed on the
// fallback node therefore draws the task there and lets it start at once.
//
// Data follows the new placement. Each input of a task that will run is
// delivered from a surviving copy, by a realization entry with origin
// "recovery" that the ordinary reconciler converges. Only when no copy of
// an input survives anywhere does its producer run again (lineage), which
// may in turn need its own inputs, recursively.
//
// Tasks on nodes that survive stay where they are. The decision (which
// tasks move where, which run again, which copies are made) is a pure
// function of the run's state, planRecovery; recoverRun gathers that state
// and applies the decision. Every repair is appended to status.recovery.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

// recoveryInput is the state of a run when a node is lost.
type recoveryInput struct {
	m       *planModel
	lost    map[string]bool
	assign  map[string]string   // current placement
	done    map[string]bool     // finished; its outputs were produced
	running map[string]bool     // dispatched on a surviving node, not finished
	onLost  map[string]bool     // dispatched on a lost node, not finished
	holders map[string][]string // object -> surviving nodes with a valid copy
	now     float64             // seconds since the run was deployed
	startup func(task string) float64
	// Link re-planning: tasks not yet dispatched on surviving nodes that
	// may move when that finishes clearly earlier, and the links judged
	// degraded (nil: none).
	movable map[string]bool
	slow    func(src, dst string) bool
}

// moveMargin is how much earlier a movable task must finish elsewhere to
// be moved: a re-plan should not chase noise in its own estimates.
func moveMargin(stayFinish, now float64) float64 {
	return math.Max(2, 0.1*(stayFinish-now))
}

type taskMove struct {
	Task string  `json:"task"`
	From string  `json:"from"`
	To   string  `json:"to"`
	EFT  float64 `json:"estFinishS"`
}

type copyOrder struct {
	Object string `json:"object"`
	To     string `json:"to"`
}

// recoveryPlan is what recovery does to a run.
type recoveryPlan struct {
	Moves   []taskMove  `json:"moved,omitempty"`
	Restart []string    `json:"restarted,omitempty"` // dispatched on a lost node: run again
	Rerun   []string    `json:"rerun,omitempty"`     // finished, but no copy of a needed output survives
	Copies  []copyOrder `json:"copies,omitempty"`    // inputs delivered from surviving copies
	Stuck   []string    `json:"stuck,omitempty"`     // no surviving node may run the task
	Blocked []string    `json:"blocked,omitempty"`   // waits on a stuck task's output
}

func (p recoveryPlan) empty() bool {
	return len(p.Moves)+len(p.Restart)+len(p.Rerun)+len(p.Copies)+len(p.Stuck)+len(p.Blocked) == 0
}

// planRecovery decides how to repair a run after the nodes in in.lost fail.
func planRecovery(in recoveryInput) recoveryPlan {
	m := in.m
	var plan recoveryPlan

	// Tasks that will run: everything not finished and not running on a
	// surviving node (a task dispatched on a lost node runs again).
	willRun := map[string]bool{}
	for _, t := range m.order {
		if !in.done[t] && !in.running[t] {
			willRun[t] = true
			if in.onLost[t] {
				plan.Restart = append(plan.Restart, t)
			}
		}
	}
	// Lineage: a finished producer runs again when a task that will run
	// needs one of its outputs and no copy survives. Reverse topological
	// order reaches the fixpoint in one sweep.
	rerun := map[string]bool{}
	for i := len(m.order) - 1; i >= 0; i-- {
		t := m.order[i]
		if !willRun[t] {
			continue
		}
		for _, o := range m.inputObjects(t) {
			p := producerOf(o)
			if in.done[p] && !willRun[p] && len(in.holders[o]) == 0 {
				willRun[p], rerun[p] = true, true
			}
		}
	}
	// A re-run producer may itself need lost inputs.
	for changed := true; changed; {
		changed = false
		for i := len(m.order) - 1; i >= 0; i-- {
			t := m.order[i]
			if !rerun[t] {
				continue
			}
			for _, o := range m.inputObjects(t) {
				p := producerOf(o)
				if in.done[p] && !willRun[p] && len(in.holders[o]) == 0 {
					willRun[p], rerun[p], changed = true, true, true
				}
			}
		}
	}
	for _, t := range m.order {
		if rerun[t] {
			plan.Rerun = append(plan.Rerun, t)
		}
	}

	// Placement, in topological order so a moved producer's new finish
	// time informs its consumers.
	final := make(map[string]string, len(in.assign))
	for t, n := range in.assign {
		final[t] = n
	}
	finish := map[string]float64{}
	skip := map[string]bool{}
	for _, t := range m.order {
		if in.done[t] || willRun[t] || in.lost[in.assign[t]] {
			skip[t] = true // not on a surviving node's future timeline as planned
		}
	}
	tl := m.timelines(skip)
	arrive := func(o, n string) float64 {
		best := math.Inf(1)
		for _, h := range in.holders[o] {
			if h == n {
				return in.now
			}
			best = math.Min(best, in.now+m.transferTime(o, h, n))
		}
		p := producerOf(o)
		switch {
		case willRun[p]:
			best = math.Min(best, finish[p]+m.transferTime(o, final[p], n))
		case in.running[p]:
			best = math.Min(best, math.Max(in.now, m.eft[p])+m.transferTime(o, final[p], n))
		}
		return best
	}
	for _, t := range m.order {
		if !willRun[t] {
			continue
		}
		task := m.byName[t]
		cpu, mem := parseTaskCPUMillis(task.CPU), parseTaskMemBytes(task.Memory)
		from := in.assign[t]
		notBefore := in.now + in.startup(t)
		if !in.lost[from] && from != "" {
			ready := notBefore
			for _, o := range m.inputObjects(t) {
				ready = math.Max(ready, arrive(o, from))
			}
			dur := m.runtime(t, from)
			stay := startOn(tl[from], cpu, mem, dur, ready) + dur
			if in.movable[t] {
				to, eft := m.bestNode(t, in.lost, tl, notBefore, arrive)
				if to != "" && to != from && eft < stay-moveMargin(stay, in.now) {
					final[t], finish[t] = to, eft
					d := m.runtime(t, to)
					if tl[to] != nil {
						tl[to].commit(t, eft-d, eft, cpu, mem)
					}
					plan.Moves = append(plan.Moves, taskMove{Task: t, From: from, To: to, EFT: round3(eft)})
					continue
				}
			}
			finish[t] = stay
			if tl[from] != nil {
				tl[from].commit(t, stay-dur, stay, cpu, mem)
			}
			continue
		}
		to, eft := m.bestNode(t, in.lost, tl, notBefore, arrive)
		if to == "" || math.IsInf(eft, 1) {
			if len(m.candidates(t, in.lost)) == 0 {
				plan.Stuck = append(plan.Stuck, t)
			} else {
				plan.Blocked = append(plan.Blocked, t)
			}
			finish[t] = math.Inf(1)
			continue
		}
		final[t], finish[t] = to, eft
		dur := m.runtime(t, to)
		if tl[to] != nil {
			tl[to].commit(t, eft-dur, eft, cpu, mem)
		}
		plan.Moves = append(plan.Moves, taskMove{Task: t, From: from, To: to, EFT: round3(eft)})
	}

	// Data: every input a task that will run lacks on its node comes from a
	// surviving copy, unless its producer will (re)run and push it there
	// itself. A producer already running pushes to where its consumers were
	// when it was dispatched, so a consumer that moved needs a copy; so does
	// one whose producer's node, the source of the original push, is lost.
	seen := map[copyOrder]bool{}
	for _, t := range m.order {
		if !willRun[t] || final[t] == "" || in.lost[final[t]] {
			continue
		}
		n := final[t]
		moved := n != in.assign[t]
		for _, o := range m.inputObjects(t) {
			p := producerOf(o)
			if willRun[p] || n == final[p] || contains(in.holders[o], n) {
				continue // (re)run producer pushes it, or writes it right there
			}
			if !moved && !in.lost[in.assign[p]] &&
				!(in.slow != nil && in.slow(in.assign[p], n) && fasterHolder(in, o, in.assign[p], n)) {
				// The original push from a surviving node is on its way
				// (unless it crosses a degraded link and another copy
				// could come faster).
				continue
			}
			c := copyOrder{Object: o, To: n}
			if !seen[c] {
				seen[c] = true
				plan.Copies = append(plan.Copies, c)
			}
		}
	}
	return plan
}

// fasterHolder reports whether some holder other than src, reachable over a
// link not judged degraded, could deliver obj to dst.
func fasterHolder(in recoveryInput, obj, src, dst string) bool {
	for _, h := range in.holders[obj] {
		if h != src && h != dst && !in.slow(h, dst) {
			return true
		}
	}
	return false
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Applying a recovery to a live run.
// ---------------------------------------------------------------------------

type recoveryRecord struct {
	At     string       `json:"at"`
	AtS    float64      `json:"atS"` // seconds since deployment
	Reason string       `json:"reason"`
	Lost   []string     `json:"lost"`
	Plan   recoveryPlan `json:"plan"`
}

// runRecovery serializes recovery per run and remembers what it has done.
type runRecovery struct {
	mu      sync.Mutex
	history []recoveryRecord
	handled map[string]string // task -> incarnation already cleaned up
}

var runRecoveries sync.Map // run key -> *runRecovery

// podDeleteFn deletes a task pod immediately (tests replace it).
var podDeleteFn = func(client *kubernetes.Clientset, namespace, name string) error {
	zero := int64(0)
	return client.CoreV1().Pods(namespace).Delete(context.Background(), name,
		metav1.DeleteOptions{GracePeriodSeconds: &zero})
}

// recoverRun repairs one run against the currently lost nodes. Idempotent:
// a run with nothing on a lost node, or already repaired, is left alone.
func recoverRun(dynClient dynamic.Interface, client *kubernetes.Clientset, namespace, name string) {
	repairRun(dynClient, client, namespace, name, false)
}

// repairRun re-plans a run's unfinished work against lost nodes and, when
// linkTriggered, against degraded links: consumers waiting on a slow link
// may move, or take their input from a faster copy.
func repairRun(dynClient dynamic.Interface, client *kubernetes.Clientset, namespace, name string,
	linkTriggered bool) {

	key := namespace + "/" + name
	v, _ := runRecoveries.LoadOrStore(key, &runRecovery{handled: map[string]string{}})
	rr := v.(*runRecovery)
	rr.mu.Lock()
	defer rr.mu.Unlock()

	lost := lostNodeSet()
	if len(lost) == 0 && !linkTriggered {
		return
	}
	obj, err := dynClient.Resource(odagGVR).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return
	}
	if ph, _, _ := unstructured.NestedString(obj.Object, "status", "phase"); ph != "Running" && ph != "Pending" && ph != "Scheduling" {
		return
	}
	raw, ok := assignmentCache.Load(key)
	if !ok {
		return
	}
	assignMap := raw.(map[string]nodeInfo)
	// A run with no task placed on a lost node is unaffected: its inputs
	// come from its producers' nodes, and every producer survived.
	affected := linkTriggered
	for _, ni := range assignMap {
		if lost[ni.name] {
			affected = true
			break
		}
	}
	if !affected {
		return
	}
	tasks := extractTasks(obj)
	pol := parseRealizationPolicy(obj)
	planned := modelFor(key, client, tasks, assignMap, pol)
	// Price transfers with what links deliver now, not what they did at
	// planning time (a copy: the planned model is shared).
	live := *planned
	live.bw = liveBandwidth(planned.bw)
	m := &live
	uid := obj.GetUID()
	nowT := time.Now()

	// State: which tasks finished, which run on surviving nodes, where each
	// object still has a valid copy.
	pods := runPods(namespace, name, uid, tasks)
	podOf := map[string]corev1.Pod{}
	for _, p := range pods {
		podOf[p.Labels[labelTaskName]] = p
	}
	succs := successorCounts(tasks)
	in := recoveryInput{m: m, lost: lost, assign: map[string]string{}, done: map[string]bool{},
		running: map[string]bool{}, onLost: map[string]bool{}, now: time.Since(m.deployed).Seconds(),
		startup: func(t string) float64 { return defaultRealizationPolicy().startup(m.byName[t]) }}
	if pol := parseRealizationPolicy(obj); pol.Mode != "" {
		in.startup = func(t string) float64 { return pol.startup(m.byName[t]) }
	}
	for t, ni := range assignMap {
		in.assign[t] = ni.name
	}
	for _, t := range tasks {
		ni := assignMap[t.Name]
		switch {
		case isDataVertex(t, succs[t.Name]):
			vn := vertexNode(namespace, name, t, assignMap)
			in.done[t.Name] = !lost[vn.name] && dataVertexExecuted(vn, namespace, name, t.Name)
		case cacheHitIs(namespace, name, t.Name):
			in.done[t.Name] = true
		default:
			p, has := podOf[t.Name]
			if !has {
				continue
			}
			node := p.Spec.NodeName
			if node == "" {
				node = ni.name
			}
			switch {
			case p.Status.Phase == corev1.PodSucceeded:
				in.done[t.Name] = true
			case lost[node]:
				in.onLost[t.Name] = true
			case p.Status.Phase == corev1.PodPending || p.Status.Phase == corev1.PodRunning:
				in.running[t.Name] = true
			}
		}
	}
	in.holders = objectHolders(name, m, lost)
	if linkTriggered {
		// Consumers not yet dispatched whose input would cross a degraded
		// link may move; inputs on such links may come from other copies.
		in.slow = func(src, dst string) bool { return linkDegraded(src, dst, nowT) }
		in.movable = map[string]bool{}
		for _, t := range m.order {
			if in.done[t] || in.running[t] || in.onLost[t] || lost[in.assign[t]] {
				continue
			}
			if _, has := podOf[t]; has {
				continue
			}
			for _, o := range m.inputObjects(t) {
				if pn := in.assign[producerOf(o)]; !contains(in.holders[o], in.assign[t]) && in.slow(pn, in.assign[t]) {
					in.movable[t] = true
				}
			}
		}
	}

	plan := planRecovery(in)
	// Restart only what has not been cleaned up for this incarnation yet.
	var restart []string
	for _, t := range append(append([]string{}, plan.Restart...), plan.Rerun...) {
		inc := incarnation(namespace, name, uid, t, podOf)
		if inc != "" && rr.handled[t] == inc {
			continue
		}
		restart = append(restart, t)
	}
	newCopies := filterNewCopies(parseRealization(obj), plan.Copies)
	if len(plan.Moves) == 0 && len(restart) == 0 && len(newCopies) == 0 && len(plan.Stuck)+len(plan.Blocked) == 0 {
		return
	}

	// 1. New placement first, so no dispatch pass sends a task to a lost node.
	next := make(map[string]nodeInfo, len(assignMap))
	for t, ni := range assignMap {
		next[t] = ni
	}
	for _, mv := range plan.Moves {
		runnerFailures.Delete(dispatchKey(namespace, name, uid, mv.Task))
		ip, _ := nodeIPCache.Load(mv.To)
		ni := m.nodes[mv.To]
		ni.name = mv.To
		if s, ok := ip.(string); ok {
			ni.ip = s
		}
		next[mv.Task] = ni
	}
	assignmentCache.Store(key, next)
	storeRecoveredModel(key, planned, next)
	dropFromOrder(key, plan.Moves, lost)

	// 2. Clean up tasks that run again: their pods or warm calls, dispatch
	// claims and CPU reservations.
	for _, t := range restart {
		if p, ok := podOf[t]; ok && p.Labels[labelRunner] == "" {
			if err := podDeleteFn(client, namespace, p.Name); err != nil {
				log.Printf("[recover] %s: delete pod %s: %v", key, p.Name, err)
			}
			podCache.Delete(namespace + "/" + p.Name)
		}
		rr.handled[t] = incarnation(namespace, name, uid, t, podOf)
		warmInvocations.Delete(warmKey(namespace, name, uid, t))
		releaseDispatch(dispatchKey(namespace, name, uid, t))
		runnerFailures.Delete(dispatchKey(namespace, name, uid, t)) // a new node, a new chance at its runner
		releaseCPU(namespace + "/" + name + "/" + t)
		if ni := next[t]; ni.ip != "" {
			resetTaskState(ni.ip, name, t)
		}
	}

	// 3. Inputs from surviving copies.
	if len(newCopies) > 0 {
		var entries []realizationEntry
		byObj := map[string][]string{}
		for _, c := range newCopies {
			byObj[c.Object] = append(byObj[c.Object], c.To)
		}
		for _, o := range sortedStringKeys(byObj) {
			entries = append(entries, realizationEntry{Object: o, Copies: byObj[o], Origin: originRecovery})
		}
		patchRealization(dynClient, namespace, name, mergeRealization(parseRealization(obj), entries, originRecovery))
	}

	// 4. Record, and fail the run only if some task has nowhere to go.
	reason := "node loss"
	if linkTriggered {
		reason = "degraded link"
		if len(lost) > 0 {
			reason = "node loss and degraded link"
		}
	}
	rec := recoveryRecord{At: time.Now().UTC().Format(time.RFC3339Nano), AtS: round3(in.now),
		Reason: reason, Lost: sortedKeys(lost), Plan: plan}
	rec.Plan.Restart, rec.Plan.Copies = restartSubset(plan.Restart, restart), newCopies
	rec.Plan.Rerun = restartSubset(plan.Rerun, restart)
	rr.history = append(rr.history, rec)
	st, _ := json.Marshal(map[string]any{"status": map[string]any{"recovery": rr.history}})
	_, _ = dynClient.Resource(odagGVR).Namespace(namespace).Patch(context.Background(), name,
		types.MergePatchType, st, metav1.PatchOptions{}, "status")
	log.Printf("[recover] %s: %s, lost %v -> moved %d, restarted %d, rerun %d, copies %d, stuck %d, blocked %d",
		key, reason, rec.Lost, len(plan.Moves), len(rec.Plan.Restart), len(rec.Plan.Rerun), len(newCopies),
		len(plan.Stuck), len(plan.Blocked))
	for _, mv := range plan.Moves {
		log.Printf("[recover] %s:   %s %s -> %s (est. finish %.1fs)", key, mv.Task, mv.From, mv.To, mv.EFT)
	}
	if len(plan.Stuck)+len(plan.Blocked) > 0 {
		updateODAGPhase(dynClient, namespace, name, "Failed",
			fmt.Sprintf("node loss: no surviving node may run %v (%d task(s) wait on it)",
				plan.Stuck, len(plan.Blocked)))
		return
	}
	// The repair changed the plan: re-protect it. Objects that now sit
	// alone on one node (a task moved next to its producer, a copy lost
	// with its node) get the copies the policy would give them.
	if len(plan.Moves) > 0 && pol.Mode == policyPlanAware && pol.Revise {
		reprotect(dynClient, namespace, name, tasks, planned, next, in, lost)
	}
	go processReadyTasks(dynClient, client, namespace, name, obj)
}

// incarnation identifies the execution of a task that recovery would clean
// up: its pod's UID, or its warm call's start time.
func incarnation(namespace, name string, uid types.UID, task string, podOf map[string]corev1.Pod) string {
	if v, ok := warmInvocations.Load(warmKey(namespace, name, uid, task)); ok {
		inv := v.(*warmInvocation)
		inv.mu.Lock()
		defer inv.mu.Unlock()
		return "warm@" + inv.invoked.Format(time.RFC3339Nano)
	}
	if p, ok := podOf[task]; ok && p.UID != "" {
		return string(p.UID)
	}
	return ""
}

func restartSubset(all, did []string) []string {
	set := map[string]bool{}
	for _, t := range did {
		set[t] = true
	}
	var out []string
	for _, t := range all {
		if set[t] {
			out = append(out, t)
		}
	}
	return out
}

// filterNewCopies drops copies an entry already asks for.
func filterNewCopies(existing []realizationEntry, want []copyOrder) []copyOrder {
	have := map[copyOrder]bool{}
	for _, e := range existing {
		for _, n := range e.Copies {
			have[copyOrder{Object: e.Object, To: n}] = true
		}
	}
	var out []copyOrder
	for _, c := range want {
		if !have[c] {
			out = append(out, c)
		}
	}
	return out
}

func sortedStringKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func cacheHitIs(namespace, name, task string) bool {
	_, ok := cacheHitFor(namespace, name, task)
	return ok
}

// objectHolders finds, for every object of the run, the surviving nodes
// holding a valid copy.
func objectHolders(odagName string, m *planModel, lost map[string]bool) map[string][]string {
	type q struct{ obj, node, ip string }
	var qs []q
	for _, o := range m.objects() {
		for n := range m.nodes {
			if lost[n] {
				continue
			}
			ip := m.nodes[n].ip
			if v, ok := nodeIPCache.Load(n); ok {
				ip = v.(string)
			}
			if ip != "" {
				qs = append(qs, q{o, n, ip})
			}
		}
	}
	out := map[string][]string{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, statusQueryConcurrency)
	for _, x := range qs {
		wg.Add(1)
		sem <- struct{}{}
		go func(x q) {
			defer wg.Done()
			defer func() { <-sem }()
			if isDataReady(x.ip, odagName, x.obj) {
				mu.Lock()
				out[x.obj] = append(out[x.obj], x.node)
				mu.Unlock()
			}
		}(x)
	}
	wg.Wait()
	for o := range out {
		sort.Strings(out[o])
	}
	return out
}

// modelFor returns the run's plan model, or, for a run this controller did
// not place (it restarted since), a model built from the spec's runtimes and
// the cluster's configured bandwidths.
func modelFor(key string, client *kubernetes.Clientset, tasks []taskSpec, assignMap map[string]nodeInfo,
	pol realizationPolicy) *planModel {
	if v, ok := planModels.Load(key); ok {
		return v.(*planModel)
	}
	nodeMap, err := getNodeInfoMap(client)
	if err != nil {
		nodeMap = map[string]nodeInfo{}
	}
	predicted, flows := computePredictedSchedule(tasks, assignMap, nil, nil, buildBandwidthResolver(nil, 3, "external"))
	m := newPlanModel(tasks, assignMap, nodeMap, nil, nil, buildBandwidthResolver(nil, 3, "external"), predicted, flows)
	m.deployed = time.Now()
	m.policy = pol
	planModels.Store(key, m)
	return m
}

// reprotect re-applies the plan-aware policy to a repaired run: a model of
// the new placement (predicted afresh with the planned cost model, minus
// lost nodes), the surviving copies as known holders, and only the
// consumers that have not started.
func reprotect(dynClient dynamic.Interface, namespace, name string, tasks []taskSpec,
	planned *planModel, next map[string]nodeInfo, in recoveryInput, lost map[string]bool) {

	nodes := map[string]nodeInfo{}
	for n, ni := range planned.nodes {
		if !lost[n] {
			nodes[n] = ni
		}
	}
	// Price copies at what links deliver now: a copy over a link that just
	// degraded costs what the link delivers, not what was planned.
	bw := liveBandwidth(planned.bw)
	predicted, flows := computePredictedSchedule(tasks, next, planned.runtime, planned.dataSize, bw)
	m := newPlanModel(tasks, next, nodes, planned.runtime, planned.dataSize, bw, predicted, flows)
	m.policy = planned.policy
	skip := map[string]bool{}
	for t := range in.done {
		skip[t] = in.done[t]
	}
	for t := range in.running {
		skip[t] = skip[t] || in.running[t]
	}
	obj, err := dynClient.Resource(odagGVR).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return
	}
	applyPlanPolicy(dynClient, obj, m, policyContext{holders: in.holders, skip: skip})
}

// storeRecoveredModel keeps the plan model in step with the new placement
// (a copy: the model is shared read-only with other goroutines).
func storeRecoveredModel(key string, m *planModel, assign map[string]nodeInfo) {
	c := *m
	c.assign = make(map[string]string, len(assign))
	for t, ni := range assign {
		c.assign[t] = ni.name
	}
	planModels.Store(key, &c)
}

// dropFromOrder removes moved tasks and lost nodes from an enacted
// per-node order: a moved task must not wait behind its new node's queue,
// nor its old node's.
func dropFromOrder(key string, moves []taskMove, lost map[string]bool) {
	raw, ok := schedulePlanCache.Load(key)
	if !ok || len(moves) == 0 {
		return
	}
	plan := raw.(schedulePlan)
	moved := map[string]bool{}
	for _, mv := range moves {
		moved[mv.Task] = true
	}
	order := map[string][]string{}
	for node, ts := range plan.Order {
		if lost[node] {
			continue
		}
		for _, t := range ts {
			if !moved[t] {
				order[node] = append(order[node], t)
			}
		}
	}
	plan.Order = order
	schedulePlanCache.Store(key, plan)
}
