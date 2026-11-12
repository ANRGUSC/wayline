package main

// The reference policies, run as their own process (--mode=policy).
//
// The process is a client of Wayline like any other policy: it reads what
// the controller publishes (the run's plan ConfigMap, the observations
// ConfigMap, status.tasks, and the data agents' answers about which objects
// they hold) and acts only by writing the run's spec: spec.realization for
// copies, spec.placement for where tasks run, spec.rerun for what runs
// again. It shares the controller's code for the cost model and the
// decisions (planmodel.go, copypolicy.go, recovery.go's planRecovery), but
// nothing in memory.
//
// A run gets these policies when it has spec.realizationPolicy:
//
//   - copies (mode planAware or static): written once, when the run's plan
//     is published;
//   - repair (repair, default true): when a node the run uses is lost, its
//     unfinished work moves to the surviving node where it would finish
//     earliest, inputs come from surviving copies, and a producer runs again
//     only when no copy of a needed output survives;
//   - revision (revise, default true): after a repair the copy policy runs
//     again on the new placement, and consumers waiting on a degraded link
//     move, or take their input from another copy, when that finishes
//     clearly earlier.
//
// Every decision is appended to status.policy.decisions.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

const policyTick = 500 * time.Millisecond

type policyRun struct {
	mu        sync.Mutex
	uid       types.UID
	copied    bool      // plan-time copies written
	lastLink  time.Time // last link-triggered re-plan
	lastRep   time.Time // last loss-triggered repair attempt
	lostSig   string    // lost set at the last repair attempt
	decisions []map[string]any
}

var policyRuns sync.Map // run key -> *policyRun

var clusterNodes sync.Map // name -> nodeInfo (capacity and data address)

// runPolicyProcess is the policy process's main loop.
func runPolicyProcess(dynClient dynamic.Interface, client *kubernetes.Clientset) {
	log.Printf("[policy] reference policies running as a separate process")
	refreshClusterNodes(client)
	go func() {
		for range time.Tick(10 * time.Second) {
			refreshClusterNodes(client)
		}
	}()
	for range time.Tick(policyTick) {
		if _, _, err := readObservations(client); err != nil {
			log.Printf("[policy] observations: %v", err)
		}
		list, err := dynClient.Resource(odagGVR).Namespace("").List(context.Background(), metav1.ListOptions{})
		if err != nil {
			log.Printf("[policy] list runs: %v", err)
			continue
		}
		live := map[string]bool{}
		for i := range list.Items {
			obj := &list.Items[i]
			ph, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
			key := obj.GetNamespace() + "/" + obj.GetName()
			if ph != "Running" && ph != "Pending" {
				continue
			}
			pol := parseRealizationPolicy(obj)
			if !pol.Attached {
				continue
			}
			live[key] = true
			v, _ := policyRuns.LoadOrStore(key, &policyRun{uid: obj.GetUID()})
			pr := v.(*policyRun)
			if pr.uid != obj.GetUID() { // the name was reused by a new run
				pr = &policyRun{uid: obj.GetUID()}
				policyRuns.Store(key, pr)
			}
			go actOnRun(dynClient, client, obj, pol, pr)
		}
		policyRuns.Range(func(k, _ any) bool {
			if !live[k.(string)] {
				policyRuns.Delete(k)
			}
			return true
		})
	}
}

// refreshClusterNodes records every node's capacity and data address.
func refreshClusterNodes(client *kubernetes.Clientset) {
	list, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return
	}
	for _, n := range list.Items {
		ip := nodeDataIP(n)
		if ip != "" {
			nodeIPCache.Store(n.Name, ip)
		}
		ni := nodeInfo{name: n.Name, ip: ip}
		if cpu, ok := n.Status.Allocatable[corev1.ResourceCPU]; ok {
			ni.cpuMillis = cpu.MilliValue()
		}
		if mem, ok := n.Status.Allocatable[corev1.ResourceMemory]; ok {
			ni.memBytes = mem.Value()
		}
		clusterNodes.Store(n.Name, ni)
	}
}

func clusterNodeMap() map[string]nodeInfo {
	out := map[string]nodeInfo{}
	clusterNodes.Range(func(k, v any) bool {
		out[k.(string)] = v.(nodeInfo)
		return true
	})
	return out
}

// actOnRun applies whichever policies are due for one run.
func actOnRun(dynClient dynamic.Interface, client *kubernetes.Clientset, obj *unstructured.Unstructured,
	pol realizationPolicy, pr *policyRun) {

	if !pr.mu.TryLock() {
		return // the previous tick's work on this run is still going
	}
	defer pr.mu.Unlock()
	ns, name := obj.GetNamespace(), obj.GetName()

	pub, err := loadPublishedPlan(client, ns, name)
	if err != nil || pub.UID != string(obj.GetUID()) {
		return // not published yet (or a previous run's leftover)
	}
	tasks := extractTasks(obj)
	planned := modelFromPublished(pub, tasks, clusterNodeMap())
	planned.policy = pol

	// Copies, once, from the plan as published at deployment.
	if !pr.copied && (pol.Mode == policyPlanAware || pol.Mode == policyStatic) {
		applyPlanPolicy(dynClient, obj, planned, policyContext{})
		pr.copied = true
		pr.record(dynClient, ns, name, "copies", map[string]any{"mode": pol.Mode})
		return
	}
	pr.copied = true

	now := time.Now()
	lost := lostNodeSet()
	if pol.Repair && len(lost) > 0 {
		sig := strings.Join(sortedKeys(lost), ",")
		if sig != pr.lostSig || now.Sub(pr.lastRep) >= 2*time.Second {
			pr.lostSig, pr.lastRep = sig, now
			if policyRepair(dynClient, client, obj, pol, planned, pub, lost, false, pr) {
				return
			}
		}
	}
	if pol.Revise && len(degradedNow(now)) > 0 && now.Sub(pr.lastLink) >= replanInterval {
		pr.lastLink = now
		policyRepair(dynClient, client, obj, pol, planned, pub, lost, true, pr)
	}
}

// taskObs is what status.tasks says about one task.
type taskObs struct {
	node  string
	phase string
	state string // "DataVertex" and "CacheHit" run no code
	lost  bool
}

func observedTasks(obj *unstructured.Unstructured) map[string]taskObs {
	out := map[string]taskObs{}
	items, _, _ := unstructured.NestedSlice(obj.Object, "status", "tasks")
	for _, it := range items {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		if name == "" {
			continue
		}
		o := taskObs{}
		o.node, _ = m["node"].(string)
		o.phase, _ = m["phase"].(string)
		o.state, _ = m["state"].(string)
		o.lost, _ = m["lost"].(bool)
		out[name] = o
	}
	return out
}

// policyRepair re-plans a run's unfinished work against lost nodes and,
// when linkTriggered, degraded links, and writes the decision to the spec.
// It reports whether it changed anything.
func policyRepair(dynClient dynamic.Interface, client *kubernetes.Clientset, obj *unstructured.Unstructured,
	pol realizationPolicy, planned *planModel, pub *publishedPlan, lost map[string]bool,
	linkTriggered bool, pr *policyRun) bool {

	ns, name := obj.GetNamespace(), obj.GetName()
	affected := linkTriggered
	for _, n := range pub.Placement {
		if lost[n] {
			affected = true
			break
		}
	}
	if !affected {
		return false
	}
	live := *planned
	live.bw = liveBandwidth(planned.bw)
	m := &live
	now := time.Now()

	obs := observedTasks(obj)
	in := recoveryInput{m: m, lost: lost, assign: map[string]string{}, done: map[string]bool{},
		running: map[string]bool{}, onLost: map[string]bool{}, now: time.Since(m.deployed).Seconds(),
		startup: func(t string) float64 { return pol.startup(m.byName[t]) }}
	for t, n := range pub.Placement {
		in.assign[t] = n
	}
	for t, o := range obs {
		node := o.node
		if node == "" {
			node = in.assign[t]
		}
		active := o.phase == "Pending" || o.phase == "Running"
		switch {
		case o.phase == "Succeeded":
			in.done[t] = true
		case o.state == "DataVertex" || o.state == "CacheHit":
			// runs no code: it executes (again) wherever it is placed
		case active && (o.lost || lost[node]):
			in.onLost[t] = true
		case active:
			in.running[t] = true
		}
	}
	in.holders = likelyHolders(name, m, obj, lost)
	if linkTriggered {
		in.slow = func(src, dst string) bool { return linkDegraded(src, dst, now) }
		in.movable = map[string]bool{}
		for _, t := range m.order {
			if _, dispatched := obs[t]; dispatched || lost[in.assign[t]] {
				continue
			}
			for _, o := range m.inputObjects(t) {
				if pn := in.assign[producerOf(o)]; in.done[producerOf(o)] &&
					!contains(in.holders[o], in.assign[t]) && in.slow(pn, in.assign[t]) {
					in.movable[t] = true
				}
			}
		}
		if len(in.movable) == 0 {
			return false
		}
	}

	plan := planRecovery(in)

	// Reruns: a request is a new generation of spec.rerun. One the
	// controller has not enacted yet (status.rerunEnacted is behind) is not
	// repeated; once enacted, a later loss asks again.
	curRerun := parseRerun(obj)
	enacted := parseRerunEnacted(obj)
	rerun := map[string]any{}
	var asked []string
	for _, t := range plan.Rerun {
		if curRerun[t] > enacted[t] {
			continue
		}
		rerun[t] = curRerun[t] + 1
		asked = append(asked, t)
	}
	placement := map[string]any{}
	for _, mv := range plan.Moves {
		placement[mv.Task] = mv.To
	}
	newCopies := filterNewCopies(parseRealization(obj), plan.Copies)
	if len(placement) == 0 && len(rerun) == 0 && len(newCopies) == 0 && len(plan.Stuck)+len(plan.Blocked) == 0 {
		return false
	}
	if len(plan.Stuck)+len(plan.Blocked) > 0 {
		updateODAGPhase(dynClient, ns, name, "Failed",
			fmt.Sprintf("repair: no surviving node may run %v (%d task(s) wait on it)", plan.Stuck, len(plan.Blocked)))
		pr.record(dynClient, ns, name, "stuck", map[string]any{"stuck": plan.Stuck, "blocked": plan.Blocked})
		return true
	}

	// One patch: where tasks go, what runs again, where their inputs come from.
	spec := map[string]any{}
	if len(placement) > 0 {
		spec["placement"] = placement
	}
	if len(rerun) > 0 {
		spec["rerun"] = rerun
	}
	if len(newCopies) > 0 {
		byObj := map[string][]string{}
		for _, c := range newCopies {
			byObj[c.Object] = append(byObj[c.Object], c.To)
		}
		var entries []realizationEntry
		for _, o := range sortedStringKeys(byObj) {
			entries = append(entries, realizationEntry{Object: o, Copies: byObj[o], Origin: originRecovery})
		}
		spec["realization"] = realizationItems(mergeRealization(parseRealization(obj), entries, originRecovery))
	}
	body, _ := json.Marshal(map[string]any{"spec": spec})
	if _, err := dynClient.Resource(odagGVR).Namespace(ns).Patch(context.Background(), name,
		types.MergePatchType, body, metav1.PatchOptions{}); err != nil {
		log.Printf("[policy] %s/%s: patch: %v", ns, name, err)
		return false
	}
	reason := "node loss"
	if linkTriggered {
		reason = "degraded link"
	}
	pr.record(dynClient, ns, name, reason, map[string]any{
		"atS": round3(in.now), "lost": sortedKeys(lost), "moved": plan.Moves, "restarted": plan.Restart,
		"rerun": asked, "copies": newCopies,
	})
	log.Printf("[policy] %s/%s: %s -> moved %d, rerun %d, copies %d", ns, name, reason,
		len(plan.Moves), len(asked), len(newCopies))

	// The repair changed the plan: re-protect it.
	if len(plan.Moves) > 0 && pol.Mode == policyPlanAware && pol.Revise {
		next := map[string]nodeInfo{}
		for t, n := range in.assign {
			next[t] = planned.nodes[n]
			ni := next[t]
			ni.name = n
			next[t] = ni
		}
		for _, mv := range plan.Moves {
			ni := planned.nodes[mv.To]
			ni.name = mv.To
			next[mv.Task] = ni
		}
		skip := map[string]bool{}
		for t := range in.done {
			skip[t] = true
		}
		for t := range in.running {
			skip[t] = true
		}
		if fresh, err := dynClient.Resource(odagGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{}); err == nil {
			rm := repairedModel(extractTasks(fresh), planned, next, lost)
			applyPlanPolicy(dynClient, fresh, rm, policyContext{holders: in.holders, skip: skip})
		}
	}
	return true
}

// likelyHolders finds, for every object, the surviving nodes holding a
// valid copy, asking only nodes an object can have reached: its producer's
// node, its consumers' nodes (now and before any move), and every node a
// realization entry copies it to.
func likelyHolders(odagName string, m *planModel, obj *unstructured.Unstructured, lost map[string]bool) map[string][]string {
	cand := map[string]map[string]bool{}
	add := func(o, n string) {
		if n == "" || lost[n] {
			return
		}
		if cand[o] == nil {
			cand[o] = map[string]bool{}
		}
		cand[o][n] = true
	}
	moved := map[string][]string{} // task -> nodes it was moved away from
	items, _, _ := unstructured.NestedSlice(obj.Object, "status", "recovery")
	for _, it := range items {
		rec, _ := it.(map[string]any)
		plan, _ := rec["plan"].(map[string]any)
		mvs, _ := plan["moved"].([]any)
		for _, x := range mvs {
			mv, _ := x.(map[string]any)
			t, _ := mv["task"].(string)
			from, _ := mv["from"].(string)
			moved[t] = append(moved[t], from)
		}
	}
	for _, o := range m.objects() {
		p := producerOf(o)
		add(o, m.assign[p])
		for _, f := range moved[p] {
			add(o, f)
		}
	}
	for _, t := range m.order {
		for _, o := range m.inputObjects(t) {
			add(o, m.assign[t])
			for _, f := range moved[t] {
				add(o, f)
			}
		}
	}
	for _, e := range parseRealization(obj) {
		for _, n := range e.Copies {
			add(e.Object, n)
		}
	}
	type q struct{ obj, node, ip string }
	var qs []q
	for o, ns := range cand {
		for n := range ns {
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

// record appends a decision to status.policy.decisions.
func (pr *policyRun) record(dynClient dynamic.Interface, ns, name, kind string, detail map[string]any) {
	d := map[string]any{"at": time.Now().UTC().Format(time.RFC3339Nano), "kind": kind}
	for k, v := range detail {
		d[k] = v
	}
	pr.decisions = append(pr.decisions, d)
	st, _ := json.Marshal(map[string]any{"status": map[string]any{"policy": map[string]any{"decisions": pr.decisions}}})
	_, _ = dynClient.Resource(odagGVR).Namespace(ns).Patch(context.Background(), name,
		types.MergePatchType, st, metav1.PatchOptions{}, "status")
}

// realizationItems renders entries as spec.realization items.
func realizationItems(entries []realizationEntry) []map[string]any {
	items := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		it := map[string]any{"object": e.Object}
		if len(e.Copies) > 0 {
			it["copies"] = e.Copies
		}
		if len(e.Evict) > 0 {
			it["evict"] = e.Evict
		}
		if e.ServingCopy != "" {
			it["servingCopy"] = e.ServingCopy
		}
		if e.Durable {
			it["durable"] = true
		}
		if e.Origin != "" {
			it["origin"] = e.Origin
		}
		items = append(items, it)
	}
	return items
}
