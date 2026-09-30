package main

// Runtime revision of object realizations (spec.realization).
//
// A policy patches the live ODAG's spec.realization; this reconciler
// converges the data plane toward it using only existing agent verbs:
// a copy is created by asking a node that holds valid bytes to push
// them (idempotent, digest-guarded install on the target), and removed
// with the per-object DELETE. The logical DAG, the pods, and the task
// placements are never touched. status.objects reports the actual copy
// state so a revision's entire life is visible on the run object.
//
// Entries come from three writers, recorded in `origin`: a user or external
// policy (no origin), the plan-aware policy ("plan", copypolicy.go) and
// node-loss recovery ("recovery", recovery.go). All converge the same way.
//
// Safety rules:
//   - additive by default: copies not listed are left alone unless
//     named in evict;
//   - an evict that would remove the LAST installed copy of an object
//     whose consumers are not all finished is refused and logged;
//   - conflicting entries (a node in both copies and evict) are
//     refused and logged;
//   - a lost node (nodehealth.go) is neither a source nor a target.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

type realizationEntry struct {
	Object      string
	Copies      []string
	ServingCopy string
	Evict       []string
	Durable     bool   // make every installed copy durable (durability.go)
	Origin      string // "" (user/external policy), "plan", or "recovery"
}

// lastSpecGen: run key -> the spec generation last handed to the reconciler.
var lastSpecGen sync.Map

// realizationCache holds the parsed spec.realization per run
// ("namespace/odag" -> []realizationEntry), refreshed on every
// processReadyTasks pass and every reconcile, so the dispatch path can
// resolve serving-copy overrides without an API round-trip.
var realizationCache sync.Map

// nodeIPCache maps node name -> internal IP for every schedulable node,
// refreshed whenever getNodeInfoMap runs. Lets the vertex path resolve
// agents on nodes outside the run's assignment.
var nodeIPCache sync.Map

// servingOverride returns the servingCopy node for an object if a
// revision names one, else "".
func servingOverride(key, object string) string {
	raw, ok := realizationCache.Load(key)
	if !ok {
		return ""
	}
	for _, e := range raw.([]realizationEntry) {
		if e.Object == object {
			return e.ServingCopy
		}
	}
	return ""
}

func parseRealization(obj *unstructured.Unstructured) []realizationEntry {
	items, found, _ := unstructured.NestedSlice(obj.Object, "spec", "realization")
	if !found {
		return nil
	}
	var out []realizationEntry
	for _, it := range items {
		m, ok := it.(map[string]interface{})
		if !ok {
			continue
		}
		e := realizationEntry{}
		e.Object, _ = m["object"].(string)
		if e.Object == "" {
			continue
		}
		e.ServingCopy, _ = m["servingCopy"].(string)
		e.Durable, _ = m["durable"].(bool)
		e.Origin, _ = m["origin"].(string)
		for _, c := range asStringSlice(m["copies"]) {
			e.Copies = append(e.Copies, c)
		}
		for _, c := range asStringSlice(m["evict"]) {
			e.Evict = append(e.Evict, c)
		}
		out = append(out, e)
	}
	return out
}

func asStringSlice(v interface{}) []string {
	raw, ok := v.([]interface{})
	if !ok {
		return nil
	}
	var out []string
	for _, x := range raw {
		if s, ok := x.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

// pushCopy asks the agent on srcIP to deliver (odag, object) to dstNode.
// The successor name namespaces the transfer record away from real
// consumers. 2xx (202 by design) means durably enqueued.
func pushCopy(srcIP, odagName, object, dstNode, dstIP string) error {
	body, _ := json.Marshal(map[string]interface{}{
		"successors": []map[string]string{{
			"name": "rev-" + dstNode, "host": dstIP, "node": dstNode,
		}},
	})
	url := fmt.Sprintf("http://%s:%d/push/%s/%s", srcIP, dataAgentPort, odagName, object)
	resp, err := httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("push: HTTP %d", resp.StatusCode)
	}
	return nil
}

// transferStateOf reads the per-consumer transfer state on the source
// agent ("" when unknown/absent).
func transferStateOf(srcIP, odagName, object, consumer string) string {
	url := fmt.Sprintf("http://%s:%d/transfers/%s/%s/%s",
		srcIP, dataAgentPort, odagName, object, consumer)
	resp, err := httpClient.Get(url)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// consumerPushesPending reports whether the agent on nodeIP is still
// delivering an object to its real consumers (not revision copies).
func consumerPushesPending(nodeIP, odagName, object string) bool {
	url := fmt.Sprintf("http://%s:%d/transfers/%s/%s", nodeIP, dataAgentPort, odagName, object)
	resp, err := httpClient.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	var states map[string]string
	if json.NewDecoder(resp.Body).Decode(&states) != nil {
		return false
	}
	for consumer, st := range states {
		if !strings.HasPrefix(consumer, "rev-") && (st == "Pending" || st == "Transferring") {
			return true
		}
	}
	return false
}

func evictCopy(nodeIP, odagName, object string) error {
	url := fmt.Sprintf("http://%s:%d/data/%s/%s", nodeIP, dataAgentPort, odagName, object)
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("evict: HTTP %d", resp.StatusCode)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Reconcile worker: one per run.
//
// A revision used to start one goroutine per spec generation that polled
// every 1 s, then every 5 s. A copy of an object produced late in the run
// then started up to 5 s after its producer finished. Now each run has one
// worker. It makes a pass whenever the spec changes or a dispatch pass
// reports progress (a producer finishing is a pod or warm-call event), and
// at least every second while unconverged and the run is active. It keeps
// what it has enqueued across revisions, so a transfer in flight is never
// restarted from byte zero by a later revision.
// ---------------------------------------------------------------------------

type realizeWorker struct {
	mu        sync.Mutex
	running   bool
	obj       *unstructured.Unstructured
	dyn       dynamic.Interface
	client    *kubernetes.Clientset
	wake      chan struct{}
	posted    map[string]bool // "object/node" enqueued; "sync/object/node" synced
	converged bool
	idle      int // passes since the run stopped being active
	status    string
}

var realizeWorkers sync.Map // run key -> *realizeWorker

const (
	realizeMinGap      = 250 * time.Millisecond
	realizeActiveEvery = time.Second
	realizeIdleEvery   = 5 * time.Second
	realizeIdlePasses  = 200 // passes after the run ended before giving up
)

// reconcileRealization hands a spec change to the run's worker. Status
// patches also fire MODIFIED; only spec changes bump the generation.
func reconcileRealization(dynClient dynamic.Interface, client *kubernetes.Clientset,
	obj *unstructured.Unstructured) {

	key := runKeyOf(obj)
	gen := obj.GetGeneration()
	if g, ok := lastSpecGen.Load(key); ok && g.(int64) >= gen {
		return
	}
	lastSpecGen.Store(key, gen)
	entries := parseRealization(obj)
	realizationCache.Store(key, entries)
	if len(entries) == 0 {
		return
	}
	kickRealization(dynClient, client, obj, true)
}

// realizationPending reports whether the run has a worker that has not
// converged; dispatch passes kick it so copies follow producers promptly.
func realizationPending(key string) bool {
	v, ok := realizeWorkers.Load(key)
	if !ok {
		return false
	}
	w := v.(*realizeWorker)
	w.mu.Lock()
	defer w.mu.Unlock()
	return !w.converged
}

// kickRealization asks for a pass with the latest run object.
func kickRealization(dynClient dynamic.Interface, client *kubernetes.Clientset,
	obj *unstructured.Unstructured, specChanged bool) {

	key := runKeyOf(obj)
	v, _ := realizeWorkers.LoadOrStore(key, &realizeWorker{
		wake: make(chan struct{}, 1), posted: map[string]bool{}})
	w := v.(*realizeWorker)
	w.mu.Lock()
	if obj.GetGeneration() >= generationOf(w.obj) {
		w.obj = obj
	}
	w.dyn, w.client = dynClient, client
	if specChanged {
		w.converged = false
		w.idle = 0
	}
	start := !w.running
	w.running = true
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
	if start {
		go w.loop(key)
	}
}

func generationOf(obj *unstructured.Unstructured) int64 {
	if obj == nil {
		return -1
	}
	return obj.GetGeneration()
}

func runKeyOf(obj *unstructured.Unstructured) string {
	ns := obj.GetNamespace()
	if ns == "" {
		ns = "default"
	}
	return ns + "/" + obj.GetName()
}

func (w *realizeWorker) loop(key string) {
	for {
		select {
		case <-w.wake:
		default:
		}
		w.mu.Lock()
		obj, dyn := w.obj, w.dyn
		w.mu.Unlock()

		converged := w.pass(key, dyn, obj)

		w.mu.Lock()
		w.converged = converged
		_, active := runningODAGs.Load(key)
		if !active {
			w.idle++
		}
		if converged || w.idle >= realizeIdlePasses {
			if !converged {
				log.Printf("[realize] %s: gave up before convergence (transfers may still complete)", key)
				w.converged = true
			} else {
				log.Printf("[realize] %s: realization converged", key)
			}
			// Exit unless a kick arrived during the pass.
			select {
			case <-w.wake:
				w.mu.Unlock()
				continue
			default:
			}
			w.running = false
			w.mu.Unlock()
			return
		}
		w.mu.Unlock()
		every := realizeActiveEvery
		if !active {
			every = realizeIdleEvery
		}
		time.Sleep(realizeMinGap)
		select {
		case <-w.wake:
		case <-time.After(every - realizeMinGap):
		}
	}
}

// pass runs one reconcile pass with the latest run object.
func (w *realizeWorker) pass(key string, dyn dynamic.Interface, obj *unstructured.Unstructured) bool {
	entries := parseRealization(obj)
	realizationCache.Store(key, entries)
	if len(entries) == 0 {
		return true
	}
	tasks := extractTasks(obj)
	taskByName := make(map[string]taskSpec, len(tasks))
	for _, t := range tasks {
		taskByName[t.Name] = t
	}
	consumersOf := make(map[string][]string)
	for _, t := range tasks {
		for _, d := range t.Dependencies {
			consumersOf[d] = append(consumersOf[d], t.Name)
		}
	}
	nodeMap := reconcileNodes(key, entries)
	converged := reconcileOnce(key, obj.GetName(), entries, taskByName,
		consumersOf, nodeMap, w.posted, linkBandwidth(key))
	if dyn != nil {
		w.status = writeObjectStatus(dyn, obj.GetNamespace(), obj.GetName(), entries, nodeMap, w.status)
	}
	return converged
}

// reconcileNodes is where a run's objects can have copies: the nodes its
// tasks are assigned to and every node its entries name, minus lost nodes.
// (Not every node with an agent: probing a whole cluster's agents, each
// with a 2 s timeout when one is down, would stall every pass.)
func reconcileNodes(key string, entries []realizationEntry) map[string]nodeInfo {
	nodeMap := map[string]nodeInfo{}
	add := func(n string) {
		if n == "" || nodeLost(n) {
			return
		}
		if _, ok := nodeMap[n]; ok {
			return
		}
		if ip, ok := nodeIPCache.Load(n); ok {
			nodeMap[n] = nodeInfo{name: n, ip: ip.(string)}
		}
	}
	if raw, ok := assignmentCache.Load(key); ok {
		for _, ni := range raw.(map[string]nodeInfo) {
			add(ni.name)
			if _, present := nodeMap[ni.name]; !present && ni.ip != "" && !nodeLost(ni.name) {
				nodeMap[ni.name] = ni
			}
		}
	}
	for _, e := range entries {
		add(e.ServingCopy)
		for _, n := range e.Copies {
			add(n)
		}
		for _, n := range e.Evict {
			add(n)
		}
	}
	return nodeMap
}

// linkBandwidth returns the bandwidth model a run was planned with, or the
// cluster's configured bandwidths for a run this controller did not place,
// with degraded links priced at what they deliver now (linkstate.go).
func linkBandwidth(key string) func(src, dst string) float64 {
	if v, ok := planModels.Load(key); ok {
		return liveBandwidth(v.(*planModel).bw)
	}
	res := buildBandwidthResolver(nil, 3, "external")
	return liveBandwidth(func(src, dst string) float64 {
		if b := res(src, dst); b > 0 {
			return b
		}
		return heftDefaultBandwidth
	})
}

// pickSource chooses where a copy for `target` comes from: the servingCopy
// when its bytes are valid there, else the ready holder with the fastest
// link to the target (ties by name, so the choice is stable).
func pickSource(e realizationEntry, target string, ready func(string) bool,
	nodes []string, bw func(src, dst string) float64) string {

	if e.ServingCopy != "" && e.ServingCopy != target && ready(e.ServingCopy) {
		return e.ServingCopy
	}
	best, bestBW := "", -1.0
	for _, n := range nodes {
		if n == target || !ready(n) {
			continue
		}
		if b := bw(n, target); b > bestBW+1e-9 {
			best, bestBW = n, b
		}
	}
	return best
}

// reconcileOnce performs one pass of actions; returns true when every
// desired copy is installed and every evict target is gone.
// posted remembers which (object, node) copies have been enqueued:
// re-POSTing would rewrite the durable transfer entry back to Pending and
// restart the copy, so each target is enqueued once and then polled to
// readiness (and re-enqueued only if its transfer died).
func reconcileOnce(key, odagName string, entries []realizationEntry,
	taskByName map[string]taskSpec, consumersOf map[string][]string,
	nodeMap map[string]nodeInfo, posted map[string]bool,
	bw func(src, dst string) float64) bool {

	nodes := make([]string, 0, len(nodeMap))
	for n := range nodeMap {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)

	converged := true
	for _, e := range entries {
		// e.Object is a producing task, or "task.output" for a named
		// output; either way the key is used verbatim on the data plane.
		producer := producerOf(e.Object)
		if _, ok := taskByName[producer]; !ok {
			log.Printf("[realize] %s: unknown object %q (skipped)", key, e.Object)
			continue
		}
		evictSet := make(map[string]bool, len(e.Evict))
		for _, n := range e.Evict {
			evictSet[n] = true
		}
		conflict := false
		for _, n := range e.Copies {
			if evictSet[n] {
				log.Printf("[realize] %s: %s: node %s in both copies and evict (entry refused)",
					key, e.Object, n)
				conflict = true
			}
		}
		if conflict {
			continue
		}

		readyMemo := map[string]bool{}
		ready := func(node string) bool {
			if r, ok := readyMemo[node]; ok {
				return r
			}
			ni, ok := nodeMap[node]
			r := ok && ni.ip != "" && isDataReady(ni.ip, odagName, e.Object)
			readyMemo[node] = r
			return r
		}

		// A plan copy waits until the producer's pushes to the object's
		// consumers are done, so it never competes with the transfers the
		// plan priced.
		deferred := false
		if e.Origin == originPlan {
			if pn := assignedNode(key, producer); pn != "" {
				if ni, ok := nodeMap[pn]; ok && consumerPushesPending(ni.ip, odagName, e.Object) {
					deferred = true
				}
			}
		}

		for _, n := range e.Copies {
			ni, ok := nodeMap[n]
			if !ok || ni.ip == "" {
				if nodeLost(n) {
					continue // a lost target is not waited for; recovery re-plans
				}
				log.Printf("[realize] %s: %s: no agent for copy target %s", key, e.Object, n)
				continue
			}
			if ready(n) {
				continue
			}
			converged = false
			if deferred {
				continue
			}
			source := pickSource(e, n, ready, nodes, bw)
			if source == "" {
				continue // not produced yet (or no surviving copy): next pass
			}
			if posted[e.Object+"/"+n] {
				// Re-enqueue only when the prior attempt is dead: a
				// Pending or Transferring entry must never be reset (a
				// re-POST restarts the copy from byte zero), but a
				// Failed one must be retried — under intermittent
				// connectivity (temporal relaying) the transfer that
				// failed during a blackout succeeds when the next
				// contact window opens.
				if copyInFlight(nodeMap, nodes, odagName, e.Object, n) {
					continue
				}
				// The earlier readiness check may predate a transfer that
				// completed since: ask again before sending a second copy.
				if isDataReady(ni.ip, odagName, e.Object) {
					readyMemo[n] = true
					continue
				}
			}
			if err := pushCopy(nodeMap[source].ip, odagName, e.Object, n, ni.ip); err != nil {
				log.Printf("[realize] %s: %s: copy %s->%s: %v", key, e.Object, source, n, err)
			} else {
				posted[e.Object+"/"+n] = true
				log.Printf("[realize] %s: %s: copy enqueued %s->%s (origin %q)",
					key, e.Object, source, n, e.Origin)
			}
		}

		// Durability revision: sync every installed copy in place, once per
		// node. Copies made later inherit it, because each push carries the
		// source copy's durability.
		if e.Durable {
			for _, node := range nodes {
				if !ready(node) || posted["sync/"+e.Object+"/"+node] {
					continue
				}
				if err := syncCopy(nodeMap[node].ip, odagName, e.Object); err != nil {
					log.Printf("[realize] %s: %s: sync on %s: %v", key, e.Object, node, err)
					converged = false
				} else {
					posted["sync/"+e.Object+"/"+node] = true
					log.Printf("[realize] %s: %s: copy on %s made durable", key, e.Object, node)
				}
			}
		}

		for _, n := range e.Evict {
			if !ready(n) {
				continue // already gone (or agentless): idempotent
			}
			// Never remove the last installed copy while any consumer of
			// this object could still need it.
			others := 0
			for _, node := range nodes {
				if node != n && ready(node) {
					others++
				}
			}
			if others == 0 && len(consumersOf[producer]) > 0 {
				log.Printf("[realize] %s: %s: refusing to evict last copy on %s", key, e.Object, n)
				converged = false
				continue
			}
			if err := evictCopy(nodeMap[n].ip, odagName, e.Object); err != nil {
				log.Printf("[realize] %s: %s: evict %s: %v", key, e.Object, n, err)
				converged = false
			} else {
				readyMemo[n] = false
				log.Printf("[realize] %s: %s: evicted copy on %s", key, e.Object, n)
			}
		}
	}
	return converged
}

// copyInFlight reports whether some live node is still transferring the
// object to `target` as a revision copy.
func copyInFlight(nodeMap map[string]nodeInfo, nodes []string, odagName, object, target string) bool {
	for _, n := range nodes {
		st := transferStateOf(nodeMap[n].ip, odagName, object, "rev-"+target)
		if st == "Pending" || st == "Transferring" {
			return true
		}
	}
	return false
}

// assignedNode is the node a task of the run is assigned to ("" if unknown).
func assignedNode(key, task string) string {
	if raw, ok := assignmentCache.Load(key); ok {
		return raw.(map[string]nodeInfo)[task].name
	}
	return ""
}

// writeObjectStatus reports actual copy state for every revised object, and
// returns what it wrote; an unchanged report is not written again.
func writeObjectStatus(dynClient dynamic.Interface, namespace, odagName string,
	entries []realizationEntry, nodeMap map[string]nodeInfo, last string) string {

	var objects []map[string]interface{}
	for _, e := range entries {
		var copies []map[string]interface{}
		seen := make(map[string]bool)
		evicted := make(map[string]bool, len(e.Evict))
		for _, n := range e.Evict {
			evicted[n] = true
		}
		for _, n := range append(append([]string{}, e.Copies...), e.Evict...) {
			if seen[n] {
				continue
			}
			seen[n] = true
			state := "Transferring"
			if nodeLost(n) {
				state = "Lost"
			} else if ni, ok := nodeMap[n]; ok && ni.ip != "" {
				if isDataReady(ni.ip, odagName, e.Object) {
					state = "Installed"
				} else if evicted[n] {
					state = "Evicted"
				}
			}
			copies = append(copies, map[string]interface{}{"node": n, "state": state})
		}
		o := map[string]interface{}{
			"object": e.Object, "copies": copies, "servingCopy": e.ServingCopy,
		}
		if e.Origin != "" {
			o["origin"] = e.Origin
		}
		objects = append(objects, o)
	}
	data, _ := json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{"objects": objects},
	})
	if string(data) == last {
		return last
	}
	if _, err := dynClient.Resource(odagGVR).Namespace(namespace).Patch(
		context.Background(), odagName, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	); err != nil {
		return last
	}
	return string(data)
}

// forgetRealization drops a deleted run's reconcile state.
func forgetRealization(key string) {
	realizeWorkers.Delete(key)
	realizationCache.Delete(key)
	lastSpecGen.Delete(key)
	planModels.Delete(key)
}
