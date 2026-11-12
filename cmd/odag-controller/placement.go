package main

// External placement and re-execution: how a policy outside the controller
// changes where tasks run and what runs again.
//
//	spec:
//	  placement:
//	    <task>: <node>
//	  rerun:
//	    <task>: <generation>
//
// spec.placement names the node a policy wants for a task. The controller
// enacts it for every task that has not started, and for every task whose
// execution was on a node that has since been lost (that execution is
// abandoned and the task starts again on the new node). A task that started
// on a live node is committed and keeps its node; its entry is rejected and
// the rejection reported in status.placementRejected, as is an entry naming
// a lost or unknown node. Enacting a move:
//
//   - updates the run's assignment, so the task is gated and dispatched on
//     its new node, and producers dispatched later push straight there;
//   - for each input whose producer has already been dispatched on a live
//     node, orders a copy to the new node served from the producer's node,
//     unless spec.realization already brings the object there (a policy
//     that moves a task usually says where its inputs come from in the same
//     patch), and cancels the producer's push to the old node, including one
//     that has not started yet (the agent remembers the cancel);
//   - republishes the plan (planpub.go) with the new placement;
//   - records the move in status.recovery (reason "external placement").
//
// spec.rerun asks for a finished task to run again, for example a producer
// whose every copy of an output was lost with its node. Each entry carries
// a generation; the controller acts once per new generation. A rerun task
// is re-dispatched on its current node (a policy that wants it elsewhere
// writes spec.placement in the same patch) and pushes its outputs to its
// consumers' current nodes.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

// parsePlacement reads spec.placement as task -> node.
func parsePlacement(obj *unstructured.Unstructured) map[string]string {
	raw, found, _ := unstructured.NestedMap(obj.Object, "spec", "placement")
	if !found {
		return nil
	}
	out := make(map[string]string, len(raw))
	for t, v := range raw {
		if s, ok := v.(string); ok && s != "" {
			out[t] = s
		}
	}
	return out
}

// parseRerun reads spec.rerun as task -> generation.
func parseRerun(obj *unstructured.Unstructured) map[string]int64 {
	raw, found, _ := unstructured.NestedMap(obj.Object, "spec", "rerun")
	if !found {
		return nil
	}
	out := make(map[string]int64, len(raw))
	for t, v := range raw {
		switch g := v.(type) {
		case int64:
			out[t] = g
		case float64:
			out[t] = int64(g)
		case int:
			out[t] = int64(g)
		}
	}
	return out
}

// parseRerunEnacted reads status.rerunEnacted: task -> the spec.rerun
// generation the controller has enacted.
func parseRerunEnacted(obj *unstructured.Unstructured) map[string]int64 {
	out := map[string]int64{}
	raw, found, _ := unstructured.NestedMap(obj.Object, "status", "rerunEnacted")
	if !found {
		return out
	}
	for t, v := range raw {
		switch g := v.(type) {
		case int64:
			out[t] = g
		case float64:
			out[t] = int64(g)
		case int:
			out[t] = int64(g)
		}
	}
	return out
}

func publishRerunEnacted(dynClient dynamic.Interface, namespace, name string, gens map[string]int64) {
	cp := make(map[string]int64, len(gens))
	for t, g := range gens {
		cp[t] = g
	}
	st, _ := json.Marshal(map[string]any{"status": map[string]any{"rerunEnacted": cp}})
	if _, err := dynClient.Resource(odagGVR).Namespace(namespace).Patch(context.Background(), name,
		types.MergePatchType, st, metav1.PatchOptions{}, "status"); err != nil {
		log.Printf("[placement] %s/%s: status.rerunEnacted: %v", namespace, name, err)
	}
}

var cancelHTTP = &http.Client{Timeout: 3 * time.Second}

// cancelPushTo asks the agent at holderIP to drop its push of object `obj`
// to consumer `consumer` (queued, in flight, or not started yet).
func cancelPushTo(holderIP, odagName, obj, consumer string) error {
	u := fmt.Sprintf("http://%s:%d/cancel/%s/%s?consumer=%s", holderIP, dataAgentPort, odagName, obj,
		url.QueryEscape(consumer))
	resp, err := cancelHTTP.Post(u, "application/json", bytes.NewReader(nil))
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("cancel %s at %s: status %d", obj, holderIP, resp.StatusCode)
	}
	return nil
}

// reconcilePlacement enacts spec.placement and spec.rerun.
func reconcilePlacement(dynClient dynamic.Interface, client *kubernetes.Clientset,
	namespace, name string, obj *unstructured.Unstructured) {

	want := parsePlacement(obj)
	rerun := parseRerun(obj)
	if len(want) == 0 && len(rerun) == 0 {
		return
	}
	key := namespace + "/" + name
	v, _ := runRecoveries.LoadOrStore(key, &runRecovery{handled: map[string]string{}})
	rr := v.(*runRecovery)
	rr.mu.Lock()
	defer rr.mu.Unlock()

	raw, ok := assignmentCache.Load(key)
	if !ok {
		return
	}
	assign := raw.(map[string]nodeInfo)
	tasks := extractTasks(obj)
	byName := make(map[string]taskSpec, len(tasks))
	for _, t := range tasks {
		byName[t.Name] = t
	}
	uid := obj.GetUID()
	pods := runPods(namespace, name, uid, tasks)
	podOf := map[string]corev1.Pod{}
	for _, p := range pods {
		podOf[p.Labels[labelTaskName]] = p
	}
	// An execution on a lost node that had not finished is abandoned: the
	// task may be placed again.
	abandoned := map[string]bool{}
	for t, p := range podOf {
		node := p.Spec.NodeName
		if node == "" {
			node = assign[t].name
		}
		if p.Status.Phase != corev1.PodSucceeded && nodeLost(node) {
			abandoned[t] = true
		}
	}

	// Reruns: once per new generation, for a task that has finished. The
	// enacted generations are published in status.rerunEnacted, so a policy
	// knows which of its requests took effect, and a restarted controller
	// does not enact an old request again.
	var reruns []string
	if rr.rerunGen == nil {
		rr.rerunGen = parseRerunEnacted(obj)
	}
	enactedBefore := len(rr.rerunGen)
	genChanged := false
	succs := successorCounts(tasks)
	for _, t := range sortedStringKeys(rerun) {
		gen := rerun[t]
		if gen <= rr.rerunGen[t] {
			continue
		}
		genChanged = true
		task, ok := byName[t]
		if !ok {
			rr.rerunGen[t] = gen
			continue
		}
		finished := false
		if isDataVertex(task, succs[t]) {
			finished = dataVertexExecuted(vertexNode(namespace, name, task, assign), namespace, name, t)
		} else if p, ok := podOf[t]; ok {
			finished = p.Status.Phase == corev1.PodSucceeded
		}
		rr.rerunGen[t] = gen
		if finished {
			reruns = append(reruns, t)
		}
	}
	if genChanged || len(rr.rerunGen) != enactedBefore {
		publishRerunEnacted(dynClient, namespace, name, rr.rerunGen)
	}
	rerunning := map[string]bool{}
	for _, t := range reruns {
		rerunning[t] = true // its finished execution is discarded: it may move too
	}
	rejected := map[string]string{}
	var moves []taskMove
	for _, t := range sortedStringKeys(want) {
		to := want[t]
		cur, known := assign[t]
		if !known {
			rejected[t] = "no such task"
			continue
		}
		if to == cur.name {
			continue
		}
		if _, has := podOf[t]; has && !abandoned[t] && !rerunning[t] {
			rejected[t] = "started on " + cur.name
			continue
		}
		if _, claimed := dispatchClaims.Load(dispatchKey(namespace, name, uid, t)); claimed && !abandoned[t] && !rerunning[t] {
			continue // being dispatched on its current node right now; not a rejection yet
		}
		if _, ok := nodeIPCache.Load(to); !ok {
			rejected[t] = "unknown node " + to
			continue
		}
		if nodeLost(to) {
			rejected[t] = "node " + to + " is lost"
			continue
		}
		moves = append(moves, taskMove{Task: t, From: cur.name, To: to})
	}
	reportRejections(dynClient, namespace, name, rr, rejected)

	if len(moves) == 0 && len(reruns) == 0 {
		return
	}

	pol := parseRealizationPolicy(obj)
	planned := modelFor(key, client, tasks, assign, pol)
	next := make(map[string]nodeInfo, len(assign))
	for t, ni := range assign {
		next[t] = ni
	}
	var restarted []string
	for _, mv := range moves {
		ni := planned.nodes[mv.To]
		ni.name = mv.To
		if ip, ok := nodeIPCache.Load(mv.To); ok {
			ni.ip = ip.(string)
		}
		next[mv.Task] = ni
		readySince.Delete(dispatchKey(namespace, name, uid, mv.Task))
		runnerFailures.Delete(dispatchKey(namespace, name, uid, mv.Task))
		if abandoned[mv.Task] {
			restarted = append(restarted, mv.Task)
		}
	}
	assignmentCache.Store(key, next)
	storeRecoveredModel(key, planned, next)
	dropFromOrder(key, moves, lostNodeSet())

	// Abandoned executions and reruns: clear the old execution so the task
	// is dispatched afresh.
	for _, t := range append(append([]string{}, restarted...), reruns...) {
		clearExecution(client, namespace, name, uid, t, podOf, next, rr)
	}

	// Which producers already have their consumers' locations, decided after
	// the new placement is published: a dispatch that claims from here on
	// reads it (dispatchTask builds its env from the current assignment),
	// while one already claimed or started pushes to the old node and needs
	// a copy. Deciding before the publish missed producers dispatched in
	// between: their output went to the old node and the moved task waited
	// for it forever (E17: 2 of ~900 repair runs).
	started := startedTasks(namespace, name, uid, tasks)
	for _, t := range tasks {
		if _, claimed := dispatchClaims.Load(dispatchKey(namespace, name, uid, t.Name)); claimed {
			started[t.Name] = true
		}
	}
	for _, t := range reruns {
		started[t] = false // it pushes to its consumers' current nodes when it runs again
	}

	// Inputs of moved tasks whose producers have been dispatched on a live
	// node: re-send from the producer's node, unless the policy already
	// brings the object there, and drop the push to the old node.
	existing := parseRealization(obj)
	brings := map[copyOrder]bool{}
	for _, e := range existing {
		for _, n := range e.Copies {
			brings[copyOrder{Object: e.Object, To: n}] = true
		}
	}
	var entries []realizationEntry
	var copies []copyOrder
	for _, mv := range moves {
		t := byName[mv.Task]
		for _, dep := range t.Dependencies {
			if !started[dep] {
				continue // the producer is dispatched later, toward the new node
			}
			prod := next[dep]
			for _, o := range consumedKeys(t, dep) {
				if prod.ip != "" && !nodeLost(prod.name) {
					if err := cancelPushTo(prod.ip, name, o, mv.Task); err != nil {
						log.Printf("[placement] %s: cancel %s -> %s at %s: %v", key, o, mv.Task, prod.name, err)
					}
				}
				c := copyOrder{Object: o, To: mv.To}
				if brings[c] || nodeLost(prod.name) || prod.name == mv.To {
					continue // the policy brings it, or only a policy can say from where
				}
				copies = append(copies, c)
				entries = append(entries, realizationEntry{Object: o, Copies: []string{mv.To},
					ServingCopy: prod.name, Origin: originPlacement})
			}
		}
	}
	if len(entries) > 0 {
		patchRealization(dynClient, namespace, name, mergeRealization(existing, entries, originPlacement))
	}
	if mv, ok := planModels.Load(key); ok {
		publishPlan(client, namespace, name, uid, mv.(*planModel))
	}

	at := 0.0
	if planned != nil && !planned.deployed.IsZero() {
		at = time.Since(planned.deployed).Seconds()
	}
	sort.Slice(moves, func(i, j int) bool { return moves[i].Task < moves[j].Task })
	rec := recoveryRecord{At: time.Now().UTC().Format(time.RFC3339Nano), AtS: round3(at),
		Reason: "external placement", Lost: sortedKeys(lostNodeSet()),
		Plan: recoveryPlan{Moves: moves, Restart: restarted, Rerun: reruns, Copies: copies}}
	rr.history = append(rr.history, rec)
	st, _ := json.Marshal(map[string]any{"status": map[string]any{"recovery": rr.history}})
	_, _ = dynClient.Resource(odagGVR).Namespace(namespace).Patch(context.Background(), name,
		types.MergePatchType, st, metav1.PatchOptions{}, "status")
	log.Printf("[placement] %s: moved %d task(s) (%d restarted), rerun %d, %d input copies",
		key, len(moves), len(restarted), len(reruns), len(copies))
	for _, mv := range moves {
		log.Printf("[placement] %s:   %s %s -> %s", key, mv.Task, mv.From, mv.To)
	}
}

// clearExecution forgets a task's current execution (its pod or warm call,
// dispatch claim, CPU reservation and agent state on its node), so the next
// dispatch pass starts it again.
func clearExecution(client *kubernetes.Clientset, namespace, name string, uid types.UID, t string,
	podOf map[string]corev1.Pod, assign map[string]nodeInfo, rr *runRecovery) {

	if p, ok := podOf[t]; ok && p.Labels[labelRunner] == "" {
		if err := podDeleteFn(client, namespace, p.Name); err != nil {
			log.Printf("[placement] %s/%s: delete pod %s: %v", namespace, name, p.Name, err)
		}
		podCache.Delete(namespace + "/" + p.Name)
	}
	rr.handled[t] = incarnation(namespace, name, uid, t, podOf)
	warmInvocations.Delete(warmKey(namespace, name, uid, t))
	releaseDispatch(dispatchKey(namespace, name, uid, t))
	runnerFailures.Delete(dispatchKey(namespace, name, uid, t))
	releaseCPU(namespace + "/" + name + "/" + t)
	if ni := assign[t]; ni.ip != "" {
		resetTaskState(ni.ip, name, t)
	}
}

// reportRejections writes status.placementRejected when it changes.
func reportRejections(dynClient dynamic.Interface, namespace, name string, rr *runRecovery, rejected map[string]string) {
	if sameStringMap(rr.rejected, rejected) {
		return
	}
	rr.rejected = rejected
	var val any = rejected
	if len(rejected) == 0 {
		val = nil
	}
	st, _ := json.Marshal(map[string]any{"status": map[string]any{"placementRejected": val}})
	_, _ = dynClient.Resource(odagGVR).Namespace(namespace).Patch(context.Background(), name,
		types.MergePatchType, st, metav1.PatchOptions{}, "status")
	for t, why := range rejected {
		log.Printf("[placement] %s/%s: %s rejected: %s", namespace, name, t, why)
	}
}

func sameStringMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// startedTasks: the run's tasks that have a pod or a warm call.
func startedTasks(namespace, name string, uid types.UID, tasks []taskSpec) map[string]bool {
	started := map[string]bool{}
	for _, p := range runPods(namespace, name, uid, tasks) {
		started[p.Labels[labelTaskName]] = true
	}
	return started
}
