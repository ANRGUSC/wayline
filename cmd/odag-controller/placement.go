package main

// External placement: spec.placement re-places tasks that have not started.
//
//	spec:
//	  placement:
//	    <task>: <node>
//
// An outside policy (a repair driver, a scheduler that re-plans on new
// information) writes the node it wants for each task; the controller enacts
// it for every task that has no pod or warm call yet. A task that has
// started is committed and keeps its node; its entry is ignored. Enacting a
// move:
//
//   - updates the run's assignment, so the task is gated and dispatched on
//     its new node, and producers dispatched later push straight there;
//   - for each input whose producer has already been dispatched (running or
//     finished), orders a copy to the new node served from the producer's
//     node (the producer re-sends its output in full), and cancels the
//     producer's push to the old node, including one that has not started
//     yet (the agent remembers the cancel);
//   - records the move in status.recovery (reason "external placement").
//
// A placement entry naming a task's current node, or a node the controller
// does not know, is a no-op. The run's own revision (realizationPolicy) is
// not involved: a run driven this way normally sets revise: false.

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

// reconcilePlacement enacts spec.placement for tasks that have not started.
func reconcilePlacement(dynClient dynamic.Interface, client *kubernetes.Clientset,
	namespace, name string, obj *unstructured.Unstructured) {

	want := parsePlacement(obj)
	if len(want) == 0 {
		return
	}
	key := namespace + "/" + name
	// Serialize with node-loss and link recovery for this run.
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
	started := startedTasks(namespace, name, uid, tasks)

	var moves []taskMove
	for _, t := range sortedStringKeys(want) {
		to := want[t]
		cur, known := assign[t]
		if !known || to == cur.name {
			continue
		}
		if started[t] {
			continue // committed: it keeps its node
		}
		if _, claimed := dispatchClaims.Load(dispatchKey(namespace, name, uid, t)); claimed {
			continue // being dispatched on its current node right now
		}
		if _, ok := nodeIPCache.Load(to); !ok {
			log.Printf("[placement] %s: %s -> %s: unknown node, ignored", key, t, to)
			continue
		}
		moves = append(moves, taskMove{Task: t, From: cur.name, To: to})
	}
	if len(moves) == 0 {
		return
	}

	pol := parseRealizationPolicy(obj)
	planned := modelFor(key, client, tasks, assign, pol)
	next := make(map[string]nodeInfo, len(assign))
	for t, ni := range assign {
		next[t] = ni
	}
	for _, mv := range moves {
		ni := planned.nodes[mv.To]
		ni.name = mv.To
		if ip, ok := nodeIPCache.Load(mv.To); ok {
			ni.ip = ip.(string)
		}
		next[mv.Task] = ni
		readySince.Delete(dispatchKey(namespace, name, uid, mv.Task))
		runnerFailures.Delete(dispatchKey(namespace, name, uid, mv.Task))
	}
	assignmentCache.Store(key, next)
	storeRecoveredModel(key, planned, next)
	dropFromOrder(key, moves, nil)

	// Which producers already have their consumers' locations, decided after
	// the new placement is published: a dispatch that claims from here on
	// reads it (dispatchTask builds its env from the current assignment),
	// while one already claimed or started pushes to the old node and needs
	// a copy. Deciding before the publish missed producers dispatched in
	// between: their output went to the old node and the moved task waited
	// for it forever (E17: 2 of ~900 repair runs).
	started = startedTasks(namespace, name, uid, tasks)
	for _, t := range tasks {
		if _, claimed := dispatchClaims.Load(dispatchKey(namespace, name, uid, t.Name)); claimed {
			started[t.Name] = true
		}
	}

	// Inputs of moved tasks whose producers have been dispatched: re-send
	// from the producer's node, and drop the push to the old node.
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
				copies = append(copies, copyOrder{Object: o, To: mv.To})
				entries = append(entries, realizationEntry{Object: o, Copies: []string{mv.To},
					ServingCopy: prod.name, Origin: originRecovery})
				if prod.ip != "" {
					if err := cancelPushTo(prod.ip, name, o, mv.Task); err != nil {
						log.Printf("[placement] %s: cancel %s -> %s at %s: %v", key, o, mv.Task, prod.name, err)
					}
				}
			}
		}
	}
	if len(entries) > 0 {
		patchRealization(dynClient, namespace, name, mergeRealization(parseRealization(obj), entries, originRecovery))
	}

	at := 0.0
	if planned != nil && !planned.deployed.IsZero() {
		at = time.Since(planned.deployed).Seconds()
	}
	sort.Slice(moves, func(i, j int) bool { return moves[i].Task < moves[j].Task })
	rec := recoveryRecord{At: time.Now().UTC().Format(time.RFC3339Nano), AtS: round3(at),
		Reason: "external placement", Plan: recoveryPlan{Moves: moves, Copies: copies}}
	rr.history = append(rr.history, rec)
	st, _ := json.Marshal(map[string]any{"status": map[string]any{"recovery": rr.history}})
	_, _ = dynClient.Resource(odagGVR).Namespace(namespace).Patch(context.Background(), name,
		types.MergePatchType, st, metav1.PatchOptions{}, "status")
	log.Printf("[placement] %s: moved %d task(s), %d input copies", key, len(moves), len(copies))
	for _, mv := range moves {
		log.Printf("[placement] %s:   %s %s -> %s", key, mv.Task, mv.From, mv.To)
	}
}

// startedTasks: the run's tasks that have a pod or a warm call.
func startedTasks(namespace, name string, uid types.UID, tasks []taskSpec) map[string]bool {
	started := map[string]bool{}
	for _, p := range runPods(namespace, name, uid, tasks) {
		started[p.Labels[labelTaskName]] = true
	}
	return started
}
