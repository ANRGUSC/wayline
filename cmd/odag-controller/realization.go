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
// Safety rules:
//   - additive by default: copies not listed are left alone unless
//     named in evict;
//   - an evict that would remove the LAST installed copy of an object
//     whose consumers are not all finished is refused and logged;
//   - conflicting entries (a node in both copies and evict) are
//     refused and logged.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
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
}

// reconcileGen dedupes concurrent reconciles per run: only the goroutine
// holding the latest generation keeps converging.
var reconcileGen sync.Map // key -> int64
var lastSpecGen sync.Map  // key -> generation last reconciled

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

// reconcileRealization converges the data plane toward spec.realization.
// Runs as a goroutine per ODAG MODIFIED event; polls until converged or
// superseded by a newer revision.
func reconcileRealization(dynClient dynamic.Interface, client *kubernetes.Clientset,
	obj *unstructured.Unstructured) {

	namespace := obj.GetNamespace()
	if namespace == "" {
		namespace = "default"
	}
	odagName := obj.GetName()
	key := namespace + "/" + odagName

	entries := parseRealization(obj)
	if len(entries) == 0 {
		return
	}
	// Status patches also fire MODIFIED; only spec changes bump the
	// object's generation, so reconcile once per generation.
	specGen := obj.GetGeneration()
	if g, ok := lastSpecGen.Load(key); ok && g.(int64) >= specGen {
		return
	}
	lastSpecGen.Store(key, specGen)
	realizationCache.Store(key, entries)
	gen := time.Now().UnixNano()
	reconcileGen.Store(key, gen)

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

	nodeMap, err := getNodeInfoMap(client)
	if err != nil {
		log.Printf("[realize] %s: node map: %v", key, err)
		return
	}
	// The run's assigned nodes may include nodes the scheduler map filters
	// (none today, but be defensive): merge in the assignment cache.
	if raw, ok := assignmentCache.Load(key); ok {
		for _, ni := range raw.(map[string]nodeInfo) {
			if _, present := nodeMap[ni.name]; !present && ni.ip != "" {
				nodeMap[ni.name] = ni
			}
		}
	}

	posted := make(map[string]bool)
	for attempt := 0; attempt < 120; attempt++ {
		if g, _ := reconcileGen.Load(key); g != gen {
			return // superseded by a newer revision
		}
		converged := reconcileOnce(key, odagName, entries, taskByName,
			consumersOf, nodeMap, posted)
		writeObjectStatus(dynClient, namespace, odagName, entries, nodeMap)
		if converged {
			log.Printf("[realize] %s: realization converged", key)
			return
		}
		time.Sleep(5 * time.Second)
	}
	log.Printf("[realize] %s: gave up before convergence (transfers may still complete)", key)
}

// reconcileOnce performs one pass of actions; returns true when every
// desired copy is installed and every evict target is gone.
// posted remembers which (object, node) copies this reconcile generation
// has already enqueued: re-POSTing would rewrite the durable transfer
// entry back to Pending and restart the copy, so each target is enqueued
// exactly once and then polled to readiness.
func reconcileOnce(key, odagName string, entries []realizationEntry,
	taskByName map[string]taskSpec, consumersOf map[string][]string,
	nodeMap map[string]nodeInfo, posted map[string]bool) bool {

	converged := true
	for _, e := range entries {
		// e.Object is a producing task, or "task.output" for a named
		// output; either way the key is used verbatim on the data plane.
		producer := e.Object
		if i := strings.IndexByte(producer, '.'); i > 0 {
			producer = producer[:i]
		}
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

		ready := func(node string) bool {
			ni, ok := nodeMap[node]
			return ok && ni.ip != "" && isDataReady(ni.ip, odagName, e.Object)
		}

		// Source preference: servingCopy if its bytes are valid, else any
		// desired copy that is, else wherever the object already lives.
		source := ""
		if e.ServingCopy != "" && ready(e.ServingCopy) {
			source = e.ServingCopy
		}
		if source == "" {
			for _, n := range e.Copies {
				if ready(n) {
					source = n
					break
				}
			}
		}
		if source == "" {
			for node := range nodeMap {
				if ready(node) {
					source = node
					break
				}
			}
		}

		for _, n := range e.Copies {
			if ready(n) {
				continue
			}
			converged = false
			if posted[e.Object+"/"+n] && source != "" {
				// Re-enqueue only when the prior attempt is dead: a
				// Pending or Transferring entry must never be reset (a
				// re-POST restarts the copy from byte zero), but a
				// Failed one must be retried — under intermittent
				// connectivity (temporal relaying) the transfer that
				// failed during a blackout succeeds when the next
				// contact window opens, and the 5s reconcile cadence is
				// what catches the window.
				st := transferStateOf(nodeMap[source].ip, odagName,
					e.Object, "rev-"+n)
				if st == "Pending" || st == "Transferring" {
					continue
				}
			}
			ni, ok := nodeMap[n]
			if !ok || ni.ip == "" {
				log.Printf("[realize] %s: %s: no agent for copy target %s", key, e.Object, n)
				continue
			}
			if source == "" {
				log.Printf("[realize] %s: %s: no valid copy anywhere to source from", key, e.Object)
				break
			}
			if err := pushCopy(nodeMap[source].ip, odagName, e.Object, n, ni.ip); err != nil {
				log.Printf("[realize] %s: %s: copy %s->%s: %v", key, e.Object, source, n, err)
			} else {
				posted[e.Object+"/"+n] = true
				log.Printf("[realize] %s: %s: copy enqueued %s->%s", key, e.Object, source, n)
			}
		}

		for _, n := range e.Evict {
			if !ready(n) {
				continue // already gone (or agentless): idempotent
			}
			// Never remove the last installed copy while any consumer of
			// this object could still need it.
			others := 0
			for node := range nodeMap {
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
				log.Printf("[realize] %s: %s: evicted copy on %s", key, e.Object, n)
			}
		}
	}
	return converged
}

// writeObjectStatus reports actual copy state for every revised object.
func writeObjectStatus(dynClient dynamic.Interface, namespace, odagName string,
	entries []realizationEntry, nodeMap map[string]nodeInfo) {

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
			if ni, ok := nodeMap[n]; ok && ni.ip != "" {
				if isDataReady(ni.ip, odagName, e.Object) {
					state = "Installed"
				} else if evicted[n] {
					state = "Evicted"
				}
			}
			copies = append(copies, map[string]interface{}{"node": n, "state": state})
		}
		objects = append(objects, map[string]interface{}{
			"object": e.Object, "copies": copies, "servingCopy": e.ServingCopy,
		})
	}
	data, _ := json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{"objects": objects},
	})
	_, _ = dynClient.Resource(odagGVR).Namespace(namespace).Patch(
		context.Background(), odagName, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	)
}
