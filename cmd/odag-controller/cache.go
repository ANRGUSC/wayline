package main

// Cross-run reuse: a task that declares spec.tasks[].cacheKey can be
// satisfied by an output a previous run already produced, instead of
// executing again.
//
// This is the third realization of the same mechanism data vertices use: a
// cache-satisfied task is a vertex whose alias source is another run's
// output. The controller pins the task to the node holding the copy and
// gives it runtime 0 BEFORE scheduling, so any external scheduler — which
// sees only tasks, constraints, and costs — places its consumers near the
// cached bytes without knowing what a cache is. Location-awareness rides on
// the ordinary cost model; no scheduler API change.
//
// Scope (deliberate): whole-task reuse. The cached task itself is skipped;
// its upstream still runs if the DAG contains it (their outputs simply go
// unused by this task). That keeps fallback trivially safe: if the cached
// copy disappears between deploy and dispatch (retention), the task runs as
// a normal pod on the node it was pinned to and its inputs are there
// waiting. Content-addressed generality is future work.
//
// Freshness: the registry maps cacheKey -> the newest completed copy.
// Every hit re-aliases the payload under the new run's name (hardlink), and
// the registry is repointed to the new run, so the entry stays valid even
// as old runs are retired by retention. Controller restart drops in-memory
// state; the registry is rebuilt from surviving ODAG objects, and a run
// whose hit state is lost simply executes normally.

import (
	"context"
	"log"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

// cacheEntry names the newest known copy of a cacheKey'd output.
type cacheEntry struct {
	Odag string // run that holds the copy
	Task string // task name within that run
	Node string // node the copy is installed on
	Unix float64
}

var cacheRegistry sync.Map // cacheKey -> cacheEntry
var cacheHits sync.Map     // "ns/odag/task" -> cacheEntry (hits bound at deploy)

func recordCacheEntry(key string, e cacheEntry) {
	if prev, ok := cacheRegistry.Load(key); ok &&
		prev.(cacheEntry).Unix > e.Unix {
		return // keep the newer copy
	}
	cacheRegistry.Store(key, e)
}

// cacheHitFor returns the hit bound for this task at deploy time, if any.
func cacheHitFor(namespace, odagName, taskName string) (cacheEntry, bool) {
	v, ok := cacheHits.Load(namespace + "/" + odagName + "/" + taskName)
	if !ok {
		return cacheEntry{}, false
	}
	return v.(cacheEntry), true
}

// clearCacheHit demotes a task back to normal execution (source vanished).
func clearCacheHit(namespace, odagName, taskName, key string) {
	cacheHits.Delete(namespace + "/" + odagName + "/" + taskName)
	if key != "" {
		cacheRegistry.Delete(key)
	}
}

// applyCacheHit rewrites a task spec so scheduling reflects the hit: the
// task is pinned to the node holding the copy and costs ~nothing. Pure —
// unit-testable without a cluster.
func applyCacheHit(t *taskSpec, e cacheEntry) {
	t.Constraints = []string{e.Node}
	t.Runtime = 0
	t.RuntimeProfile = nil
}

// markCacheHits binds registry hits for this run, mutating the local task
// slice before scheduling. Verification is live: a hit is only bound if the
// source output is still installed on its node right now.
func markCacheHits(namespace, odagName string, tasks []taskSpec,
	nodeMap map[string]nodeInfo) int {
	hits := 0
	for i := range tasks {
		t := &tasks[i]
		if t.CacheKey == "" || t.Type == "data" {
			continue
		}
		v, ok := cacheRegistry.Load(t.CacheKey)
		if !ok {
			continue
		}
		e := v.(cacheEntry)
		ni, ok := nodeMap[e.Node]
		if !ok || ni.ip == "" || !isDataReady(ni.ip, e.Odag, e.Task) {
			// Copy gone (retention, node drained). Drop the stale entry so
			// later runs stop probing it; this run executes normally.
			cacheRegistry.Delete(t.CacheKey)
			continue
		}
		applyCacheHit(t, e)
		cacheHits.Store(namespace+"/"+odagName+"/"+t.Name, e)
		hits++
		log.Printf("[odag-ctrl] cache hit %s/%s: cacheKey=%q satisfied by "+
			"%s/%s on %s — task will not execute",
			odagName, t.Name, t.CacheKey, e.Odag, e.Task, e.Node)
	}
	return hits
}

// executeCachedTask realizes a cache-satisfied task: reset any stale state
// under this run's name, alias the prior run's output across runs, push to
// remote successors, and repoint the registry at the fresh copy. On a
// vanished source the hit is cleared and the task runs as a normal pod on
// the next reconcile — its inputs (if any) were produced normally and are
// already on its node.
func executeCachedTask(namespace, odagName string, task taskSpec, e cacheEntry,
	assignMap map[string]nodeInfo, allTasks []taskSpec) error {

	ni := assignMap[task.Name]
	if ni.ip != "" && !isDataReady(ni.ip, e.Odag, e.Task) {
		log.Printf("[odag-ctrl] cache source %s/%s vanished; %s/%s falls "+
			"back to normal execution", e.Odag, e.Task, odagName, task.Name)
		clearCacheHit(namespace, odagName, task.Name, task.CacheKey)
		return nil
	}
	err := realizeVertex(namespace, odagName, task, e.Odag+"/"+e.Task,
		assignMap, allTasks)
	if err != nil {
		return err
	}
	recordCacheEntry(task.CacheKey, cacheEntry{
		Odag: odagName, Task: task.Name, Node: ni.name,
		Unix: float64(time.Now().UnixNano()) / 1e9,
	})
	return nil
}

// rebuildCacheRegistry repopulates the registry from ODAG objects that
// survived a controller restart: any Succeeded task that declared a
// cacheKey and reports a node is a candidate copy (liveness is re-verified
// at markCacheHits time, so stale rows are harmless).
func rebuildCacheRegistry(dynClient dynamic.Interface, namespace string) {
	list, err := dynClient.Resource(odagGVR).Namespace(namespace).List(
		context.Background(), metav1.ListOptions{})
	if err != nil {
		log.Printf("[odag-ctrl] cache registry rebuild: %v", err)
		return
	}
	n := 0
	for i := range list.Items {
		obj := &list.Items[i]
		keys := map[string]string{} // task name -> cacheKey
		for _, t := range extractTasks(obj) {
			if t.CacheKey != "" {
				keys[t.Name] = t.CacheKey
			}
		}
		if len(keys) == 0 {
			continue
		}
		statuses, _, _ := unstructured.NestedSlice(obj.Object, "status", "tasks")
		for _, raw := range statuses {
			ts, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			name, _ := ts["name"].(string)
			phase, _ := ts["phase"].(string)
			node, _ := ts["node"].(string)
			key := keys[name]
			if key == "" || phase != "Succeeded" || node == "" {
				continue
			}
			unix := 0.0
			if ct, _ := ts["completionTime"].(string); ct != "" {
				if p, err := time.Parse(time.RFC3339Nano, ct); err == nil {
					unix = float64(p.UnixNano()) / 1e9
				}
			}
			recordCacheEntry(key, cacheEntry{
				Odag: obj.GetName(), Task: name, Node: node, Unix: unix,
			})
			n++
		}
	}
	if n > 0 {
		log.Printf("[odag-ctrl] cache registry rebuilt: %d candidate cop(ies)", n)
	}
}
