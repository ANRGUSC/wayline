package main

// Status reporting for a run, one pass at a time.
//
// Dispatch passes run concurrently: every pod event, warm-call completion
// and the 500 ms sweep starts one. Each used to also rebuild status.tasks
// from its own pod snapshot (querying the data agents for every task), write
// the whole list, then check completion. With 29 tasks a pass took seconds,
// passes finished out of order, and an older snapshot overwrote a newer one:
// the task list flapped between 6 and 29 entries, tasks went from Succeeded
// back to Pending, and a run whose last task ended at 37 s was marked
// Succeeded at 66 s. Now a dispatch pass only requests a status pass. A run
// has at most one status pass in flight; requests made during it coalesce
// into one more pass, which takes its snapshot when it starts, so status
// never goes backwards.

import (
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

type statusArgs struct {
	dynClient dynamic.Interface
	client    *kubernetes.Clientset
	namespace string
	odagName  string
	ownerUID  types.UID
	tasks     []taskSpec
}

type statusRunner struct {
	mu      sync.Mutex
	running bool
	again   bool
	latest  statusArgs
}

// statusRunners: "ns/odag" -> *statusRunner.
var statusRunners sync.Map

// statusPassFn runs one status pass (tests replace it).
var statusPassFn = statusPass

// requestStatus asks for a status pass for the run, coalescing with one in
// flight.
func requestStatus(a statusArgs) {
	v, _ := statusRunners.LoadOrStore(a.namespace+"/"+a.odagName, &statusRunner{})
	r := v.(*statusRunner)
	r.mu.Lock()
	r.latest = a
	if r.running {
		r.again = true
		r.mu.Unlock()
		return
	}
	r.running = true
	r.mu.Unlock()
	go func() {
		for {
			r.mu.Lock()
			next := r.latest
			r.again = false
			r.mu.Unlock()
			statusPassFn(next)
			r.mu.Lock()
			if !r.again {
				r.running = false
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
		}
	}()
}

// statusPass writes per-task statuses from a fresh snapshot, then decides
// completion from the same snapshot, then records network flows (slowest,
// informational).
func statusPass(a statusArgs) {
	raw, ok := assignmentCache.Load(a.namespace + "/" + a.odagName)
	if !ok {
		return
	}
	assignMap := raw.(map[string]nodeInfo)
	pods := runPods(a.namespace, a.odagName, a.ownerUID, a.tasks)
	updateTaskStatuses(a.dynClient, a.namespace, a.odagName, pods, assignMap, a.tasks)
	checkODAGCompletion(a.dynClient, a.client, pods, a.namespace, a.odagName, a.ownerUID,
		podTaskCount(a.namespace, a.odagName, a.tasks))
	updateActualFlows(a.dynClient, a.namespace, a.odagName, assignMap, pods)
}

// runPods returns the run's pods from the in-memory cache, plus its warm
// invocations presented as pods.
//
// Pods are matched by the ODAG's UID, not just its name label: when an ODAG
// is deleted and recreated with the same name (common during eval debugging
// cycles), pods from the previous incarnation can linger in the cache, and a
// stale Failed pod from a prior run would falsely fail the new one.
func runPods(namespace, odagName string, ownerUID types.UID, tasks []taskSpec) []corev1.Pod {
	var pods []corev1.Pod
	podCache.Range(func(_, val interface{}) bool {
		p := val.(*corev1.Pod)
		if p.Namespace != namespace || p.Labels[labelODAGName] != odagName {
			return true
		}
		for _, or := range p.OwnerReferences {
			if or.UID == ownerUID {
				pods = append(pods, *p)
				break
			}
		}
		return true
	})
	// Warm invocations have no pod; each is presented as an in-memory pod
	// so dispatch gates, statuses, completion and makespan treat both alike.
	return append(pods, warmPods(namespace, odagName, ownerUID, tasks)...)
}

// podTaskCount is the number of tasks that execute (pods or warm calls):
// data vertices and cache-satisfied tasks run no code.
func podTaskCount(namespace, odagName string, tasks []taskSpec) int {
	succs := successorCounts(tasks)
	n := 0
	for _, t := range tasks {
		if isDataVertex(t, succs[t.Name]) {
			continue
		}
		if _, ok := cacheHitFor(namespace, odagName, t.Name); ok {
			continue
		}
		n++
	}
	return n
}

// dispatchClaims: "ns/odag/uid/task" -> true while the task is dispatched
// (or being dispatched) in this run. A task is dispatched at most once per
// run; a failed dispatch releases its claim so a later pass retries.
var dispatchClaims sync.Map

func dispatchKey(namespace, odagName string, uid types.UID, task string) string {
	return namespace + "/" + odagName + "/" + string(uid) + "/" + task
}

// claimDispatch reports whether this caller won the right to dispatch.
func claimDispatch(key string) bool {
	_, taken := dispatchClaims.LoadOrStore(key, true)
	return !taken
}

func releaseDispatch(key string) { dispatchClaims.Delete(key) }

// forgetDispatches drops a deleted run's claims.
func forgetDispatches(odagKey string) {
	dispatchClaims.Range(func(k, _ any) bool {
		if strings.HasPrefix(k.(string), odagKey+"/") {
			dispatchClaims.Delete(k)
		}
		return true
	})
}
