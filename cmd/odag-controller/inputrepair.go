package main

// Input repair: the last line of defense for an input that will never arrive.
//
// A task waits until every input object is installed on its node. Normally a
// producer's push or a revision copy brings it. When a task is re-placed
// while its producers are being dispatched, a push can be revoked or aimed at
// the node the task left, and if the copy meant to replace it is lost too the
// task waits forever (E17: a 100-input sink, 4 of ~1500 repair runs). The
// check below notices a task whose producers have all finished but whose
// input is still missing, and, when nothing is on its way to the task's node
// (the producer's push to it revoked, failed, or done elsewhere; no copy in
// flight or delivered), orders a copy from the producer's node. A duplicate
// delivery is harmless: installs are idempotent by digest.

import (
	"log"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// inputRepairAfter is how long a task waits on a finished producer's
// missing input before the transfer states are examined.
const inputRepairAfter = 5 * time.Second

// inputWaits: "ns/odag/uid/task/object" -> when the object was first seen
// missing with its producer finished. inputRepairs: same key -> last copy
// ordered (one per inputRepairAfter at most).
var inputWaits, inputRepairs sync.Map

// inputChecks: "ns/odag/uid/task" -> last check of a blocked task's inputs
// (once a second: a 100-input sink waiting on slow transfers would otherwise
// query its agent for every input on every pass).
var inputChecks sync.Map

// forgetInputWaits drops a deleted run's ("ns/odag") entries.
func forgetInputWaits(odagKey string) {
	prefix := odagKey + "/"
	for _, m := range []*sync.Map{&inputWaits, &inputRepairs, &inputChecks} {
		m.Range(func(k, _ any) bool {
			if len(k.(string)) > len(prefix) && k.(string)[:len(prefix)] == prefix {
				m.Delete(k)
			}
			return true
		})
	}
}

// inFlightStates are transfer states that mean bytes may still arrive.
var inFlightStates = map[string]bool{"Pending": true, "Transferring": true}

// repairMissingInput is called by a dispatch pass for an input object of a
// not-started task that is missing on the task's node while its producer has
// finished. It returns whether it ordered a copy.
func repairMissingInput(runKey, odagName, uid, task, object string, prod, child nodeInfo, now time.Time) bool {
	if prod.ip == "" || child.ip == "" || prod.name == child.name {
		return false
	}
	key := runKey + "/" + uid + "/" + task + "/" + object
	first, _ := inputWaits.LoadOrStore(key, now)
	if now.Sub(first.(time.Time)) < inputRepairAfter {
		return false
	}
	if last, ok := inputRepairs.Load(key); ok && now.Sub(last.(time.Time)) < inputRepairAfter {
		return false
	}
	// Anything on its way to (or already delivered at) the task's node?
	// The producer's push to the task by name may be aimed at an old node, so
	// it counts only while in flight; a revision copy names the node.
	if inFlightStates[transferStateOf(prod.ip, odagName, object, task)] {
		return false
	}
	switch transferStateOf(prod.ip, odagName, object, "rev-"+child.name) {
	case "Pending", "Transferring":
		return false
	}
	if err := pushCopy(prod.ip, odagName, object, child.name, child.ip); err != nil {
		log.Printf("[inputs] %s: %s for %s on %s: copy from %s failed: %v", runKey, object, task, child.name, prod.name, err)
		return false
	}
	inputRepairs.Store(key, now)
	log.Printf("[inputs] %s: %s for %s missing on %s with its producer done; copy ordered from %s",
		runKey, object, task, child.name, prod.name)
	return true
}

// repairBlockedInputs checks every finished producer of a task blocked on
// data: each consumed object missing on the task's node is handed to
// repairMissingInput.
func repairBlockedInputs(runKey, odagName, uid string, task taskSpec, assignMap map[string]nodeInfo,
	child nodeInfo, podPhases map[string]corev1.PodPhase, vertex map[string]bool, cached map[string]cacheEntry) {
	now := time.Now()
	ck := runKey + "/" + uid + "/" + task.Name
	if last, ok := inputChecks.Load(ck); ok && now.Sub(last.(time.Time)) < time.Second {
		return
	}
	inputChecks.Store(ck, now)
	for _, dep := range task.Dependencies {
		if vertex[dep] || cached[dep] != (cacheEntry{}) || podPhases[dep] != corev1.PodSucceeded {
			continue
		}
		for _, obj := range consumedKeys(task, dep) {
			if !isDataReady(child.ip, odagName, obj) {
				repairMissingInput(runKey, odagName, uid, task.Name, obj, assignMap[dep], child, now)
			}
		}
	}
}
