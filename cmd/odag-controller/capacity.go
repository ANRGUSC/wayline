package main

// Controller-side CPU admission for pods and warm calls together.
//
// Pods are admitted by the kubelet against pod requests; warm calls by their
// runner against its own budget. Runner pods request no CPU, so neither side
// sees the other's tasks, and a node could run a 4-CPU pod and a 4-CPU warm
// call at once on 7.4 free CPUs (measured). The schedulers plan one shared
// capacity per node, so the controller admits every dispatch against one
// account per node: Wayline task pods not yet finished, warm calls not yet
// finished, and dispatches made but not yet visible (reservations). A task is
// dispatched only if its CPU request fits the node's free capacity (the
// capacity the scheduler planned with); otherwise it waits for the next
// dispatch pass. A task larger than the whole node still runs, alone.
//
// A pod's CPU ends when its task does, not when the kubelet reports the pod
// Succeeded: on a busy node that report lags by seconds (about 20 s measured
// on a node pulling a 1 GB image), and until then the finished pod held CPU
// that the next task needed. When a task does not fit, the controller asks
// the node's data agent which counted pods have already reported
// ComputeDone and stops counting them.

import (
	"log"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// admitMu serialises the check-and-reserve step, so two dispatch passes
// cannot both take the last CPUs on a node.
var admitMu sync.Mutex

type cpuReservation struct {
	node string
	cpu  int64
	at   time.Time
}

// reservations: "ns/odag/task" -> cpuReservation, from the admit decision
// until the dispatched pod shows up in the pod cache (or the warm
// invocation in warmInvocations), at most reservationTTL.
var reservations sync.Map

const reservationTTL = 60 * time.Second

// computeDone: pod UID -> true once the pod's task reported ComputeDone or
// Failed to its node's data agent. Keyed by UID, so a retried task's new pod
// counts again. computeQueried rate-limits the agent queries per pod.
var computeDone, computeQueried sync.Map

const computeQueryInterval = 250 * time.Millisecond

// taskStateFn asks a node's data agent for a task's state (tests replace it).
var taskStateFn = queryTaskState

// deferLogged: task key -> last time a deferral was logged (one line per
// task per deferLogInterval, so a waiting task does not flood the log).
var deferLogged sync.Map

const deferLogInterval = 5 * time.Second

func podCPUMillis(p *corev1.Pod) int64 {
	var total int64
	for _, c := range p.Spec.Containers {
		if q, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
			total += q.MilliValue()
		}
	}
	return total
}

// cpuUse is a node's CPU account, split by source for the deferral log.
type cpuUse struct{ pods, warm, reserved int64 }

func (u cpuUse) total() int64 { return u.pods + u.warm + u.reserved }

// nodeCPUInUse sums the CPU requested on `node` by every Wayline task that is
// dispatched and not finished, across all runs.
func nodeCPUInUse(node string) int64 { return nodeCPU(node).total() }

func nodeCPU(node string) cpuUse {
	var u cpuUse
	visible := map[string]bool{}
	// A dispatched task is "visible" (its reservation can go) as soon as its
	// pod or invocation exists, finished or not; its CPU counts only while
	// it runs. (Marking only running ones visible left a fast warm call's
	// reservation counted until the TTL: phantom load that held later tasks
	// back for up to a minute.)
	podCache.Range(func(_, v any) bool {
		p := v.(*corev1.Pod)
		if p.Labels[labelODAGName] == "" {
			return true
		}
		visible[p.Namespace+"/"+p.Labels[labelODAGName]+"/"+p.Labels[labelTaskName]] = true
		if p.Spec.NodeName != node || p.DeletionTimestamp != nil ||
			p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			return true
		}
		if _, done := computeDone.Load(p.UID); done {
			return true
		}
		u.pods += podCPUMillis(p)
		return true
	})
	warmInvocations.Range(func(k, v any) bool {
		inv := v.(*warmInvocation)
		inv.mu.Lock()
		visible[inv.taskKey] = true
		if inv.node == node && inv.finished.IsZero() {
			u.warm += inv.cpuMillis
		}
		inv.mu.Unlock()
		return true
	})
	now := time.Now()
	reservations.Range(func(k, v any) bool {
		r := v.(cpuReservation)
		switch {
		case visible[k.(string)] || now.Sub(r.at) > reservationTTL:
			reservations.Delete(k)
		case r.node == node:
			u.reserved += r.cpu
		}
		return true
	})
	return u
}

// refreshComputeDone asks the node's data agent about every running Wayline
// pod on `node` not yet known to be done, and records those whose task has
// ended. It also forgets pods that left the cache.
func refreshComputeDone(node string) {
	live := map[any]bool{}
	var ask []*corev1.Pod
	now := time.Now()
	podCache.Range(func(_, v any) bool {
		p := v.(*corev1.Pod)
		live[p.UID] = true
		if p.Labels[labelODAGName] == "" || p.Spec.NodeName != node || p.Status.HostIP == "" ||
			p.DeletionTimestamp != nil || p.Status.Phase != corev1.PodRunning {
			return true
		}
		if _, done := computeDone.Load(p.UID); done {
			return true
		}
		if t, ok := computeQueried.Load(p.UID); ok && now.Sub(t.(time.Time)) < computeQueryInterval {
			return true
		}
		ask = append(ask, p)
		return true
	})
	for _, p := range ask {
		computeQueried.Store(p.UID, now)
		switch taskStateFn(p.Status.HostIP, p.Labels[labelODAGName], p.Labels[labelTaskName]) {
		case "ComputeDone", "Failed":
			computeDone.Store(p.UID, true)
		}
	}
	for _, m := range []*sync.Map{&computeDone, &computeQueried} {
		m.Range(func(k, _ any) bool {
			if !live[k] {
				m.Delete(k)
			}
			return true
		})
	}
}

// admitCPU reserves `cpu` millicores on `node` for task `key` if they fit
// within `capacity` (millicores; <= 0 means unknown, always admit).
func admitCPU(key, node string, cpu, capacity int64) bool {
	if capacity <= 0 || cpu <= 0 {
		return true
	}
	if tryAdmit(key, node, cpu, capacity) {
		return true
	}
	// Does not fit: free the CPU of pods whose task already ended (agent
	// queries, outside the lock), then decide again.
	refreshComputeDone(node)
	if tryAdmit(key, node, cpu, capacity) {
		return true
	}
	now := time.Now()
	if t, ok := deferLogged.Load(key); !ok || now.Sub(t.(time.Time)) >= deferLogInterval {
		deferLogged.Store(key, now)
		u := nodeCPU(node)
		log.Printf("[admit] %s waits on %s: needs %dm, in use %dm (pods %dm, warm %dm, reserved %dm) of %dm",
			key, node, cpu, u.total(), u.pods, u.warm, u.reserved, capacity)
	}
	return false
}

func tryAdmit(key, node string, cpu, capacity int64) bool {
	admitMu.Lock()
	defer admitMu.Unlock()
	if _, held := reservations.Load(key); held {
		return true
	}
	used := nodeCPUInUse(node)
	if used > 0 && used+cpu > capacity {
		return false
	}
	reservations.Store(key, cpuReservation{node: node, cpu: cpu, at: time.Now()})
	deferLogged.Delete(key)
	return true
}

// releaseCPU drops a reservation whose dispatch failed.
func releaseCPU(key string) { reservations.Delete(key) }
