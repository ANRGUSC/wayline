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

import (
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

func podCPUMillis(p *corev1.Pod) int64 {
	var total int64
	for _, c := range p.Spec.Containers {
		if q, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
			total += q.MilliValue()
		}
	}
	return total
}

// nodeCPUInUse sums the CPU requested on `node` by every Wayline task that is
// dispatched and not finished, across all runs.
func nodeCPUInUse(node string) int64 {
	var used int64
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
		used += podCPUMillis(p)
		return true
	})
	warmInvocations.Range(func(k, v any) bool {
		inv := v.(*warmInvocation)
		inv.mu.Lock()
		visible[inv.taskKey] = true
		if inv.node == node && inv.finished.IsZero() {
			used += inv.cpuMillis
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
			used += r.cpu
		}
		return true
	})
	return used
}

// admitCPU reserves `cpu` millicores on `node` for task `key` if they fit
// within `capacity` (millicores; <= 0 means unknown, always admit).
func admitCPU(key, node string, cpu, capacity int64) bool {
	if capacity <= 0 || cpu <= 0 {
		return true
	}
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
	return true
}

// releaseCPU drops a reservation whose dispatch failed.
func releaseCPU(key string) { reservations.Delete(key) }
