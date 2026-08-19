package main

import (
	"log"
	"sort"
)

// constraintOnlyAssign places every task on a node it is allowed to run on,
// and does nothing else.
//
// This is the neutral placement: it enforces spec.tasks[].constraints.nodeNames
// (the only placement rule the user actually declared) and makes no attempt to
// optimise makespan, locality, or load beyond spreading tasks evenly so a
// single node is not trivially oversubscribed.
//
// It is deliberately the default and the fallback. A framework whose purpose
// is comparing schedulers must not silently substitute a *good* scheduler when
// the requested one is unavailable: falling back to HEFT would mean a failed
// external scheduler quietly produces an optimised placement, and the
// resulting measurement would describe HEFT while claiming to describe the
// scheduler under test. Falling back to the neutral placement is both honest
// and obvious in the numbers.
//
// Deterministic: tasks are visited in spec order and ties break by node name,
// so the same ODAG yields the same placement on every run. Use "random" when
// randomised placement is wanted as a baseline.
func constraintOnlyAssign(tasks []taskSpec, nodeMap map[string]nodeInfo) map[string]nodeInfo {
	allNodes := make([]string, 0, len(nodeMap))
	for name := range nodeMap {
		allNodes = append(allNodes, name)
	}
	sort.Strings(allNodes)

	load := make(map[string]int, len(nodeMap))
	result := make(map[string]nodeInfo, len(tasks))

	for _, t := range tasks {
		candidates := allNodes
		if len(t.Constraints) > 0 {
			var allowed []string
			for _, c := range t.Constraints {
				if _, ok := nodeMap[c]; ok {
					allowed = append(allowed, c)
				}
			}
			sort.Strings(allowed)
			if len(allowed) > 0 {
				candidates = allowed
			} else {
				log.Printf("[constraints] task %s: none of its constrained nodes %v are "+
					"schedulable; falling back to any node", t.Name, t.Constraints)
			}
		}
		if len(candidates) == 0 {
			continue
		}
		// Least-loaded allowed node; ties by name (candidates are sorted).
		best := candidates[0]
		for _, c := range candidates[1:] {
			if load[c] < load[best] {
				best = c
			}
		}
		load[best]++
		result[t.Name] = nodeMap[best]
	}
	return result
}
