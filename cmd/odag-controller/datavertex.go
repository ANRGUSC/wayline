package main

// Data vertices: physical-DAG nodes that hold or move bytes but run no code.
//
// The logical->physical translation (wl.augment) realizes store/checkpoint/
// relay policies by inserting vertices whose only job is to re-emit a
// producer's payload from a chosen node. Executing one needs no container:
// the data-agent already has every verb — the producer's push delivers the
// payload to the vertex's node, an alias materializes it under the vertex's
// name, and a push serves it onward to the vertex's successors. The
// controller drives those verbs directly, so the vertex pays no pod
// dispatch, image pull, or interpreter start.
//
// A task opts in with `type: data` in the ODAG spec. It must have exactly
// one dependency (it re-emits one payload) and at least one successor
// (a sink that runs no code does nothing); anything else falls back to
// pod execution with a warning, so a malformed spec degrades rather than
// wedges.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
)

// dataVertexDone remembers vertices this controller has executed, keyed
// "namespace/odag/task". The agent-side alias is idempotent and .wl-ready on
// the vertex's node is the durable truth, so this is only a cheap guard
// against re-issuing HTTP calls every reconcile tick.
var dataVertexDone sync.Map

// isDataVertex reports whether a task is a well-formed data vertex given
// the number of successors it has in this DAG. Malformed declarations are
// logged once per reconcile and executed as ordinary pods.
func isDataVertex(t taskSpec, successorCount int) bool {
	if t.Type != "data" {
		return false
	}
	if len(t.Dependencies) != 1 || successorCount == 0 {
		log.Printf("[odag-ctrl] task %q: type=data needs exactly 1 dependency "+
			"(has %d) and ≥1 successor (has %d); executing as a pod",
			t.Name, len(t.Dependencies), successorCount)
		return false
	}
	return true
}

// dataVertexExecuted reports whether the vertex's output is installed on its
// own node. Fast path is the in-memory guard; the agent's .wl-ready marker
// is checked so execution state survives a controller restart.
func dataVertexExecuted(ni nodeInfo, namespace, odagName, taskName string) bool {
	key := namespace + "/" + odagName + "/" + taskName
	if _, ok := dataVertexDone.Load(key); ok {
		return true
	}
	if ni.ip != "" && isDataReady(ni.ip, odagName, taskName) {
		dataVertexDone.Store(key, true)
		return true
	}
	return false
}

// executeDataVertex realizes a data vertex on its assigned node: alias the
// dependency's (already delivered) payload under the vertex's name, then
// push it to every remote successor. Both agent operations are idempotent,
// so a partial failure is safe to retry on the next reconcile.
func executeDataVertex(namespace, odagName string, task taskSpec,
	assignMap map[string]nodeInfo, allTasks []taskSpec) error {
	return realizeVertex(namespace, odagName, task,
		odagName+"/"+task.Dependencies[0], assignMap, allTasks)
}

// realizeVertex materializes <odagName>/<task> on the task's assigned node
// by aliasing an output already installed there (aliasFrom, "<odag>/<task>"
// form — same-run for a data vertex, cross-run for a cache hit), then pushes
// it to every remote successor. Idempotent end to end.
func realizeVertex(namespace, odagName string, task taskSpec,
	aliasFrom string, assignMap map[string]nodeInfo, allTasks []taskSpec) error {

	ni := assignMap[task.Name]
	if ni.ip == "" {
		return fmt.Errorf("no data-agent for node %q", ni.name)
	}

	// 1. Materialize <odag>/<task> from aliasFrom on this node.
	aliasBody, _ := json.Marshal(map[string]string{
		"from": aliasFrom,
	})
	aliasURL := fmt.Sprintf("http://%s:%d/alias/%s/%s",
		ni.ip, dataAgentPort, odagName, task.Name)
	resp, err := httpClient.Post(aliasURL, "application/json",
		bytes.NewReader(aliasBody))
	if err != nil {
		return fmt.Errorf("alias: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("alias: HTTP %d", resp.StatusCode)
	}

	// 2. Push to remote successors. Same-node successors read the alias
	// directly, exactly as they would a local producer's output.
	type succ struct {
		Name string `json:"name"`
		Host string `json:"host"`
		Node string `json:"node"`
	}
	var remote []succ
	for _, t := range allTasks {
		for _, d := range t.Dependencies {
			if d != task.Name {
				continue
			}
			sni := assignMap[t.Name]
			if sni.name != "" && sni.name != ni.name {
				remote = append(remote, succ{Name: t.Name, Host: sni.ip, Node: sni.name})
			}
			break
		}
	}
	if len(remote) > 0 {
		pushBody, _ := json.Marshal(map[string]interface{}{"successors": remote})
		pushURL := fmt.Sprintf("http://%s:%d/push/%s/%s",
			ni.ip, dataAgentPort, odagName, task.Name)
		resp, err := httpClient.Post(pushURL, "application/json",
			bytes.NewReader(pushBody))
		if err != nil {
			return fmt.Errorf("push: %w", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("push: HTTP %d", resp.StatusCode)
		}
	}

	dataVertexDone.Store(namespace+"/"+odagName+"/"+task.Name, true)
	log.Printf("[odag-ctrl] data vertex %s/%s executed on %s "+
		"(alias %s, %d remote successor(s), no pod)",
		odagName, task.Name, ni.name, aliasFrom, len(remote))
	return nil
}

// successorCounts returns, for each task name, how many tasks depend on it.
func successorCounts(tasks []taskSpec) map[string]int {
	out := make(map[string]int, len(tasks))
	for _, t := range tasks {
		for _, d := range t.Dependencies {
			out[d]++
		}
	}
	return out
}
