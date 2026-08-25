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
	key := namespace + "/" + odagName + "/" + taskName + "@" + ni.name
	if _, ok := dataVertexDone.Load(key); ok {
		return true
	}
	if ni.ip != "" && isDataReady(ni.ip, odagName, taskName) {
		dataVertexDone.Store(key, true)
		return true
	}
	return false
}

// vertexNode resolves where a data vertex executes: its assigned node,
// unless a runtime revision names a servingCopy for its input object
// whose bytes are installed there — then the vertex serves from that
// node instead. This is the serving-point rebinding of the revision
// interface: the template binds the initial serving node, a policy
// patch rebinds it, and no pod or compute placement changes either way.
func vertexNode(namespace, odagName string, task taskSpec,
	assignMap map[string]nodeInfo) nodeInfo {
	assigned := assignMap[task.Name]
	if len(task.Dependencies) != 1 {
		return assigned
	}
	over := servingOverride(namespace+"/"+odagName, task.Dependencies[0])
	if over == "" || over == assigned.name {
		return assigned
	}
	ipRaw, ok := nodeIPCache.Load(over)
	if !ok {
		log.Printf("[odag-ctrl] vertex %s/%s: servingCopy %q unknown; serving as assigned",
			odagName, task.Name, over)
		return assigned
	}
	// A declared override BINDS the serving point even before its bytes
	// arrive: executing from the assigned node anyway would race the
	// migration onto the very path the policy is avoiding. Execution
	// simply retries until the reconciler lands the copy.
	return nodeInfo{name: over, ip: ipRaw.(string)}
}

// executeDataVertex realizes a data vertex on its serving node: alias the
// dependency's (already delivered) payload under the vertex's name, then
// push it to every remote successor. Both agent operations are idempotent,
// so a partial failure is safe to retry on the next reconcile.
func executeDataVertex(namespace, odagName string, task taskSpec,
	assignMap map[string]nodeInfo, allTasks []taskSpec) error {
	ni := vertexNode(namespace, odagName, task, assignMap)
	assigned := assignMap[task.Name]
	if ni.name != assigned.name && assigned.ip != "" {
		// The serving point moved: revoke the old node's outbound
		// transfers of this object so the revised realization does not
		// compete with the flows it replaces. Idempotent; best-effort.
		cancelURL := fmt.Sprintf("http://%s:%d/cancel/%s/%s",
			assigned.ip, dataAgentPort, odagName, task.Name)
		if resp, err := httpClient.Post(cancelURL, "application/json", nil); err == nil {
			resp.Body.Close()
		}
	}
	return realizeVertexOn(namespace, odagName, task, ni,
		odagName+"/"+task.Dependencies[0], assignMap, allTasks)
}

// realizeVertex materializes <odagName>/<task> on the task's assigned node
// by aliasing an output already installed there (aliasFrom, "<odag>/<task>"
// form — same-run for a data vertex, cross-run for a cache hit), then pushes
// it to every remote successor. Idempotent end to end.
func realizeVertex(namespace, odagName string, task taskSpec,
	aliasFrom string, assignMap map[string]nodeInfo, allTasks []taskSpec) error {
	return realizeVertexOn(namespace, odagName, task, assignMap[task.Name],
		aliasFrom, assignMap, allTasks)
}

func realizeVertexOn(namespace, odagName string, task taskSpec, ni nodeInfo,
	aliasFrom string, assignMap map[string]nodeInfo, allTasks []taskSpec) error {

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
	if resp.StatusCode/100 != 2 {
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
		// The push endpoint answers 202 Accepted: the transfer has begun.
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("push: HTTP %d", resp.StatusCode)
		}
	}

	dataVertexDone.Store(namespace+"/"+odagName+"/"+task.Name+"@"+ni.name, true)
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

// taskByNameIn finds a task spec by name.
func taskByNameIn(tasks []taskSpec, name string) (taskSpec, bool) {
	for _, t := range tasks {
		if t.Name == name {
			return t, true
		}
	}
	return taskSpec{}, false
}
