package main

// Per-object durability: which objects the data agents fsync.
//
// Syncing every object to flash made each task's output handoff wait behind
// other files' large flushes (E13: 7-11 s of stalls on the critical path).
// Most objects are intermediates a crash would make the run redo anyway, so
// only objects whose loss loses real work are made durable.
//
// spec.durability (the run's policy):
//   auto (default)  durable: a run's final outputs (objects nothing consumes)
//                   and outputs with a cacheKey (later runs reuse them);
//                   everything else is written atomically but not synced
//   all             every object durable
//   none            no object durable
//   node            no decision; each agent's --sync default applies
// Overrides: spec.tasks[].durable (the task's default output, and the
// default for its named outputs) and spec.tasks[].outputs[].durable.
// A realization revision with durable: true makes an object durable after
// the fact on every node holding a copy.
//
// The decision reaches the agents through the SDK (WL_DURABLE for the
// default output, WL_OUT_<NAME>_DURABLE for named ones, sent as a header on
// install) and then travels with every copy the agents push.

import (
	"fmt"
	"net/http"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func runDurability(obj *unstructured.Unstructured) string {
	if v, _, _ := unstructured.NestedString(obj.Object, "spec", "durability"); v != "" {
		return v
	}
	return "auto"
}

// hasConsumers reports whether any task reads `output` of `task` ("" = its
// default output).
func hasConsumers(task taskSpec, output string, all []taskSpec) bool {
	for _, t := range all {
		named := false
		for _, in := range t.Inputs {
			if in.Producer != task.Name {
				continue
			}
			named = true
			if in.Object == output {
				return true
			}
		}
		if output == "" && !named {
			for _, d := range t.Dependencies {
				if d == task.Name {
					return true
				}
			}
		}
	}
	return false
}

// objectDurability decides one object's durability. ok is false when the
// agent's node default should apply (policy "node", no override).
func objectDurability(policy string, task taskSpec, output string, all []taskSpec) (durable, ok bool) {
	if output != "" {
		for _, o := range task.Outputs {
			if o.Name == output && o.Durable != nil {
				return *o.Durable, true
			}
		}
	}
	if task.Durable != nil {
		return *task.Durable, true
	}
	switch policy {
	case "all":
		return true, true
	case "none":
		return false, true
	case "node":
		return false, false
	}
	return task.CacheKey != "" || !hasConsumers(task, output, all), true
}

// durabilityEnv is the SDK's view of the decision for every object the task
// produces.
func durabilityEnv(policy string, task taskSpec, all []taskSpec) []corev1.EnvVar {
	flag := func(b bool) string {
		if b {
			return "1"
		}
		return "0"
	}
	var env []corev1.EnvVar
	if d, ok := objectDurability(policy, task, "", all); ok {
		env = append(env, corev1.EnvVar{Name: "WL_DURABLE", Value: flag(d)})
	}
	for _, o := range task.Outputs {
		if d, ok := objectDurability(policy, task, o.Name, all); ok {
			key := strings.ToUpper(strings.NewReplacer("-", "_", ".", "_").Replace(o.Name))
			env = append(env, corev1.EnvVar{Name: fmt.Sprintf("WL_OUT_%s_DURABLE", key), Value: flag(d)})
		}
	}
	return env
}

// syncCopy asks a node's agent to make its installed copy of an object
// durable in place (idempotent).
func syncCopy(nodeIP, odagName, object string) error {
	resp, err := gcHTTP.Post(fmt.Sprintf("http://%s:%d/sync/%s/%s", nodeIP, dataAgentPort, odagName, object),
		"application/json", nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}
