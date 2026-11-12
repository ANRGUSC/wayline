package main

// Ablation switches: experiment-only, per run. Each disables one enactment
// rule so the evaluation can show what the rule prevents. A run opts in with
// the annotation wl.io/ablate, a comma-separated list of:
//
//	repost     a repeated or newer request (a new generation of the run's
//	           spec) posts a copy that is already in flight again instead of
//	           leaving it to finish (the controller's half of the rule; the
//	           source agent still joins a repeated push to the one in flight)
//	no-cancel  when a revision moves an object's serving copy, the old serving
//	           node's queued and active fan-out is not cancelled
//
// Never set in normal operation; a run without the annotation is unaffected.

import (
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const annotationAblate = "wl.io/ablate"

var runAblations sync.Map // "ns/odag" -> map[string]bool

// noteAblations records the run's ablation switches (or forgets them).
func noteAblations(obj *unstructured.Unstructured) {
	key := runKeyOf(obj)
	if g, ok := runSpecGen.Load(key); !ok || g.(int64) < obj.GetGeneration() {
		runSpecGen.Store(key, obj.GetGeneration())
	}
	raw := obj.GetAnnotations()[annotationAblate]
	if raw == "" {
		runAblations.Delete(key)
		return
	}
	set := map[string]bool{}
	for _, s := range strings.Split(raw, ",") {
		if s = strings.TrimSpace(s); s != "" {
			set[s] = true
		}
	}
	runAblations.Store(key, set)
}

// ablated reports whether the run ("ns/odag") disables the named rule.
func ablated(key, rule string) bool {
	v, ok := runAblations.Load(key)
	return ok && v.(map[string]bool)[rule]
}

// copyPostedGen: "ns/odag/object/node" -> the spec generation whose pass
// posted the copy (read only by the repost ablation).
var copyPostedGen sync.Map

func postedGen(key, object, node string) int64 {
	if v, ok := copyPostedGen.Load(key + "/" + object + "/" + node); ok {
		return v.(int64)
	}
	return 0
}

// runSpecGen: "ns/odag" -> the newest spec generation seen (for the repost
// ablation's "a new request").
var runSpecGen sync.Map

func specGen(key string) int64 {
	if v, ok := runSpecGen.Load(key); ok {
		return v.(int64)
	}
	return 0
}

// forgetAblations drops a deleted run's ("ns/odag") entries.
func forgetAblations(odagKey string) {
	runAblations.Delete(odagKey)
	runSpecGen.Delete(odagKey)
	copyPostedGen.Range(func(k, _ any) bool {
		if strings.HasPrefix(k.(string), odagKey+"/") {
			copyPostedGen.Delete(k)
		}
		return true
	})
}
