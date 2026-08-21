package main

import "testing"

func TestApplyCacheHit(t *testing.T) {
	task := taskSpec{Name: "prep", CacheKey: "dataset-v1", Runtime: 30,
		Constraints: []string{"anrg-3", "anrg-4"},
		RuntimeProfile: map[string]float64{"anrg-3": 25}}
	applyCacheHit(&task, cacheEntry{Odag: "run-1", Task: "prep", Node: "anrg-7"})
	if len(task.Constraints) != 1 || task.Constraints[0] != "anrg-7" {
		t.Errorf("task not pinned to cache node: %v", task.Constraints)
	}
	if task.Runtime != 0 || task.RuntimeProfile != nil {
		t.Errorf("cache-satisfied task must cost nothing: rt=%v profile=%v",
			task.Runtime, task.RuntimeProfile)
	}
}

func TestRecordCacheEntryLatestWins(t *testing.T) {
	defer cacheRegistry.Delete("k")
	recordCacheEntry("k", cacheEntry{Odag: "old", Unix: 100})
	recordCacheEntry("k", cacheEntry{Odag: "new", Unix: 200})
	recordCacheEntry("k", cacheEntry{Odag: "stale", Unix: 150}) // ignored
	v, _ := cacheRegistry.Load("k")
	if v.(cacheEntry).Odag != "new" {
		t.Errorf("registry should keep newest copy, got %v", v)
	}
}

func TestCacheHitBinding(t *testing.T) {
	key := "ns/run-2/prep"
	defer cacheHits.Delete(key)
	cacheHits.Store(key, cacheEntry{Odag: "run-1", Task: "prep", Node: "n1"})
	if e, ok := cacheHitFor("ns", "run-2", "prep"); !ok || e.Odag != "run-1" {
		t.Errorf("bound hit not found: %v %v", e, ok)
	}
	clearCacheHit("ns", "run-2", "prep", "")
	if _, ok := cacheHitFor("ns", "run-2", "prep"); ok {
		t.Error("hit survived clearCacheHit")
	}
}
