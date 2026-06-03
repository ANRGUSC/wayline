package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

// CRD reference for wl.io/v1/ODAGTemplate
var odagTemplateGVR = schema.GroupVersionResource{
	Group:    "wl.io",
	Version:  "v1",
	Resource: "odagtemplates",
}

// templateCache stores the latest ODAGTemplate objects, keyed by "ns/name".
var templateCache sync.Map

// --------------------------------------------------------------------------
// Template watcher
// --------------------------------------------------------------------------

// watchODAGTemplates watches ODAGTemplate CRs and caches them in memory.
// Each iteration first does a List to prime/refresh the cache (so profiling
// keeps working even if the Watch stream returns an "unknown" error, which
// we've seen happen after CRD schema changes), then opens a Watch.
func watchODAGTemplates(dynClient dynamic.Interface) {
	listAndCache := func() {
		list, err := dynClient.Resource(odagTemplateGVR).Namespace("").List(
			context.Background(), metav1.ListOptions{},
		)
		if err != nil {
			log.Printf("[template] list failed: %v", err)
			return
		}
		seen := make(map[string]bool, len(list.Items))
		for i := range list.Items {
			obj := &list.Items[i]
			key := obj.GetNamespace() + "/" + obj.GetName()
			seen[key] = true
			templateCache.Store(key, obj.DeepCopy())
		}
		// Evict templates that no longer exist.
		templateCache.Range(func(k, _ any) bool {
			if ks, ok := k.(string); ok && !seen[ks] {
				templateCache.Delete(ks)
			}
			return true
		})
		log.Printf("[template] primed cache with %d templates", len(list.Items))
	}

	for {
		listAndCache()
		watcher, err := dynClient.Resource(odagTemplateGVR).Namespace("").Watch(
			context.Background(), metav1.ListOptions{},
		)
		if err != nil {
			log.Printf("[template] error watching ODAGTemplates: %v; re-listing in 30s", err)
			time.Sleep(30 * time.Second)
			continue
		}
		log.Println("[template] watching ODAGTemplate resources")
		for event := range watcher.ResultChan() {
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			key := obj.GetNamespace() + "/" + obj.GetName()
			switch string(event.Type) {
			case "ADDED", "MODIFIED":
				templateCache.Store(key, obj.DeepCopy())
				log.Printf("[template] cached template %s", key)
			case "DELETED":
				templateCache.Delete(key)
				log.Printf("[template] removed template %s", key)
			}
		}
		log.Println("[template] ODAGTemplate watcher closed; reconnecting in 2s")
		time.Sleep(2 * time.Second)
	}
}

// --------------------------------------------------------------------------
// Create a run from a template
// --------------------------------------------------------------------------

// createRunFromTemplate creates a new ODAG CR from an ODAGTemplate.
// It auto-increments the run number and names the ODAG "<template>-run-NNN".
func createRunFromTemplate(dynClient dynamic.Interface, db *sql.DB,
	templateObj *unstructured.Unstructured) (string, error) {

	templateName := templateObj.GetName()
	namespace := templateObj.GetNamespace()

	// Get next run number.
	runNum, err := nextRunID(db, templateName)
	if err != nil {
		return "", fmt.Errorf("get next run ID: %w", err)
	}
	odagName := fmt.Sprintf("%s-run-%03d", templateName, runNum)

	// Extract spec from template, stripping template-only fields.
	spec, _, err := unstructured.NestedMap(templateObj.Object, "spec")
	if err != nil {
		return "", fmt.Errorf("extract template spec: %w", err)
	}
	delete(spec, "profiling")
	delete(spec, "defaults")
	delete(spec, "retention")
	delete(spec, "description")

	// Build the ODAG CR.
	odag := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "wl.io/v1",
			"kind":       "ODAG",
			"metadata": map[string]interface{}{
				"name":      odagName,
				"namespace": namespace,
				"labels": map[string]interface{}{
					"wl.io/template": templateName,
					"wl.io/run":      fmt.Sprintf("%d", runNum),
				},
			},
			"spec": spec,
		},
	}

	if _, err := dynClient.Resource(odagGVR).Namespace(namespace).Create(
		context.Background(), odag, metav1.CreateOptions{},
	); err != nil {
		return "", fmt.Errorf("create ODAG run: %w", err)
	}

	log.Printf("[template] created run %s/%s (run #%d) from template %s",
		namespace, odagName, runNum, templateName)
	return odagName, nil
}

// --------------------------------------------------------------------------
// Profiling config extraction
// --------------------------------------------------------------------------

// profilingConfig holds the profiling settings extracted from an ODAGTemplate.
type profilingConfig struct {
	Enabled         bool
	WarmupRuns      int
	MinSamples      int
	EmaAlpha        float64
	MaxSamples      int
	RuntimeSource   string // "manual" | "profiler" | "hybrid"
	BandwidthSource string // "external" | "profiler" | "hybrid"
}

// defaultProfilingConfig returns the default profiling configuration.
func defaultProfilingConfig() profilingConfig {
	return profilingConfig{
		Enabled:         true,
		WarmupRuns:      0,
		MinSamples:      3,
		EmaAlpha:        0.3,
		MaxSamples:      100,
		RuntimeSource:   "profiler",
		BandwidthSource: "external",
	}
}

// extractProfilingConfig reads the profiling settings from a template object.
func extractProfilingConfig(templateObj *unstructured.Unstructured) profilingConfig {
	cfg := defaultProfilingConfig()
	if templateObj == nil {
		return cfg
	}

	prof, ok, _ := unstructured.NestedMap(templateObj.Object, "spec", "profiling")
	if !ok {
		return cfg
	}

	if v, ok := prof["enabled"].(bool); ok {
		cfg.Enabled = v
	}
	if v, ok, _ := unstructured.NestedInt64(prof, "warmupRuns"); ok {
		cfg.WarmupRuns = int(v)
	}
	if v, ok, _ := unstructured.NestedInt64(prof, "minSamples"); ok {
		cfg.MinSamples = int(v)
	}
	if v, ok := prof["emaAlpha"].(float64); ok {
		cfg.EmaAlpha = v
	} else if v, ok, _ := unstructured.NestedInt64(prof, "emaAlpha"); ok {
		cfg.EmaAlpha = float64(v)
	}
	if v, ok, _ := unstructured.NestedInt64(prof, "maxSamples"); ok {
		cfg.MaxSamples = int(v)
	}
	if v, ok := prof["runtimeSource"].(string); ok && (v == "manual" || v == "profiler" || v == "hybrid") {
		cfg.RuntimeSource = v
	}
	if v, ok := prof["bandwidthSource"].(string); ok && (v == "external" || v == "profiler" || v == "hybrid") {
		cfg.BandwidthSource = v
	}

	return cfg
}

// --------------------------------------------------------------------------
// Scheduler config extraction (spec.schedulerConfig.*)
// --------------------------------------------------------------------------

// schedulerConfig holds tunable knobs for the scheduler (HEFT-specific today).
type schedulerConfig struct {
	// SpreadEpsilon (seconds): when > 0, candidate nodes within ε of the
	// minimum EFT are treated as tied and the least-loaded is chosen.
	// 0 preserves strict EFT selection (with least-loaded exact-tie break).
	SpreadEpsilon float64
}

// extractSchedulerConfig reads spec.schedulerConfig.* from an ODAGTemplate.
// Returns zero-valued config (no spread, strict HEFT) when unset.
func extractSchedulerConfig(templateObj *unstructured.Unstructured) schedulerConfig {
	var cfg schedulerConfig
	if templateObj == nil {
		return cfg
	}
	sc, ok, _ := unstructured.NestedMap(templateObj.Object, "spec", "schedulerConfig")
	if !ok {
		return cfg
	}
	if v, ok := sc["spreadEpsilon"].(float64); ok {
		cfg.SpreadEpsilon = v
	} else if v, ok, _ := unstructured.NestedInt64(sc, "spreadEpsilon"); ok {
		cfg.SpreadEpsilon = float64(v)
	}
	return cfg
}

// extractDefaultRuntime reads spec.defaults.runtime from a template.
func extractDefaultRuntime(templateObj *unstructured.Unstructured) float64 {
	if templateObj == nil {
		return 10.0
	}
	v, ok, _ := unstructured.NestedFloat64(templateObj.Object, "spec", "defaults", "runtime")
	if !ok {
		// Try int64 (CRD stores numbers as int64 when they have no decimal).
		if iv, ok, _ := unstructured.NestedInt64(templateObj.Object, "spec", "defaults", "runtime"); ok {
			return float64(iv)
		}
		return 10.0
	}
	return v
}

// extractDefaultDataSize reads spec.defaults.dataSize from a template.
func extractDefaultDataSize(templateObj *unstructured.Unstructured) string {
	if templateObj == nil {
		return "0"
	}
	v, ok, _ := unstructured.NestedString(templateObj.Object, "spec", "defaults", "dataSize")
	if !ok {
		return "0"
	}
	return v
}

// --------------------------------------------------------------------------
// Data retention config extraction
// --------------------------------------------------------------------------

// dataRetentionConfig holds the on-disk data retention policy from an ODAGTemplate.
type dataRetentionConfig struct {
	Policy         string        // "immediate" | "delayed" | "keepLatest" | "none"
	KeepRuns       int           // number of recent completed runs to keep data for
	MaxSizePerNode int64         // bytes; 0 = no limit
	DeleteDelay    time.Duration // only for "delayed" policy
}

// defaultDataRetentionConfig returns the default data retention configuration.
func defaultDataRetentionConfig() dataRetentionConfig {
	return dataRetentionConfig{
		Policy:         "keepLatest",
		KeepRuns:       3,
		MaxSizePerNode: 0,
		DeleteDelay:    0,
	}
}

// extractDataRetentionConfig reads spec.retention.data from a template.
func extractDataRetentionConfig(templateObj *unstructured.Unstructured) dataRetentionConfig {
	cfg := defaultDataRetentionConfig()
	if templateObj == nil {
		return cfg
	}

	data, ok, _ := unstructured.NestedMap(templateObj.Object, "spec", "retention", "data")
	if !ok {
		return cfg
	}

	if v, ok := data["policy"].(string); ok {
		switch v {
		case "immediate", "delayed", "keepLatest", "none":
			cfg.Policy = v
		}
	}
	if v, ok, _ := unstructured.NestedInt64(data, "keepRuns"); ok {
		cfg.KeepRuns = int(v)
	}
	if v, ok := data["maxSizePerNode"].(string); ok && v != "0" {
		cfg.MaxSizePerNode = parseDataSizeBytes(v)
	}
	if v, ok := data["deleteDelay"].(string); ok && v != "0s" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.DeleteDelay = d
		}
	}

	return cfg
}

// extractRetentionMaxRuns reads spec.retention.maxRuns from a template.
func extractRetentionMaxRuns(templateObj *unstructured.Unstructured) int {
	if templateObj == nil {
		return 50
	}
	v, ok, _ := unstructured.NestedInt64(templateObj.Object, "spec", "retention", "maxRuns")
	if !ok {
		return 50
	}
	return int(v)
}

// --------------------------------------------------------------------------
// Post-completion profiling
// --------------------------------------------------------------------------

// profileCompletedRun records profiler observations from a completed ODAG run.
// Called from checkODAGCompletion when the ODAG has the wl.io/template label.
func profileCompletedRun(dynClient dynamic.Interface, client *kubernetes.Clientset, db *sql.DB,
	namespace, odagName, templateName string, runNum int,
	tasks []taskSpec, assignMap map[string]nodeInfo,
	taskStartTimes, taskCompletionTimes map[string]time.Time,
	makespan float64) {

	// Look up the template from cache.
	key := namespace + "/" + templateName
	raw, ok := templateCache.Load(key)
	if !ok {
		log.Printf("[profiler] template %s not in cache; skipping profiling", key)
		return
	}
	templateObj := raw.(*unstructured.Unstructured)
	cfg := extractProfilingConfig(templateObj)

	if !cfg.Enabled {
		log.Printf("[profiler] profiling disabled for template %s", templateName)
		return
	}

	// Check warmup: skip profiling for early runs.
	if runNum <= cfg.WarmupRuns {
		log.Printf("[profiler] run %d <= warmupRuns %d for %s; skipping", runNum, cfg.WarmupRuns, templateName)
		return
	}

	// Record task profiles.
	for _, t := range tasks {
		ni := assignMap[t.Name]
		start, hasStart := taskStartTimes[t.Name]
		end, hasEnd := taskCompletionTimes[t.Name]
		if !hasStart || !hasEnd {
			continue
		}
		observedRuntime := end.Sub(start).Seconds()

		// Query actual output bytes from the data-agent on the task's node.
		// Falls back to spec hint if the agent doesn't have the data.
		observedDataBytes := float64(queryTaskBytes(ni.ip, odagName, t.Name))
		if observedDataBytes <= 0 {
			observedDataBytes = float64(parseDataSizeBytes(t.DataSize))
		}

		if err := recordTaskProfile(db, templateName, t.Name, ni.name,
			observedRuntime, observedDataBytes, cfg.EmaAlpha, cfg.MaxSamples); err != nil {
			log.Printf("[profiler] error recording task %s: %v", t.Name, err)
		} else {
			log.Printf("[profiler] recorded %s/%s on %s: %.2fs", templateName, t.Name, ni.name, observedRuntime)
		}
		// Also record image-based profile (shared across templates).
		if err := recordImageProfile(db, t.Image, ni.name,
			observedRuntime, observedDataBytes, cfg.EmaAlpha, cfg.MaxSamples); err != nil {
			log.Printf("[profiler] error recording image profile %s: %v", t.Image, err)
		}
	}

	// Record link profiles (data transfers between dependent tasks on different nodes).
	taskByName := make(map[string]*taskSpec, len(tasks))
	for i := range tasks {
		taskByName[tasks[i].Name] = &tasks[i]
	}
	for _, t := range tasks {
		for _, dep := range t.Dependencies {
			srcNode := assignMap[dep].name
			dstNode := assignMap[t.Name].name
			if srcNode == dstNode {
				continue
			}
			depEnd, hasDep := taskCompletionTimes[dep]
			childStart, hasChild := taskStartTimes[t.Name]
			if !hasDep || !hasChild {
				continue
			}
			transferSec := childStart.Sub(depEnd).Seconds()
			if transferSec < 0 {
				transferSec = 0
			}
			dataBytes := float64(parseDataSizeBytes(taskByName[dep].DataSize))

			if err := recordLinkProfile(db, templateName, dep, t.Name, srcNode, dstNode,
				dataBytes, transferSec, cfg.EmaAlpha, cfg.MaxSamples); err != nil {
				log.Printf("[profiler] error recording link %s→%s: %v", dep, t.Name, err)
			}
		}
	}

	// Update template status.
	updateTemplateStatus(dynClient, namespace, templateName, odagName, makespan)

	// Run CR GC if needed.
	maxRuns := extractRetentionMaxRuns(templateObj)
	gcOldRuns(dynClient, namespace, templateName, maxRuns)

	// Run data retention cleanup on all data-agent nodes.
	dataCfg := extractDataRetentionConfig(templateObj)
	if dataCfg.Policy != "none" {
		nodeMap, err := getNodeInfoMap(client)
		if err != nil {
			log.Printf("[data-gc] failed to list nodes: %v", err)
		} else {
			cleanupRunData(dynClient, namespace, templateName, dataCfg, nodeMap)
		}
	}
}

// --------------------------------------------------------------------------
// Template status updates
// --------------------------------------------------------------------------

// updateTemplateStatus updates the ODAGTemplate's status with the latest run info
// and a condensed profile summary.
func updateTemplateStatus(dynClient dynamic.Interface, namespace, templateName, lastRunName string, makespan float64) {
	// Read current runCount.
	tmpl, err := dynClient.Resource(odagTemplateGVR).Namespace(namespace).Get(
		context.Background(), templateName, metav1.GetOptions{},
	)
	if err != nil {
		log.Printf("[template] failed to get template %s for status update: %v", templateName, err)
		return
	}

	runCount, _, _ := unstructured.NestedInt64(tmpl.Object, "status", "runCount")

	// Build profile summary from profiler DB if available.
	var profileSummary map[string]interface{}
	if profilerDB != nil {
		profiles := getTaskProfiles(profilerDB, templateName)
		if len(profiles) > 0 {
			profileSummary = make(map[string]interface{})
			for task, nodeMap := range profiles {
				nodeRuntimes := make(map[string]interface{})
				for node, p := range nodeMap {
					nodeRuntimes[node] = p.Runtime
				}
				profileSummary[task] = nodeRuntimes
			}
		}
	}

	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"runCount":        runCount + 1,
			"lastRunName":     lastRunName,
			"lastRunPhase":    "Succeeded",
			"lastRunMakespan": makespan,
		},
	}
	if profileSummary != nil {
		patch["status"].(map[string]interface{})["profileSummary"] = profileSummary
	}

	data, _ := json.Marshal(patch)
	_, _ = dynClient.Resource(odagTemplateGVR).Namespace(namespace).Patch(
		context.Background(), templateName, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	)
}

// --------------------------------------------------------------------------
// Garbage collection of old runs
// --------------------------------------------------------------------------

// gcOldRuns deletes the oldest ODAG runs for a template if the count exceeds maxRuns.
func gcOldRuns(dynClient dynamic.Interface, namespace, templateName string, maxRuns int) {
	list, err := dynClient.Resource(odagGVR).Namespace(namespace).List(
		context.Background(), metav1.ListOptions{
			LabelSelector: fmt.Sprintf("wl.io/template=%s", templateName),
		},
	)
	if err != nil || len(list.Items) <= maxRuns {
		return
	}

	// Sort by creation time (oldest first).
	sort.Slice(list.Items, func(i, j int) bool {
		ti := list.Items[i].GetCreationTimestamp().Time
		tj := list.Items[j].GetCreationTimestamp().Time
		return ti.Before(tj)
	})

	// Only GC completed runs.
	var candidates []unstructured.Unstructured
	for _, item := range list.Items {
		phase, _, _ := unstructured.NestedString(item.Object, "status", "phase")
		if phase == "Succeeded" || phase == "Failed" {
			candidates = append(candidates, item)
		}
	}

	// Keep maxRuns total (including running ones). Delete oldest completed.
	running := len(list.Items) - len(candidates)
	toDelete := len(list.Items) - maxRuns
	if toDelete <= 0 {
		return
	}
	// Don't delete more than completed candidates.
	if toDelete > len(candidates) {
		toDelete = len(candidates)
	}
	// Keep at least enough room for running ones.
	_ = running

	for i := 0; i < toDelete; i++ {
		name := candidates[i].GetName()
		if err := dynClient.Resource(odagGVR).Namespace(namespace).Delete(
			context.Background(), name, metav1.DeleteOptions{},
		); err != nil {
			log.Printf("[template] GC: failed to delete run %s: %v", name, err)
		} else {
			log.Printf("[template] GC: deleted old run %s", name)
		}
	}
}

// --------------------------------------------------------------------------
// Data retention: on-disk cleanup via data-agent
// --------------------------------------------------------------------------

// dataAgentRunInfo mirrors the JSON returned by GET /runs on the data-agent.
type dataAgentRunInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// cleanupRunData enforces the data retention policy by calling the data-agent on
// each node to delete on-disk data for evicted ODAG runs.
func cleanupRunData(dynClient dynamic.Interface, namespace, templateName string,
	cfg dataRetentionConfig, nodeMap map[string]nodeInfo) {

	// List all completed runs for this template (sorted oldest first).
	list, err := dynClient.Resource(odagGVR).Namespace(namespace).List(
		context.Background(), metav1.ListOptions{
			LabelSelector: fmt.Sprintf("wl.io/template=%s", templateName),
		},
	)
	if err != nil {
		log.Printf("[data-gc] failed to list runs for %s: %v", templateName, err)
		return
	}

	sort.Slice(list.Items, func(i, j int) bool {
		return list.Items[i].GetCreationTimestamp().Time.Before(
			list.Items[j].GetCreationTimestamp().Time)
	})

	// Separate completed runs from running ones.
	var completed []string // ODAG names, oldest first
	active := make(map[string]bool)
	for _, item := range list.Items {
		phase, _, _ := unstructured.NestedString(item.Object, "status", "phase")
		name := item.GetName()
		if phase == "Succeeded" || phase == "Failed" {
			completed = append(completed, name)
		} else {
			active[name] = true
		}
	}

	// Determine which completed runs to evict based on policy.
	var toEvict []string
	switch cfg.Policy {
	case "immediate":
		// Evict all completed runs except the one that just finished (last in list).
		// Actually, evict ALL completed run data — "immediate" means no data kept.
		toEvict = completed

	case "delayed":
		// For delayed, we check completion time. Evict runs completed more than
		// deleteDelay ago. The just-completed run won't be evicted until later.
		for _, item := range list.Items {
			phase, _, _ := unstructured.NestedString(item.Object, "status", "phase")
			if phase != "Succeeded" && phase != "Failed" {
				continue
			}
			ct, _, _ := unstructured.NestedString(item.Object, "status", "completionTime")
			if ct == "" {
				continue
			}
			completionTime, err := time.Parse(time.RFC3339, ct)
			if err != nil {
				continue
			}
			if time.Since(completionTime) > cfg.DeleteDelay {
				toEvict = append(toEvict, item.GetName())
			}
		}

	case "keepLatest":
		// Keep the last K completed runs, evict the rest.
		if len(completed) > cfg.KeepRuns {
			toEvict = completed[:len(completed)-cfg.KeepRuns]
		}

	case "none":
		return
	}

	// Apply maxSizePerNode cap: query each node for per-run sizes, evict oldest
	// until under the cap. This composes with the policy-based eviction above.
	if cfg.MaxSizePerNode > 0 && len(toEvict) < len(completed) {
		toEvict = applySizeCap(templateName, completed, toEvict, active, cfg.MaxSizePerNode, nodeMap)
	}

	if len(toEvict) == 0 {
		return
	}

	// Broadcast DELETE to all data-agent nodes for each evicted run.
	evictSet := make(map[string]bool, len(toEvict))
	for _, name := range toEvict {
		evictSet[name] = true
	}

	for _, ni := range nodeMap {
		if ni.ip == "" {
			continue
		}
		for name := range evictSet {
			deleteRunDataOnNode(ni.ip, name)
		}
	}

	log.Printf("[data-gc] evicted data for %d run(s) of template %s across %d node(s)",
		len(toEvict), templateName, len(nodeMap))
}

// applySizeCap adds additional runs to the eviction list if total data on any
// node exceeds maxSizePerNode. It queries each node's data-agent for sizes and
// evicts oldest completed runs first (excluding active runs and already-evicted ones).
func applySizeCap(templateName string, completed, alreadyEvicted []string,
	active map[string]bool, maxSize int64, nodeMap map[string]nodeInfo) []string {

	evictSet := make(map[string]bool, len(alreadyEvicted))
	for _, name := range alreadyEvicted {
		evictSet[name] = true
	}

	// Query the first reachable node for sizes (all nodes get the same cross-node
	// push data, so any node's size view is representative for the cap check).
	var runs []dataAgentRunInfo
	for _, ni := range nodeMap {
		if ni.ip == "" {
			continue
		}
		var err error
		runs, err = queryNodeRuns(ni.ip, templateName)
		if err == nil {
			break
		}
	}

	if len(runs) == 0 {
		return alreadyEvicted
	}

	// Compute total size excluding already-evicted and active runs.
	runSizeMap := make(map[string]int64, len(runs))
	var totalSize int64
	for _, r := range runs {
		runSizeMap[r.Name] = r.Size
		if !evictSet[r.Name] && !active[r.Name] {
			totalSize += r.Size
		}
	}

	// Evict oldest completed runs until under cap.
	for _, name := range completed {
		if totalSize <= maxSize {
			break
		}
		if evictSet[name] || active[name] {
			continue
		}
		evictSet[name] = true
		totalSize -= runSizeMap[name]
		log.Printf("[data-gc] size cap: evicting %s (freeing %d bytes)", name, runSizeMap[name])
	}

	result := make([]string, 0, len(evictSet))
	for name := range evictSet {
		result = append(result, name)
	}
	return result
}

// queryNodeRuns calls GET /runs?prefix=<template> on a data-agent node.
func queryNodeRuns(nodeIP, templatePrefix string) ([]dataAgentRunInfo, error) {
	url := fmt.Sprintf("http://%s:%d/runs?prefix=%s", nodeIP, dataAgentPort, templatePrefix)
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var runs []dataAgentRunInfo
	if err := json.NewDecoder(resp.Body).Decode(&runs); err != nil {
		return nil, err
	}
	return runs, nil
}

// deleteRunDataOnNode calls DELETE /data/<odag> on a data-agent node.
func deleteRunDataOnNode(nodeIP, odagName string) {
	url := fmt.Sprintf("http://%s:%d/data/%s", nodeIP, dataAgentPort, odagName)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("[data-gc] DELETE %s on %s: %v", odagName, nodeIP, err)
		return
	}
	resp.Body.Close()
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

// getTemplateForODAG returns the cached ODAGTemplate for an ODAG if it has the
// wl.io/template label. Returns nil if no template is associated.
func getTemplateForODAG(obj *unstructured.Unstructured) *unstructured.Unstructured {
	labels := obj.GetLabels()
	templateName := labels["wl.io/template"]
	if templateName == "" {
		return nil
	}
	key := obj.GetNamespace() + "/" + templateName
	raw, ok := templateCache.Load(key)
	if !ok {
		return nil
	}
	return raw.(*unstructured.Unstructured)
}

// getRunNumber returns the run number from an ODAG's wl.io/run label, or 0.
func getRunNumber(obj *unstructured.Unstructured) int {
	labels := obj.GetLabels()
	s := labels["wl.io/run"]
	if s == "" {
		return 0
	}
	var n int
	fmt.Sscanf(s, "%d", &n)
	return n
}

// templateNameFromLabels extracts the template name from ODAG labels.
func templateNameFromLabels(labels map[string]string) string {
	if labels == nil {
		return ""
	}
	return labels["wl.io/template"]
}

// isTemplateRun returns true if the ODAG labels indicate it was created from a template.
func isTemplateRun(labels map[string]string) bool {
	return templateNameFromLabels(labels) != ""
}

// getODAGLabels extracts labels from an unstructured object.
func getODAGLabels(obj *unstructured.Unstructured) map[string]string {
	return obj.GetLabels()
}

// filterNonEmpty removes empty strings from a slice. Useful for split results.
func filterNonEmpty(ss []string) []string {
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		if strings.TrimSpace(s) != "" {
			out = append(out, s)
		}
	}
	return out
}
