package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// CRD reference for wl.io/v1/ODAG
var odagGVR = schema.GroupVersionResource{
	Group:    "wl.io",
	Version:  "v1",
	Resource: "odags",
}

const (
	labelODAGName  = "wl-odag"
	labelTaskName  = "wl-task"
	dataOutputPath = "/data/wl-outputs"
	dataAgentPort  = 8082
)

// nodeInfo holds both the node name and its internal IP (needed for cross-node
// data fetches via the data-agent DaemonSet).
type nodeInfo struct {
	name      string
	ip        string
	cpuMillis int64 // allocatable CPU in millicores (e.g. 4000 = 4 cores)
	memBytes  int64 // allocatable memory in bytes
}

// assignmentCache stores task→nodeInfo assignments keyed by "namespace/odagName".
// Populated in deployODAG, read in processReadyTasks.
var assignmentCache sync.Map // "ns/name" -> map[string]nodeInfo

// podCache stores the latest pod state for every ODAG pod, keyed by pod name.
// Updated by watchPods on every event, read by processReadyTasks.
var podCache sync.Map // "ns/podName" -> *corev1.Pod

// profilerDB is the SQLite database for task/link profiling.
// Initialized in main(); nil if profiling is not configured.
var profilerDB *sql.DB

// processedODAGs prevents double-deploying the same ODAG on reconnect.
var processedODAGs sync.Map // "ns/name" -> bool

// runningODAGs tracks ODAGs currently in Running/Scheduling/Pending phase
// so the status poller knows which ones to refresh.
var runningODAGs sync.Map // "ns/name" -> bool

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	var kubeconfig string
	var dbPath string
	flag.StringVar(&kubeconfig, "kubeconfig", "", "path to kubeconfig (leave empty for in-cluster)")
	flag.StringVar(&dbPath, "db", envOrDefault("WL_PROFILER_DB", "/data/wl-profiler.db"), "profiler SQLite database path")
	flag.Parse()

	cfg, err := buildConfig(kubeconfig)
	if err != nil {
		log.Fatalf("[odag-ctrl] failed to build config: %v", err)
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("[odag-ctrl] failed to create kubernetes client: %v", err)
	}
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("[odag-ctrl] failed to create dynamic client: %v", err)
	}

	// Initialize profiler database.
	profilerDB, err = initProfilerDB(dbPath)
	if err != nil {
		log.Printf("[odag-ctrl] WARNING: profiler DB init failed: %v (profiling disabled)", err)
	}

	log.Println("[odag-ctrl] starting odag-controller (layer-by-layer, file transport)")

	// On startup, reconcile stale ODAGs whose status.phase is still
	// Running/Scheduling/Pending but whose task pods no longer exist
	// (typically left over by a previous controller crash or rollout).
	reconcileStaleODAGs(dynClient, client)

	go watchBandwidthConfigMap(client)
	go watchODAGTemplates(dynClient)
	go watchODAGs(dynClient, client)
	go pollRunningODAGs(dynClient, client)
	watchPods(client, dynClient)
}

// reconcileStaleODAGs marks any ODAG stuck in Running/Scheduling/Pending with
// no live task pods as Failed. Runs once at controller startup.
func reconcileStaleODAGs(dynClient dynamic.Interface, client *kubernetes.Clientset) {
	list, err := dynClient.Resource(odagGVR).Namespace("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		log.Printf("[odag-ctrl] startup reconcile: list ODAGs failed: %v", err)
		return
	}
	stale := 0
	for i := range list.Items {
		obj := &list.Items[i]
		ns, name := obj.GetNamespace(), obj.GetName()
		phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
		if phase != "Running" && phase != "Scheduling" && phase != "Pending" {
			continue
		}
		pods, err := client.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{
			LabelSelector: labelODAGName + "=" + name,
		})
		if err != nil {
			log.Printf("[odag-ctrl] startup reconcile: list pods for %s/%s failed: %v", ns, name, err)
			continue
		}
		liveCount, okCount, badCount := 0, 0, 0
		for _, p := range pods.Items {
			switch p.Status.Phase {
			case corev1.PodRunning, corev1.PodPending:
				liveCount++
			case corev1.PodSucceeded:
				okCount++
			case corev1.PodFailed:
				badCount++
			}
		}
		if liveCount > 0 {
			// Legitimately in progress from a prior instance; leave alone.
			runningODAGs.Store(ns+"/"+name, true)
			continue
		}
		total := okCount + badCount
		switch {
		case total == 0:
			updateODAGPhase(dynClient, ns, name, "Failed", "stale: no pods found at controller startup")
		case badCount == 0 && total > 0:
			updateODAGPhase(dynClient, ns, name, "Succeeded", "reconciled at controller startup: all pods succeeded")
		default:
			updateODAGPhase(dynClient, ns, name, "Failed", "reconciled at controller startup: one or more pods failed")
		}
		stale++
	}
	if stale > 0 {
		log.Printf("[odag-ctrl] startup reconcile: finalized %d stale ODAG(s)", stale)
	}
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	var cfg *rest.Config
	var err error
	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			cfg, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
		}
	}
	if err != nil {
		return nil, err
	}
	// Raise client-side rate limits (defaults: QPS=5, Burst=10) so that
	// multi-ODAG workloads don't stall on client-side throttling.
	cfg.QPS = 50
	cfg.Burst = 100
	return cfg, nil
}

// --------------------------------------------------------------------------
// ODAG watcher
// --------------------------------------------------------------------------

// pollRunningODAGs periodically refreshes task statuses for all Running ODAGs
// so that fast-changing fields like sending are captured between pod events.
func pollRunningODAGs(dynClient dynamic.Interface, client *kubernetes.Clientset) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		runningODAGs.Range(func(k, _ any) bool {
			parts := strings.SplitN(k.(string), "/", 2)
			if len(parts) != 2 {
				return true
			}
			ns, name := parts[0], parts[1]
			odagObj, err := dynClient.Resource(odagGVR).Namespace(ns).Get(
				context.Background(), name, metav1.GetOptions{},
			)
			if err != nil {
				return true
			}
			go processReadyTasks(dynClient, client, ns, name, odagObj)
			return true
		})
	}
}

func watchODAGs(dynClient dynamic.Interface, client *kubernetes.Clientset) {
	for {
		watcher, err := dynClient.Resource(odagGVR).Namespace("").Watch(
			context.Background(), metav1.ListOptions{},
		)
		if err != nil {
			log.Printf("[odag-ctrl] error watching ODAGs: %v; retrying in 5s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Println("[odag-ctrl] watching ODAG resources")
		for event := range watcher.ResultChan() {
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			switch string(event.Type) {
			case "ADDED":
				go deployODAG(dynClient, client, obj)
			case "DELETED":
				key := obj.GetNamespace() + "/" + obj.GetName()
				processedODAGs.Delete(key)
				assignmentCache.Delete(key)
			}
		}
		log.Println("[odag-ctrl] ODAG watcher closed; reconnecting in 2s")
		time.Sleep(2 * time.Second)
	}
}

// --------------------------------------------------------------------------
// Deploy: called once when a new ODAG CR is created.
// Assigns tasks to nodes, caches the assignment, then launches layer 0.
// --------------------------------------------------------------------------

func deployODAG(dynClient dynamic.Interface, client *kubernetes.Clientset, obj *unstructured.Unstructured) {
	odagName := obj.GetName()
	namespace := obj.GetNamespace()
	if namespace == "" {
		namespace = "default"
	}
	key := namespace + "/" + odagName

	// Stamp wl.io/run from the SQL counter if missing. The CLI and ui-server
	// create ODAGs via generateName without computing a run number, so the
	// controller is the single source of truth. Doing this before the phase
	// gate is intentional: even on restart, an ODAG that never got stamped
	// gets stamped now, so the profiler and UI see a stable run number.
	ensureRunLabel(dynClient, obj)

	// Phase gate: only fresh (unphased) ODAGs get scheduled.
	// Prevents re-execution of already-running, completed, or failed ODAGs
	// when the watcher re-delivers ADDED events (e.g., on controller restart).
	if phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase"); phase != "" {
		// Already processed by a prior controller instance — mirror to
		// in-memory caches so poller and status updates still work.
		if phase == "Scheduling" || phase == "Running" || phase == "Pending" {
			runningODAGs.Store(key, true)
		}
		processedODAGs.Store(key, true)
		return
	}

	if _, loaded := processedODAGs.LoadOrStore(key, true); loaded {
		return
	}

	log.Printf("[odag-ctrl] deploying ODAG %s", key)

	tasks := extractTasks(obj)
	if len(tasks) == 0 {
		log.Printf("[odag-ctrl] ODAG %s has no tasks; skipping", key)
		return
	}

	updateODAGPhase(dynClient, namespace, odagName, "Scheduling", "assigning tasks to nodes")

	nodeMap, err := getNodeInfoMap(client)
	if err != nil || len(nodeMap) == 0 {
		updateODAGPhase(dynClient, namespace, odagName, "Failed", "failed to list schedulable nodes")
		return
	}

	// Build runtime/dataSize/bandwidth resolvers from profiler + template config.
	var rtRes runtimeResolver
	var dsRes dataSizeResolver
	var bwRes bandwidthResolver
	templateObj := getTemplateForODAG(obj)
	if templateObj != nil && profilerDB != nil {
		tplName := obj.GetLabels()["wl.io/template"]
		cfg := extractProfilingConfig(templateObj)
		defaultRT := extractDefaultRuntime(templateObj)
		defaultDS := extractDefaultDataSize(templateObj)
		rtRes = buildRuntimeResolver(profilerDB, tplName, tasks, cfg.MinSamples, defaultRT, cfg.RuntimeSource)
		dsRes = buildDataSizeResolver(profilerDB, tplName, tasks, cfg.MinSamples, defaultDS, cfg.RuntimeSource)
		bwRes = buildBandwidthResolver(profilerDB, cfg.MinSamples, cfg.BandwidthSource)
		log.Printf("[odag-ctrl] resolvers for %s: runtime=%s, bandwidth=%s (template: %s)",
			key, cfg.RuntimeSource, cfg.BandwidthSource, tplName)
	} else {
		// Non-template ODAG: use ConfigMap bandwidth only.
		bwRes = buildBandwidthResolver(nil, 3, "external")
	}

	schedulerName, _, _ := unstructured.NestedString(obj.Object, "spec", "scheduler")
	schedCfg := extractSchedulerConfig(templateObj)
	var assignMap map[string]nodeInfo
	var predicted []predictedTaskEntry
	var flows []predictedFlowEntry
	switch schedulerName {
	case "heft":
		log.Printf("[odag-ctrl] using HEFT scheduler for %s (spreadEpsilon=%.2fs)", key, schedCfg.SpreadEpsilon)
		hr := heftAssignTasks(tasks, nodeMap, rtRes, dsRes, bwRes, heftOptions{SpreadEpsilon: schedCfg.SpreadEpsilon})
		assignMap = hr.assignMap
		// Use HEFT's own schedule directly (avoids recomputation order mismatch).
		for _, t := range tasks {
			if entry, ok := hr.schedule[t.Name]; ok {
				predicted = append(predicted, predictedTaskEntry{
					Name:     t.Name,
					Node:     entry.Node,
					EstStart: entry.EstStart,
					EstEnd:   entry.EstEnd,
				})
			}
		}
		for _, f := range hr.flows {
			flows = append(flows, predictedFlowEntry{
				FromTask: f.FromTask,
				ToTask:   f.ToTask,
				SrcNode:  f.SrcNode,
				DstNode:  f.DstNode,
				Start:    f.Start,
				End:      f.End,
				DataSize: f.DataSize,
			})
		}
	default:
		log.Printf("[odag-ctrl] using random scheduler for %s", key)
		assignMap = assignTasks(tasks, nodeMap)
		predicted, flows = computePredictedSchedule(tasks, assignMap, rtRes, dsRes, bwRes)
	}
	assignmentCache.Store(key, assignMap)

	writePredictedSchedule(dynClient, namespace, odagName, predicted, flows)

	log.Printf("[odag-ctrl] task placement for %s:", key)
	for task, ni := range assignMap {
		log.Printf("[odag-ctrl]   %-20s -> %s (%s)", task, ni.name, ni.ip)
	}

	// Clear any stale DataReady states on child nodes left over from a
	// previous run of the same ODAG (same name → same hostPath files).
	// For every (dep → child) edge, reset dep's state on the child's node
	// so the controller doesn't see stale DataReady and launch tasks early.
	for _, task := range tasks {
		childNi := assignMap[task.Name]
		if childNi.ip == "" {
			continue
		}
		for _, dep := range task.Dependencies {
			resetTaskState(childNi.ip, odagName, dep)
		}
	}

	// Also clear the data-agent flow log on every involved node, so stale
	// records from a previous run of the same ODAG name don't leak into
	// this run's chart.
	for _, ni := range assignMap {
		if ni.ip != "" {
			go deleteFlows(ni.ip, odagName)
		}
	}

	updateODAGPhase(dynClient, namespace, odagName, "Running", "")
	processReadyTasks(dynClient, client, namespace, odagName, obj)
}

// --------------------------------------------------------------------------
// Pod watcher: triggers layer-by-layer progression on pod completions.
// --------------------------------------------------------------------------

func watchPods(client *kubernetes.Clientset, dynClient dynamic.Interface) {
	for {
		watcher, err := client.CoreV1().Pods("").Watch(
			context.Background(),
			metav1.ListOptions{LabelSelector: labelODAGName},
		)
		if err != nil {
			log.Printf("[odag-ctrl] error watching pods: %v; retrying in 5s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		for event := range watcher.ResultChan() {
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}

			// Update pod cache on every event (ADDED, MODIFIED, DELETED).
			podKey := pod.Namespace + "/" + pod.Name
			if string(event.Type) == "DELETED" {
				podCache.Delete(podKey)
				continue
			}
			podCache.Store(podKey, pod.DeepCopy())

			if string(event.Type) != "MODIFIED" {
				continue
			}
			odagName := pod.Labels[labelODAGName]
			if odagName == "" {
				continue
			}
			ns := pod.Namespace

			// Fetch the ODAG CR to get ownerUID — we need it to create new pods.
			odagObj, err := dynClient.Resource(odagGVR).Namespace(ns).Get(
				context.Background(), odagName, metav1.GetOptions{},
			)
			if err != nil {
				continue
			}
			go processReadyTasks(dynClient, client, ns, odagName, odagObj)
		}
		time.Sleep(2 * time.Second)
	}
}

// --------------------------------------------------------------------------
// processReadyTasks: the core layer-by-layer scheduling loop.
//
// Called after every pod state change. For each task in the spec:
//   - skip if a pod already exists for it
//   - create a pod if ALL its dependencies have Succeeded
//
// Also updates per-task statuses and checks for overall completion.
// --------------------------------------------------------------------------

func processReadyTasks(dynClient dynamic.Interface, client *kubernetes.Clientset,
	namespace, odagName string, odagObj *unstructured.Unstructured) {

	key := namespace + "/" + odagName
	ownerUID := odagObj.GetUID()

	// Retrieve cached assignment.
	raw, ok := assignmentCache.Load(key)
	if !ok {
		return // ODAG not yet fully initialized
	}
	assignMap := raw.(map[string]nodeInfo)

	// Extract tasks from the passed ODAG object (no extra API call needed).
	tasks := extractTasks(odagObj)

	// Collect pods for this ODAG from the in-memory cache (no API call).
	// Filter by OwnerReferences UID, not just the ODAG name label — when an
	// ODAG is deleted and recreated with the same name (common during eval
	// debugging cycles), pods from the previous incarnation can linger in
	// the cache. Without the UID check, a stale Failed pod from a prior
	// run would falsely trigger the failed-state aggregator on the new
	// run. UID match is atomic per ODAG instance.
	var podItems []corev1.Pod
	podCache.Range(func(_, val interface{}) bool {
		p := val.(*corev1.Pod)
		if p.Namespace != namespace || p.Labels[labelODAGName] != odagName {
			return true
		}
		ownedByThisODAG := false
		for _, or := range p.OwnerReferences {
			if or.UID == ownerUID {
				ownedByThisODAG = true
				break
			}
		}
		if ownedByThisODAG {
			podItems = append(podItems, *p)
		}
		return true
	})

	// Build a map of which tasks already have pods, and their current pod phase.
	existingPods := make(map[string]bool)
	podPhases := make(map[string]corev1.PodPhase)
	for _, pod := range podItems {
		taskName := pod.Labels[labelTaskName]
		existingPods[taskName] = true
		podPhases[taskName] = pod.Status.Phase
	}

	// For each task: if it has no pod yet AND all dependencies have DataReady on
	// THIS task's node (Proposal-1 per-child-node DataReady), create its pod now.
	for _, task := range tasks {
		if existingPods[task.Name] {
			continue
		}
		childNi := assignMap[task.Name]
		allDepsDone := true
		for _, dep := range task.Dependencies {
			if !existingPods[dep] {
				// Dep pod hasn't been created yet — not ready.
				allDepsDone = false
				break
			}
			if childNi.ip != "" {
				// Check DataReady on this child's node: has dep's data arrived here?
				if !isDataReady(childNi.ip, odagName, dep) {
					allDepsDone = false
					break
				}
			} else {
				// No data-agent reachable for child: fall back to dep PodSucceeded.
				if podPhases[dep] != corev1.PodSucceeded {
					allDepsDone = false
					break
				}
			}
		}
		if !allDepsDone {
			continue
		}

		ni := assignMap[task.Name]
		if ni.ip != "" {
			resetTaskState(ni.ip, odagName, task.Name)
		}
		envVars := buildEnvVars(odagName, task, assignMap, tasks)
		envVars = addTemplateEnvVars(envVars, odagObj.GetLabels())
		if err := ensurePod(client, namespace, odagName, task, ni.name, envVars, ownerUID); err != nil {
			log.Printf("[odag-ctrl] error creating pod for %s/%s: %v", key, task.Name, err)
		} else {
			log.Printf("[odag-ctrl] launched task %s on node %s", task.Name, ni.name)
		}
	}

	// Update per-task statuses and check overall completion.
	updateTaskStatuses(dynClient, namespace, odagName, podItems, assignMap, tasks)
	updateActualFlows(dynClient, namespace, odagName, assignMap, podItems)
	checkODAGCompletion(dynClient, client, podItems, namespace, odagName, len(tasks))
}

// --------------------------------------------------------------------------
// Helpers: extract task specs from unstructured ODAG CR
// --------------------------------------------------------------------------

type taskSpec struct {
	Name           string
	Image          string
	Command        []string
	Args           []string
	Dependencies   []string
	DataSize       string
	Runtime        float64
	RuntimeProfile map[string]float64 // node name -> runtime (seconds)
	CPU            string
	Memory         string
	Constraints    []string
	UserEnv        []corev1.EnvVar
	// Raw K8s pod-spec passthrough. User volumes are appended after the
	// controller's base mounts (wl-outputs, wl-shared); reserved names are
	// rejected at parse time. SecurityContext is applied at the pod level.
	Volumes         []corev1.Volume
	VolumeMounts    []corev1.VolumeMount
	SecurityContext *corev1.PodSecurityContext
}

// reservedVolumeNames are the volumes the controller owns; user task specs
// may not declare volumes or volumeMounts with these names.
var reservedVolumeNames = map[string]bool{
	"wl-outputs": true,
	"wl-shared":  true,
}

// parseVolumes round-trips the unstructured value through JSON into typed
// corev1.Volume. Drops (with a warning) entries whose name collides with a
// controller-owned mount. Returns nil on any malformed input.
func parseVolumes(raw interface{}, taskName string) []corev1.Volume {
	if raw == nil {
		return nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		log.Printf("[odag-ctrl] WARN task=%s: marshal volumes: %v", taskName, err)
		return nil
	}
	var vols []corev1.Volume
	if err := json.Unmarshal(b, &vols); err != nil {
		log.Printf("[odag-ctrl] WARN task=%s: unmarshal volumes: %v", taskName, err)
		return nil
	}
	out := make([]corev1.Volume, 0, len(vols))
	for _, v := range vols {
		if v.Name == "" {
			log.Printf("[odag-ctrl] WARN task=%s: skipping volume with empty name", taskName)
			continue
		}
		if reservedVolumeNames[v.Name] {
			log.Printf("[odag-ctrl] WARN task=%s: dropping volume %q (reserved name)", taskName, v.Name)
			continue
		}
		out = append(out, v)
	}
	return out
}

// parseVolumeMounts mirrors parseVolumes for corev1.VolumeMount.
func parseVolumeMounts(raw interface{}, taskName string) []corev1.VolumeMount {
	if raw == nil {
		return nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		log.Printf("[odag-ctrl] WARN task=%s: marshal volumeMounts: %v", taskName, err)
		return nil
	}
	var mounts []corev1.VolumeMount
	if err := json.Unmarshal(b, &mounts); err != nil {
		log.Printf("[odag-ctrl] WARN task=%s: unmarshal volumeMounts: %v", taskName, err)
		return nil
	}
	out := make([]corev1.VolumeMount, 0, len(mounts))
	for _, m := range mounts {
		if m.Name == "" || m.MountPath == "" {
			log.Printf("[odag-ctrl] WARN task=%s: skipping volumeMount with empty name or mountPath", taskName)
			continue
		}
		if reservedVolumeNames[m.Name] {
			log.Printf("[odag-ctrl] WARN task=%s: dropping volumeMount %q (reserved name)", taskName, m.Name)
			continue
		}
		out = append(out, m)
	}
	return out
}

// parsePodSecurityContext round-trips the unstructured value into a typed
// corev1.PodSecurityContext.
func parsePodSecurityContext(raw interface{}, taskName string) *corev1.PodSecurityContext {
	if raw == nil {
		return nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		log.Printf("[odag-ctrl] WARN task=%s: marshal securityContext: %v", taskName, err)
		return nil
	}
	var sc corev1.PodSecurityContext
	if err := json.Unmarshal(b, &sc); err != nil {
		log.Printf("[odag-ctrl] WARN task=%s: unmarshal securityContext: %v", taskName, err)
		return nil
	}
	return &sc
}

func extractTasks(obj *unstructured.Unstructured) []taskSpec {
	rawTasks, _, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	tasks := make([]taskSpec, 0, len(rawTasks))
	for _, raw := range rawTasks {
		t, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := t["name"].(string)
		image, _ := t["image"].(string)
		if name == "" || image == "" {
			continue
		}
		cmd, _, _ := unstructured.NestedStringSlice(t, "command")
		args, _, _ := unstructured.NestedStringSlice(t, "args")
		deps, _, _ := unstructured.NestedStringSlice(t, "dependencies")
		dataSize, _ := t["dataSize"].(string)
		runtime, _ := t["runtime"].(int64)
		// Also try float64 (CRD stores number as float when it has decimals).
		var runtimeF float64
		if runtime > 0 {
			runtimeF = float64(runtime)
		} else if rf, ok := t["runtime"].(float64); ok {
			runtimeF = rf
		}

		// Parse per-node runtime profile: {"anrg-3": 6, "anrg-8": 12}
		var rtProfile map[string]float64
		if rp, ok := t["runtimeProfile"].(map[string]interface{}); ok {
			rtProfile = make(map[string]float64, len(rp))
			for node, val := range rp {
				switch v := val.(type) {
				case float64:
					rtProfile[node] = v
				case int64:
					rtProfile[node] = float64(v)
				}
			}
		}

		constraints, _, _ := unstructured.NestedStringSlice(t, "constraints", "nodeNames")
		cpu, _, _ := unstructured.NestedString(t, "resources", "cpu")
		mem, _, _ := unstructured.NestedString(t, "resources", "memory")

		var userEnv []corev1.EnvVar
		if envList, ok := t["env"].([]interface{}); ok {
			for _, e := range envList {
				if em, ok := e.(map[string]interface{}); ok {
					n, _ := em["name"].(string)
					v, _ := em["value"].(string)
					if n != "" {
						userEnv = append(userEnv, corev1.EnvVar{Name: n, Value: v})
					}
				}
			}
		}

		vols := parseVolumes(t["volumes"], name)
		vmounts := parseVolumeMounts(t["volumeMounts"], name)
		secCtx := parsePodSecurityContext(t["securityContext"], name)

		tasks = append(tasks, taskSpec{
			Name:           name,
			Image:          image,
			Command:        cmd,
			Args:           args,
			Dependencies:   deps,
			DataSize:       dataSize,
			Runtime:        runtimeF,
			RuntimeProfile: rtProfile,
			Constraints:    constraints,
			CPU:            cpu,
			Memory:         mem,
			UserEnv:        userEnv,
			Volumes:        vols,
			VolumeMounts:   vmounts,
			SecurityContext: secCtx,
		})
	}
	return tasks
}

// getNodeInfoMap returns a map of node name -> nodeInfo for all schedulable nodes.
func getNodeInfoMap(client *kubernetes.Clientset) (map[string]nodeInfo, error) {
	nodeList, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{
		FieldSelector: "spec.unschedulable!=true",
	})
	if err != nil {
		return nil, err
	}
	result := make(map[string]nodeInfo)
	for _, n := range nodeList.Items {
		noSchedule := false
		for _, taint := range n.Spec.Taints {
			if taint.Effect == corev1.TaintEffectNoSchedule {
				noSchedule = true
				break
			}
		}
		if noSchedule {
			continue
		}
		ip := ""
		for _, addr := range n.Status.Addresses {
			if addr.Type == corev1.NodeInternalIP {
				ip = addr.Address
				break
			}
		}
		// Read allocatable resources for resource-aware scheduling.
		cpuMillis := int64(0)
		memBytes := int64(0)
		if cpu, ok := n.Status.Allocatable[corev1.ResourceCPU]; ok {
			cpuMillis = cpu.MilliValue()
		}
		if mem, ok := n.Status.Allocatable[corev1.ResourceMemory]; ok {
			memBytes = mem.Value()
		}
		result[n.Name] = nodeInfo{name: n.Name, ip: ip, cpuMillis: cpuMillis, memBytes: memBytes}
	}
	return result, nil
}


// buildEnvVars builds file-transport environment variables for a task pod.
//
// Every task receives:
//
//	WL_TRANSPORT_PATTERN=file
//	WL_ODAG_NAME
//	WL_TASK_NAME
//	WL_OUTPUT_DIR       path where this task should write its output
//	WL_DEPS             comma-separated upstream dependency names
//	NODE_NAME            downward API: the node this pod is running on
//
// For each upstream dependency <dep>:
//
//	WL_DEP_<DEP>_NODE   node name where that dep ran (informational)
//
// For each downstream successor <succ>:
//
//	WL_SUCCESSORS            comma-separated successor task names
//	WL_SUCC_<SUCC>_NODE      node name where that successor will run
//	WL_SUCC_<SUCC>_HOST      internal IP of that node (for data-agent PUT)
func buildEnvVars(odagName string, task taskSpec, assignMap map[string]nodeInfo, allTasks []taskSpec) []corev1.EnvVar {
	outputDir := fmt.Sprintf("%s/%s/%s", dataOutputPath, odagName, task.Name)

	// Compute which tasks depend on this task (its successors).
	var successorNames []string
	for _, t := range allTasks {
		for _, dep := range t.Dependencies {
			if dep == task.Name {
				successorNames = append(successorNames, t.Name)
				break
			}
		}
	}

	env := []corev1.EnvVar{
		{Name: "WL_TRANSPORT_PATTERN", Value: "file"},
		{Name: "WL_ODAG_NAME", Value: odagName},
		{Name: "WL_TASK_NAME", Value: task.Name},
		{Name: "WL_OUTPUT_DIR", Value: outputDir},
		{Name: "WL_DEPS", Value: strings.Join(task.Dependencies, ",")},
		{Name: "WL_SUCCESSORS", Value: strings.Join(successorNames, ",")},
		{Name: "PYTHONUNBUFFERED", Value: "1"},
		// Downward API: node name and host IP for state protocol + routing.
		{
			Name: "NODE_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"},
			},
		},
		{
			Name: "WL_NODE_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.hostIP"},
			},
		},
		// Spec hints: runtime (seconds) and output data size (bytes) for task awareness.
		{Name: "WL_RUNTIME", Value: fmt.Sprintf("%d", int(task.Runtime))},
		{Name: "WL_DATA_SIZE", Value: fmt.Sprintf("%d", parseDataSizeBytes(task.DataSize))},
	}

	// Per-dependency: node name (informational; recv() reads locally so host/path not needed).
	for _, dep := range task.Dependencies {
		ni := assignMap[dep]
		depKey := strings.ToUpper(strings.ReplaceAll(dep, "-", "_"))
		env = append(env,
			corev1.EnvVar{Name: fmt.Sprintf("WL_DEP_%s_NODE", depKey), Value: ni.name},
		)
	}

	// Per-successor: node name and host IP (needed for data-agent PUT in send()).
	for _, succ := range successorNames {
		ni := assignMap[succ]
		succKey := strings.ToUpper(strings.ReplaceAll(succ, "-", "_"))
		env = append(env,
			corev1.EnvVar{Name: fmt.Sprintf("WL_SUCC_%s_NODE", succKey), Value: ni.name},
			corev1.EnvVar{Name: fmt.Sprintf("WL_SUCC_%s_HOST", succKey), Value: ni.ip},
		)
	}

	env = append(env, task.UserEnv...)

	// Inject template/run metadata if this ODAG was created from a template.
	// These are set when buildEnvVars is called from processReadyTasks which
	// has access to the ODAG object via the odagObj parameter.
	// The actual injection happens in processReadyTasks after buildEnvVars returns.

	return env
}

// addTemplateEnvVars appends WL_TEMPLATE_NAME and WL_RUN_ID env vars if the
// ODAG was created from a template. Called after buildEnvVars.
func addTemplateEnvVars(env []corev1.EnvVar, odagLabels map[string]string) []corev1.EnvVar {
	if tpl := odagLabels["wl.io/template"]; tpl != "" {
		env = append(env, corev1.EnvVar{Name: "WL_TEMPLATE_NAME", Value: tpl})
	}
	if run := odagLabels["wl.io/run"]; run != "" {
		env = append(env, corev1.EnvVar{Name: "WL_RUN_ID", Value: run})
	}
	return env
}

// ensurePod creates a task pod with a hostPath volume for file-based data transfer.
func ensurePod(client *kubernetes.Clientset, namespace, odagName string, task taskSpec,
	nodeName string, envVars []corev1.EnvVar, ownerUID types.UID) error {

	podName := fmt.Sprintf("%s-%s", odagName, task.Name)
	_, err := client.CoreV1().Pods(namespace).Get(context.Background(), podName, metav1.GetOptions{})
	if err == nil {
		return nil // already exists
	}

	resources := parseResources(task.CPU, task.Memory)
	hostPathType := corev1.HostPathDirectoryOrCreate

	baseVolumes := []corev1.Volume{
		{
			Name: "wl-outputs",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: dataOutputPath,
					Type: &hostPathType,
				},
			},
		},
		{
			Name: "wl-shared",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/shared/wl-outputs",
					Type: &hostPathType,
				},
			},
		},
	}
	baseMounts := []corev1.VolumeMount{
		{Name: "wl-outputs", MountPath: dataOutputPath},
		{Name: "wl-shared", MountPath: "/shared/wl-outputs"},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels: map[string]string{
				labelODAGName: odagName,
				labelTaskName: task.Name,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "wl.io/v1",
				Kind:       "ODAG",
				Name:       odagName,
				UID:        ownerUID,
			}},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyNever,
			SecurityContext: task.SecurityContext,
			Volumes:         append(baseVolumes, task.Volumes...),
			Containers: []corev1.Container{{
				Name:            task.Name,
				Image:           task.Image,
				ImagePullPolicy: corev1.PullAlways,
				Command:         task.Command,
				Args:            task.Args,
				Env:             envVars,
				Resources:       resources,
				VolumeMounts:    append(baseMounts, task.VolumeMounts...),
			}},
		},
	}

	// Pin to assigned node via node affinity.
	if nodeName != "" {
		pod.Spec.Affinity = &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "kubernetes.io/hostname",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{nodeName},
						}},
					}},
				},
			},
		}
	}

	_, err = client.CoreV1().Pods(namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil && !isAlreadyExists(err) {
		return err
	}
	log.Printf("[odag-ctrl] created pod %s/%s (node: %s)", namespace, podName, nodeName)
	return nil
}

// parseDataSizeBytes converts a human-readable size string (e.g. "30MB", "1GiB")
// to bytes as an integer string for injection into WL_DATA_SIZE.
func parseDataSizeBytes(s string) int64 {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" || s == "0" {
		return 0
	}
	type entry struct {
		suffix string
		mult   int64
	}
	for _, e := range []entry{
		{"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10},
		{"GB", 1_000_000_000}, {"MB", 1_000_000}, {"KB", 1_000}, {"B", 1},
	} {
		if strings.HasSuffix(s, e.suffix) {
			numStr := strings.TrimSpace(s[:len(s)-len(e.suffix)])
			if v, err := strconv.ParseFloat(numStr, 64); err == nil {
				return int64(v * float64(e.mult))
			}
		}
	}
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return v
	}
	return 0
}

// --------------------------------------------------------------------------
// State protocol helpers
// --------------------------------------------------------------------------

// httpClient is reused across state queries to avoid creating a new connection
// for every call.
var httpClient = &http.Client{Timeout: 2 * time.Second}

// isDataReady asks the data-agent on nodeIP whether the producer task's
// output is locally installed on that node (i.e. .wl-ready exists). This is
// the only data-plane question the scheduler asks; it never reads task state
// to decide downstream scheduling. Returns true on body == "true".
func isDataReady(nodeIP, odagName, taskName string) bool {
	url := fmt.Sprintf("http://%s:%d/ready/%s/%s", nodeIP, dataAgentPort, odagName, taskName)
	resp, err := httpClient.Get(url)
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(body)) == "true"
}

// resetTaskState prepares a (node, task) for a fresh run: writes "Pending" to
// .wl-task-state and clears any stale .wl-ready marker left by a previous
// run of the same ODAG. The two endpoints are independent — task state and
// data availability are tracked separately.
func resetTaskState(nodeIP, odagName, taskName string) {
	// Task state -> Pending.
	url := fmt.Sprintf("http://%s:%d/state/%s/%s", nodeIP, dataAgentPort, odagName, taskName)
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader("Pending"))
	if err == nil {
		if resp, err := httpClient.Do(req); err != nil {
			log.Printf("[odag-ctrl] resetTaskState state %s/%s: %v", odagName, taskName, err)
		} else {
			resp.Body.Close()
		}
	}
	// Clear stale data-ready marker.
	readyURL := fmt.Sprintf("http://%s:%d/ready/%s/%s", nodeIP, dataAgentPort, odagName, taskName)
	delReq, err := http.NewRequest(http.MethodDelete, readyURL, nil)
	if err == nil {
		if resp, err := httpClient.Do(delReq); err != nil {
			log.Printf("[odag-ctrl] resetTaskState ready %s/%s: %v", odagName, taskName, err)
		} else {
			resp.Body.Close()
		}
	}
}

// querySending returns true if the data-agent reports sending=true for the task.
func querySending(nodeIP, odagName, taskName string) bool {
	url := fmt.Sprintf("http://%s:%d/sending/%s/%s", nodeIP, dataAgentPort, odagName, taskName)
	resp, err := httpClient.Get(url)
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(body)) == "true"
}

// queryTaskBytes returns the actual output bytes recorded by the data-agent.
func queryTaskBytes(nodeIP, odagName, taskName string) int64 {
	url := fmt.Sprintf("http://%s:%d/bytes/%s/%s", nodeIP, dataAgentPort, odagName, taskName)
	resp, err := httpClient.Get(url)
	if err != nil || resp.StatusCode != http.StatusOK {
		return 0
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
	return n
}

// dataAgentFlow is one per-push flow record returned by data-agent /flows/<odag>.
type dataAgentFlow struct {
	FromTask  string  `json:"fromTask"`
	ToTask    string  `json:"toTask"`
	SrcNode   string  `json:"srcNode"`
	DstNode   string  `json:"dstNode"`
	DataSize  int64   `json:"dataSize"`
	StartUnix float64 `json:"startUnix"`
	EndUnix   float64 `json:"endUnix"`
	Ok        bool    `json:"ok"`
}

// deleteFlows truncates the flow log for an ODAG on one node's data-agent.
func deleteFlows(nodeIP, odagName string) {
	url := fmt.Sprintf("http://%s:%d/flows/%s", nodeIP, dataAgentPort, odagName)
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("[odag-ctrl] deleteFlows %s on %s: %v", odagName, nodeIP, err)
		return
	}
	resp.Body.Close()
}

// queryFlows pulls the flow log for an ODAG from one node's data-agent.
func queryFlows(nodeIP, odagName string) []dataAgentFlow {
	url := fmt.Sprintf("http://%s:%d/flows/%s", nodeIP, dataAgentPort, odagName)
	resp, err := httpClient.Get(url)
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil
	}
	defer resp.Body.Close()
	var out []dataAgentFlow
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil
	}
	return out
}

// queryTaskState returns the raw state string from the data-agent, or "" on error.
func queryTaskState(nodeIP, odagName, taskName string) string {
	url := fmt.Sprintf("http://%s:%d/state/%s/%s", nodeIP, dataAgentPort, odagName, taskName)
	resp, err := httpClient.Get(url)
	if err != nil || resp.StatusCode != http.StatusOK {
		return ""
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(body))
}

// --------------------------------------------------------------------------
// Status updates
// --------------------------------------------------------------------------

func updateTaskStatuses(dynClient dynamic.Interface,
	namespace, odagName string, pods []corev1.Pod, assignMap map[string]nodeInfo, tasks []taskSpec) {

	dataSizeMap := make(map[string]string, len(tasks))
	for _, t := range tasks {
		if n := parseDataSizeBytes(t.DataSize); n > 0 {
			dataSizeMap[t.Name] = strconv.FormatInt(n, 10)
		}
	}

	taskStatuses := make([]map[string]interface{}, 0, len(pods))
	for _, pod := range pods {
		taskName := pod.Labels[labelTaskName]
		ts := map[string]interface{}{
			"name":    taskName,
			"podName": pod.Name,
			"node":    pod.Spec.NodeName,
		}
		if pod.Status.StartTime != nil {
			ts["startTime"] = pod.Status.StartTime.UTC().Format(time.RFC3339Nano)
		}
		podPhase := "Pending"
		taskState := "Pending" // default before the SDK has had a chance to mark Running
		switch pod.Status.Phase {
		case corev1.PodRunning:
			podPhase = "Running"
			ni := assignMap[taskName]
			if ni.ip != "" {
				if s := queryTaskState(ni.ip, odagName, taskName); s != "" {
					taskState = s
				} else {
					taskState = "Running"
				}
				if querySending(ni.ip, odagName, taskName) {
					ts["sending"] = true
				}
			} else {
				taskState = "Running"
			}
		case corev1.PodSucceeded:
			podPhase = "Succeeded"
			taskState = "ComputeDone" // pod exited cleanly
			for _, cs := range pod.Status.ContainerStatuses {
				if cs.State.Terminated != nil {
					ts["completionTime"] = cs.State.Terminated.FinishedAt.UTC().Format(time.RFC3339Nano)
				}
			}
		case corev1.PodFailed:
			podPhase = "Failed"
			taskState = "Failed"
			for _, cs := range pod.Status.ContainerStatuses {
				if cs.State.Terminated != nil {
					ts["completionTime"] = cs.State.Terminated.FinishedAt.UTC().Format(time.RFC3339Nano)
				}
			}
		}
		ts["phase"] = podPhase
		ts["state"] = taskState
		if ds := dataSizeMap[taskName]; ds != "" {
			ts["dataSize"] = ds
		}
		taskStatuses = append(taskStatuses, ts)
	}

	patch := map[string]interface{}{"status": map[string]interface{}{"tasks": taskStatuses}}
	data, _ := json.Marshal(patch)
	_, _ = dynClient.Resource(odagGVR).Namespace(namespace).Patch(
		context.Background(), odagName, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	)
}

// updateActualFlows polls each node's data-agent /flows/<odag> endpoint, merges
// the per-push records, converts absolute timestamps to seconds-from-t0 where
// t0 is the earliest pod startTime (same origin the execution Gantt uses), and
// patches status.actualNetworkFlows. Safe to call every reconcile — the full
// list is written each time (flows on disk are authoritative and monotonic).
func updateActualFlows(dynClient dynamic.Interface, namespace, odagName string, assignMap map[string]nodeInfo, pods []corev1.Pod) {
	// Collect unique src nodes — only senders record flows.
	seen := make(map[string]string) // nodeName -> nodeIP
	for _, ni := range assignMap {
		if ni.ip == "" {
			continue
		}
		seen[ni.name] = ni.ip
	}
	if len(seen) == 0 {
		return
	}

	var all []dataAgentFlow
	for _, ip := range seen {
		all = append(all, queryFlows(ip, odagName)...)
	}
	if len(all) == 0 {
		return
	}

	// t0 = earliest pod startTime. Same origin as the execution Gantt
	// (GanttChart.tsx also uses min(task.startTime)). Fall back to the
	// earliest flow start if no pod has a startTime yet.
	var t0 float64
	haveT0 := false
	for _, pod := range pods {
		if pod.Status.StartTime == nil {
			continue
		}
		s := float64(pod.Status.StartTime.UnixNano()) / 1e9
		if !haveT0 || s < t0 {
			t0 = s
			haveT0 = true
		}
	}
	if !haveT0 {
		t0 = all[0].StartUnix
		for _, f := range all {
			if f.StartUnix < t0 {
				t0 = f.StartUnix
			}
		}
	}

	entries := make([]map[string]any, 0, len(all))
	for _, f := range all {
		entries = append(entries, map[string]any{
			"fromTask": f.FromTask,
			"toTask":   f.ToTask,
			"srcNode":  f.SrcNode,
			"dstNode":  f.DstNode,
			"dataSize": f.DataSize,
			"start":    f.StartUnix - t0,
			"end":      f.EndUnix - t0,
			"ok":       f.Ok,
		})
	}

	patch := map[string]any{
		"status": map[string]any{
			"actualNetworkFlows": entries,
		},
	}
	data, _ := json.Marshal(patch)
	_, _ = dynClient.Resource(odagGVR).Namespace(namespace).Patch(
		context.Background(), odagName, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	)
}

func checkODAGCompletion(dynClient dynamic.Interface, client *kubernetes.Clientset, pods []corev1.Pod, namespace, odagName string, totalTasks int) {
	// First pass: any Failed pod is terminal regardless of how many other
	// pods exist. The previous version returned early when len(pods) <
	// totalTasks, so a task that failed BEFORE its downstream pods were
	// created would never aggregate into ODAG.status.phase — leaving the
	// ODAG stuck Running forever (post-experiment-todos #5b, observed on
	// wpf-heft-run-019).
	for _, pod := range pods {
		if pod.Status.Phase != corev1.PodFailed {
			continue
		}
		taskName := pod.Labels[labelTaskName]
		reason := "pod failed"
		if len(pod.Status.ContainerStatuses) > 0 {
			for _, cs := range pod.Status.ContainerStatuses {
				if cs.State.Terminated != nil && cs.State.Terminated.Reason != "" {
					reason = cs.State.Terminated.Reason
					break
				}
			}
		}
		msg := fmt.Sprintf("task %q failed (%s)", taskName, reason)
		updateODAGPhase(dynClient, namespace, odagName, "Failed", msg)
		log.Printf("[odag-ctrl] ODAG %s/%s Failed: %s", namespace, odagName, msg)
		return
	}

	// Second pass: success only if every task has a Succeeded pod.
	if len(pods) < totalTasks {
		return // not all layers launched yet; no decision possible
	}
	for _, pod := range pods {
		if pod.Status.Phase != corev1.PodSucceeded {
			return // still progressing
		}
	}
	makespan := computeMakespan(pods)
	updateODAGCompletion(dynClient, namespace, odagName, makespan)
	log.Printf("[odag-ctrl] ODAG %s/%s Succeeded (makespan: %.2fs)", namespace, odagName, makespan)

	// Trigger profiling and data cleanup if this ODAG was created from a template.
	go profileODAGIfTemplated(dynClient, client, namespace, odagName, pods, makespan)
}

// profileODAGIfTemplated checks if a completed ODAG was created from a template
// and records profiling data if so.
func profileODAGIfTemplated(dynClient dynamic.Interface, client *kubernetes.Clientset, namespace, odagName string, pods []corev1.Pod, makespan float64) {
	if profilerDB == nil {
		return
	}

	// Fetch the ODAG to check labels.
	obj, err := dynClient.Resource(odagGVR).Namespace(namespace).Get(
		context.Background(), odagName, metav1.GetOptions{},
	)
	if err != nil {
		return
	}

	labels := obj.GetLabels()
	templateName := labels["wl.io/template"]
	if templateName == "" {
		return
	}

	runNum := getRunNumber(obj)
	tasks := extractTasks(obj)

	// Retrieve assignment map.
	key := namespace + "/" + odagName
	raw, ok := assignmentCache.Load(key)
	if !ok {
		return
	}
	assignMap := raw.(map[string]nodeInfo)

	// Extract actual start/completion times from pods.
	taskStartTimes := make(map[string]time.Time)
	taskCompletionTimes := make(map[string]time.Time)
	for _, pod := range pods {
		taskName := pod.Labels[labelTaskName]
		if pod.Status.StartTime != nil {
			taskStartTimes[taskName] = pod.Status.StartTime.Time
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Terminated != nil {
				taskCompletionTimes[taskName] = cs.State.Terminated.FinishedAt.Time
			}
		}
	}

	profileCompletedRun(dynClient, client, profilerDB, namespace, odagName, templateName, runNum,
		tasks, assignMap, taskStartTimes, taskCompletionTimes, makespan)
}

// ensureRunLabel stamps wl.io/run on a template-derived ODAG when missing.
// The number comes from the SQL run_counter so it survives ODAG deletes —
// unlike the live-resource-list approach previously used by the CLI and
// ui-server, which gave every fresh run the same number after the prior
// run was cleaned up. Idempotent: a no-op when the label is already set
// or when the ODAG isn't template-derived. Mutates the in-memory obj so
// the rest of deployODAG sees the updated label.
func ensureRunLabel(dynClient dynamic.Interface, obj *unstructured.Unstructured) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	if labels["wl.io/run"] != "" {
		return
	}
	tpl := labels["wl.io/template"]
	if tpl == "" {
		return
	}
	if profilerDB == nil {
		log.Printf("[odag-ctrl] cannot stamp run label for %s/%s: profilerDB unavailable",
			obj.GetNamespace(), obj.GetName())
		return
	}
	runNum, err := nextRunID(profilerDB, tpl)
	if err != nil {
		log.Printf("[odag-ctrl] nextRunID(%s) failed: %v", tpl, err)
		return
	}
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": map[string]interface{}{
				"wl.io/run": fmt.Sprintf("%d", runNum),
			},
		},
	}
	data, _ := json.Marshal(patch)
	if _, err := dynClient.Resource(odagGVR).Namespace(obj.GetNamespace()).Patch(
		context.Background(), obj.GetName(), types.MergePatchType, data,
		metav1.PatchOptions{},
	); err != nil {
		log.Printf("[odag-ctrl] patch run label on %s/%s failed: %v",
			obj.GetNamespace(), obj.GetName(), err)
		return
	}
	labels["wl.io/run"] = fmt.Sprintf("%d", runNum)
	obj.SetLabels(labels)
	log.Printf("[odag-ctrl] stamped %s/%s with wl.io/run=%d (template %s)",
		obj.GetNamespace(), obj.GetName(), runNum, tpl)
}

func updateODAGPhase(dynClient dynamic.Interface, namespace, name, phase, message string) {
	key := namespace + "/" + name
	if phase == "Running" || phase == "Scheduling" || phase == "Pending" {
		runningODAGs.Store(key, true)
	} else {
		runningODAGs.Delete(key)
	}
	status := map[string]interface{}{
		"phase":   phase,
		"message": message,
	}
	// Stamp startTime on first transition to Running.
	if phase == "Running" {
		status["startTime"] = time.Now().UTC().Format(time.RFC3339Nano)
	}
	patch := map[string]interface{}{
		"status": status,
	}
	data, _ := json.Marshal(patch)
	_, _ = dynClient.Resource(odagGVR).Namespace(namespace).Patch(
		context.Background(), name, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	)
}

func updateODAGCompletion(dynClient dynamic.Interface, namespace, name string, makespan float64) {
	runningODAGs.Delete(namespace + "/" + name)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"phase":          "Succeeded",
			"completionTime": now,
			"makespan":       makespan,
			"message":        "",
		},
	}
	data, _ := json.Marshal(patch)
	_, _ = dynClient.Resource(odagGVR).Namespace(namespace).Patch(
		context.Background(), name, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	)
}

// --------------------------------------------------------------------------
// Utilities
// --------------------------------------------------------------------------

func parseResources(cpu, memory string) corev1.ResourceRequirements {
	r := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{},
	}
	if cpu != "" {
		q := resource.MustParse(cpu)
		r.Requests[corev1.ResourceCPU] = q
		r.Limits[corev1.ResourceCPU] = q
	}
	if memory != "" {
		q := resource.MustParse(memory)
		r.Requests[corev1.ResourceMemory] = q
		r.Limits[corev1.ResourceMemory] = q
	}
	return r
}

// parseResourceQuantity parses a k8s resource quantity string like "500m" or "256Mi".
func parseResourceQuantity(s string) (resource.Quantity, error) {
	return resource.ParseQuantity(s)
}

func computeMakespan(pods []corev1.Pod) float64 {
	var earliest, latest time.Time
	for _, pod := range pods {
		if pod.Status.StartTime != nil {
			st := pod.Status.StartTime.Time
			if earliest.IsZero() || st.Before(earliest) {
				earliest = st
			}
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Terminated != nil {
				ft := cs.State.Terminated.FinishedAt.Time
				if latest.IsZero() || ft.After(latest) {
					latest = ft
				}
			}
		}
	}
	if earliest.IsZero() || latest.IsZero() {
		return 0
	}
	return latest.Sub(earliest).Seconds()
}

func isAlreadyExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already exists")
}
