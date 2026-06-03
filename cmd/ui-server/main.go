package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	_ "modernc.org/sqlite"
)

// --------------------------------------------------------------------------
// GVRs
// --------------------------------------------------------------------------

var (
	odagGVR         = schema.GroupVersionResource{Group: "wl.io", Version: "v1", Resource: "odags"}
	odagTemplateGVR = schema.GroupVersionResource{Group: "wl.io", Version: "v1", Resource: "odagtemplates"}
)

// --------------------------------------------------------------------------
// In-memory cache (updated by the K8s watch loop)
// --------------------------------------------------------------------------

type Server struct {
	dynClient  dynamic.Interface
	kubeClient kubernetes.Interface
	restConfig *rest.Config
	db         *sql.DB
	mu         sync.RWMutex
	odags     map[string]*unstructured.Unstructured // "ns/name" -> obj
	templates map[string]*unstructured.Unstructured

	// SSE clients: each client gets a channel of JSON event bytes.
	sseMu      sync.Mutex
	sseClients map[chan []byte]struct{}

	// Last-known per-node filesystem stats, retained across cluster-node
	// refreshes so a transient kubelet stats/summary timeout (common once slow
	// edge nodes join) doesn't blank the disk column.
	diskMu    sync.Mutex
	diskCache map[string]nodeDiskStat
}

// nodeDiskStat is a node's root-filesystem usage as reported by the kubelet
// stats/summary endpoint.
type nodeDiskStat struct{ capacity, used, available int64 }

func newServer(dynClient dynamic.Interface, kubeClient kubernetes.Interface, cfg *rest.Config, db *sql.DB) *Server {
	return &Server{
		dynClient:  dynClient,
		kubeClient: kubeClient,
		restConfig: cfg,
		db:         db,
		odags:      make(map[string]*unstructured.Unstructured),
		templates:  make(map[string]*unstructured.Unstructured),
		sseClients: make(map[chan []byte]struct{}),
		diskCache:  make(map[string]nodeDiskStat),
	}
}

// --------------------------------------------------------------------------
// Main
// --------------------------------------------------------------------------

func main() {
	var kubeconfig, addr, dbPath string
	flag.StringVar(&kubeconfig, "kubeconfig", "", "path to kubeconfig (empty = in-cluster)")
	flag.StringVar(&addr, "addr", envOrDefault("WL_LISTEN_ADDR", ":8080"), "listen address")
	flag.StringVar(&dbPath, "db", envOrDefault("WL_DB_PATH", "/data/wl-history.db"), "SQLite database path")
	flag.Parse()

	cfg, err := buildConfig(kubeconfig)
	if err != nil {
		log.Fatalf("[ui-server] failed to build config: %v", err)
	}
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("[ui-server] failed to create dynamic client: %v", err)
	}
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("[ui-server] failed to create kubernetes client: %v", err)
	}

	db, err := openDB(dbPath)
	if err != nil {
		log.Fatalf("[ui-server] failed to open database: %v", err)
	}
	defer db.Close()

	srv := newServer(dynClient, kubeClient, cfg, db)

	// Start K8s watch loops.
	go srv.watchResources(odagGVR, &srv.odags)
	go srv.watchResources(odagTemplateGVR, &srv.templates)

	// HTTP routes.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/odags", srv.handleListODAGs)
	mux.HandleFunc("GET /api/odags/{namespace}/{name}", srv.handleGetODAG)
	mux.HandleFunc("GET /api/odags/{namespace}/{name}/history", srv.handleGetODAGHistory)
	mux.HandleFunc("POST /api/odags/{namespace}/{name}/retry", srv.handleRetryODAG)
	mux.HandleFunc("POST /api/batch", srv.handleBatchSubmit)
	mux.HandleFunc("GET /api/templates", srv.handleListTemplates)
	mux.HandleFunc("GET /api/templates/{namespace}/{name}", srv.handleGetTemplate)
	mux.HandleFunc("GET /api/templates/{namespace}/{name}/runs", srv.handleGetTemplateRuns)
	mux.HandleFunc("GET /api/templates/{namespace}/{name}/history", srv.handleGetTemplateHistory)
	mux.HandleFunc("POST /api/templates/{namespace}/{name}/run", srv.handleRunTemplate)
	mux.HandleFunc("DELETE /api/templates/{namespace}/{name}", srv.handleDeleteTemplate)
	mux.HandleFunc("GET /api/events", srv.handleSSE)
	mux.HandleFunc("GET /api/cluster/nodes", srv.handleClusterNodes)

	// Serve compiled React frontend from ui/dist (embedded at build time).
	// During development, the Vite dev server proxies /api to this server.
	// SPA fallback: serve index.html for any path not matched by a file, so
	// that React Router handles client-side routes on hard refresh.
	uiDir := envOrDefault("WL_UI_DIR", "./ui/dist")
	if _, err := os.Stat(uiDir); err == nil {
		fs := http.FileServer(http.Dir(uiDir))
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			// Never serve the SPA for /api/ paths — return 404 JSON so the
			// browser gets a clear error rather than HTML it can't parse.
			if strings.HasPrefix(r.URL.Path, "/api/") {
				w.Header().Set("Content-Type", "application/json")
				http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
				return
			}
			// Serve real static assets (JS, CSS, fonts) directly.
			// no-cache ensures the browser always revalidates so it picks
			// up new content-hashed bundles without a hard refresh.
			if r.URL.Path != "/" {
				if _, err := os.Stat(uiDir + r.URL.Path); err == nil {
					w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
					fs.ServeHTTP(w, r)
					return
				}
			}
			// SPA fallback: all other paths get index.html so React Router
			// can handle them. No-cache so the browser always picks up the
			// latest content-hashed bundle filename after deploys.
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			http.ServeFile(w, r, uiDir+"/index.html")
		})
		log.Printf("[ui-server] serving frontend from %s", uiDir)
	} else {
		log.Printf("[ui-server] no frontend found at %s; serving API only", uiDir)
	}

	log.Printf("[ui-server] listening on %s", addr)
	if err := http.ListenAndServe(addr, cors(mux)); err != nil {
		log.Fatalf("[ui-server] %v", err)
	}
}

// --------------------------------------------------------------------------
// K8s watch loop
// --------------------------------------------------------------------------

func (s *Server) watchResources(gvr schema.GroupVersionResource, cache *map[string]*unstructured.Unstructured) {
	for {
		watcher, err := s.dynClient.Resource(gvr).Namespace("").Watch(
			context.Background(), metav1.ListOptions{},
		)
		if err != nil {
			log.Printf("[ui-server] error watching %s: %v; retrying in 5s", gvr.Resource, err)
			time.Sleep(5 * time.Second)
			continue
		}
		for event := range watcher.ResultChan() {
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			key := obj.GetNamespace() + "/" + obj.GetName()

			s.mu.Lock()
			switch string(event.Type) {
			case "ADDED", "MODIFIED":
				(*cache)[key] = obj
				// Record completion in history when an ODAG reaches a terminal phase.
				if gvr == odagGVR {
					phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
					if phase == "Succeeded" || phase == "Failed" {
						go s.recordHistory(obj)
					}
				}
			case "DELETED":
				delete(*cache, key)
			}
			s.mu.Unlock()

			// Notify SSE clients.
			s.broadcast(gvr.Resource, string(event.Type), obj.GetName(), obj.GetNamespace())
		}
		time.Sleep(2 * time.Second)
	}
}

// --------------------------------------------------------------------------
// REST handlers
// --------------------------------------------------------------------------

func (s *Server) handleListODAGs(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]map[string]interface{}, 0, len(s.odags))
	for _, obj := range s.odags {
		result = append(result, odagSummary(obj))
	}
	sort.Slice(result, func(i, j int) bool {
		return fmt.Sprint(result[i]["name"]) < fmt.Sprint(result[j]["name"])
	})
	writeJSON(w, result)
}

func (s *Server) handleGetODAG(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	s.mu.RLock()
	obj, ok := s.odags[ns+"/"+name]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, odagDetail(obj))
}

func (s *Server) handleGetODAGHistory(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	history, err := s.queryHistory(ns, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, history)
}

// --------------------------------------------------------------------------
// Cluster node health dashboard
// --------------------------------------------------------------------------

type nodeInfoResp struct {
	Name               string  `json:"name"`
	Ready              bool    `json:"ready"`
	Schedulable        bool    `json:"schedulable"`
	Roles              string  `json:"roles"`
	InternalIP         string  `json:"internalIP"`
	KubeletVersion     string  `json:"kubeletVersion"`
	AllocCPUMillis     int64   `json:"allocCPUMillis"`
	AllocMemBytes      int64   `json:"allocMemBytes"`
	UsedCPUMillis      int64   `json:"usedCPUMillis"`
	UsedMemBytes       int64   `json:"usedMemBytes"`
	CPUPct             float64 `json:"cpuPct"`
	MemPct             float64 `json:"memPct"`
	DiskCapacityBytes  int64   `json:"diskCapacityBytes"`
	DiskUsedBytes      int64   `json:"diskUsedBytes"`
	DiskAvailableBytes int64   `json:"diskAvailableBytes"`
	DiskPct            float64 `json:"diskPct"`
	DiskPressure       bool    `json:"diskPressure"`
	TotalPods          int     `json:"totalPods"`
	ODAGTasks          int     `json:"odagTasks"`
	RunningODAGTasks   int     `json:"runningOdagTasks"`
}

func (s *Server) handleClusterNodes(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	nodes, err := s.kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		http.Error(w, "list nodes: "+err.Error(), http.StatusInternalServerError)
		return
	}
	pods, err := s.kubeClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		http.Error(w, "list pods: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Metrics via raw REST (avoids adding k8s.io/metrics dep).
	type metricsItem struct {
		Metadata struct{ Name string } `json:"metadata"`
		Usage    struct {
			CPU    string `json:"cpu"`
			Memory string `json:"memory"`
		} `json:"usage"`
	}
	type metricsList struct {
		Items []metricsItem `json:"items"`
	}
	usage := map[string]struct{ cpu, mem int64 }{}
	if raw, err := s.kubeClient.Discovery().RESTClient().
		Get().AbsPath("/apis/metrics.k8s.io/v1beta1/nodes").DoRaw(ctx); err == nil {
		var ml metricsList
		if json.Unmarshal(raw, &ml) == nil {
			for _, it := range ml.Items {
				cpu := parseCPUQuantity(it.Usage.CPU)
				mem := parseMemoryQuantity(it.Usage.Memory)
				usage[it.Metadata.Name] = struct{ cpu, mem int64 }{cpu, mem}
			}
		}
	}

	// Per-node filesystem usage via kubelet stats/summary proxy. Successful
	// reads update s.diskCache; a node that times out keeps its last-known
	// value, so the disk column stays stable instead of flickering when slow
	// nodes intermittently miss the deadline.
	type statsSummary struct {
		Node struct {
			Fs struct {
				CapacityBytes  int64 `json:"capacityBytes"`
				UsedBytes      int64 `json:"usedBytes"`
				AvailableBytes int64 `json:"availableBytes"`
			} `json:"fs"`
		} `json:"node"`
	}
	var wg sync.WaitGroup
	for _, n := range nodes.Items {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			path := fmt.Sprintf("/api/v1/nodes/%s/proxy/stats/summary", name)
			cctx, cancel := context.WithTimeout(ctx, 8*time.Second)
			defer cancel()
			raw, err := s.kubeClient.CoreV1().RESTClient().Get().AbsPath(path).DoRaw(cctx)
			if err != nil {
				return // keep last-known value from diskCache
			}
			var ss statsSummary
			if json.Unmarshal(raw, &ss) != nil {
				return
			}
			s.diskMu.Lock()
			s.diskCache[name] = nodeDiskStat{
				capacity:  ss.Node.Fs.CapacityBytes,
				used:      ss.Node.Fs.UsedBytes,
				available: ss.Node.Fs.AvailableBytes,
			}
			s.diskMu.Unlock()
		}(n.Name)
	}
	wg.Wait()

	// Stable snapshot of cached disk stats (retained across per-node timeouts).
	s.diskMu.Lock()
	disk := make(map[string]nodeDiskStat, len(s.diskCache))
	for k, v := range s.diskCache {
		disk[k] = v
	}
	s.diskMu.Unlock()

	// Tally pods per node.
	type podTally struct {
		total, odag, odagRunning int
	}
	tally := map[string]*podTally{}
	for _, p := range pods.Items {
		n := p.Spec.NodeName
		if n == "" {
			continue
		}
		t := tally[n]
		if t == nil {
			t = &podTally{}
			tally[n] = t
		}
		t.total++
		switch {
		case p.Labels["wl-odag"] != "":
			t.odag++
			if p.Status.Phase == corev1.PodRunning {
				t.odagRunning++
			}
		}
	}

	result := make([]nodeInfoResp, 0, len(nodes.Items))
	for _, n := range nodes.Items {
		ready := false
		for _, c := range n.Status.Conditions {
			if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
				ready = true
			}
		}
		schedulable := !n.Spec.Unschedulable
		roles := []string{}
		for k := range n.Labels {
			if strings.HasPrefix(k, "node-role.kubernetes.io/") {
				r := strings.TrimPrefix(k, "node-role.kubernetes.io/")
				if r != "" {
					roles = append(roles, r)
				}
			}
		}
		if len(roles) == 0 {
			roles = []string{"worker"}
		}
		internalIP := ""
		for _, a := range n.Status.Addresses {
			if a.Type == corev1.NodeInternalIP {
				internalIP = a.Address
				break
			}
		}
		allocCPU := n.Status.Allocatable.Cpu().MilliValue()
		allocMem, _ := n.Status.Allocatable.Memory().AsInt64()
		u := usage[n.Name]
		cpuPct := 0.0
		memPct := 0.0
		if allocCPU > 0 {
			cpuPct = float64(u.cpu) / float64(allocCPU) * 100
		}
		if allocMem > 0 {
			memPct = float64(u.mem) / float64(allocMem) * 100
		}
		d := disk[n.Name]
		diskPct := 0.0
		if d.capacity > 0 {
			diskPct = float64(d.used) / float64(d.capacity) * 100
		}
		diskPressure := false
		for _, c := range n.Status.Conditions {
			if c.Type == corev1.NodeDiskPressure && c.Status == corev1.ConditionTrue {
				diskPressure = true
			}
		}
		t := tally[n.Name]
		if t == nil {
			t = &podTally{}
		}
		result = append(result, nodeInfoResp{
			Name:               n.Name,
			Ready:              ready,
			Schedulable:        schedulable,
			Roles:              strings.Join(roles, ","),
			InternalIP:         internalIP,
			KubeletVersion:     n.Status.NodeInfo.KubeletVersion,
			AllocCPUMillis:     allocCPU,
			AllocMemBytes:      allocMem,
			UsedCPUMillis:      u.cpu,
			UsedMemBytes:       u.mem,
			CPUPct:             cpuPct,
			MemPct:             memPct,
			DiskCapacityBytes:  d.capacity,
			DiskUsedBytes:      d.used,
			DiskAvailableBytes: d.available,
			DiskPct:            diskPct,
			DiskPressure:       diskPressure,
			TotalPods:          t.total,
			ODAGTasks:          t.odag,
			RunningODAGTasks:   t.odagRunning,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	writeJSON(w, result)
}

// parseCPUQuantity parses k8s CPU strings: "100m", "2", "1500000n" → millicores.
func parseCPUQuantity(s string) int64 {
	if s == "" {
		return 0
	}
	if strings.HasSuffix(s, "n") {
		v, _ := strconv.ParseInt(strings.TrimSuffix(s, "n"), 10, 64)
		return v / 1_000_000
	}
	if strings.HasSuffix(s, "u") {
		v, _ := strconv.ParseInt(strings.TrimSuffix(s, "u"), 10, 64)
		return v / 1_000
	}
	if strings.HasSuffix(s, "m") {
		v, _ := strconv.ParseInt(strings.TrimSuffix(s, "m"), 10, 64)
		return v
	}
	f, _ := strconv.ParseFloat(s, 64)
	return int64(f * 1000)
}

// parseMemoryQuantity parses "1024Ki", "2Mi", "3Gi", "500M" → bytes.
func parseMemoryQuantity(s string) int64 {
	if s == "" {
		return 0
	}
	mult := int64(1)
	for _, suf := range []struct {
		sfx string
		m   int64
	}{
		{"Ki", 1024}, {"Mi", 1024 * 1024}, {"Gi", 1024 * 1024 * 1024}, {"Ti", 1024 * 1024 * 1024 * 1024},
		{"K", 1000}, {"M", 1000_000}, {"G", 1000_000_000}, {"T", 1000_000_000_000},
	} {
		if strings.HasSuffix(s, suf.sfx) {
			mult = suf.m
			s = strings.TrimSuffix(s, suf.sfx)
			break
		}
	}
	v, _ := strconv.ParseInt(s, 10, 64)
	return v * mult
}

func (s *Server) handleGetTemplateHistory(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	history, err := s.queryTemplateHistory(ns, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, history)
}

func (s *Server) handleRetryODAG(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	ctx := r.Context()

	// Fetch current object to extract spec.
	existing, err := s.dynClient.Resource(odagGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	spec, _, _ := unstructured.NestedMap(existing.Object, "spec")

	// Delete the existing ODAG.
	if err := s.dynClient.Resource(odagGVR).Namespace(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Respond immediately — recreation happens in background so the browser
	// returns quickly and the SSE stream drives the live graph updates.
	writeJSON(w, map[string]string{"status": "ok"})

	go func() {
		bgCtx := context.Background()
		fresh := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "wl.io/v1",
				"kind":       "ODAG",
				"metadata": map[string]interface{}{
					"name":      name,
					"namespace": ns,
				},
				"spec": spec,
			},
		}
		// Wait up to 10s for the object to be gone, then recreate.
		for i := 0; i < 20; i++ {
			time.Sleep(500 * time.Millisecond)
			_, err := s.dynClient.Resource(odagGVR).Namespace(ns).Get(bgCtx, name, metav1.GetOptions{})
			if err != nil {
				break
			}
		}
		if _, err := s.dynClient.Resource(odagGVR).Namespace(ns).Create(bgCtx, fresh, metav1.CreateOptions{}); err != nil {
			log.Printf("[ui-server] retry create failed for %s/%s: %v", ns, name, err)
		}
	}()
}

// --------------------------------------------------------------------------
// Template handlers
// --------------------------------------------------------------------------

func (s *Server) handleListTemplates(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]map[string]interface{}, 0, len(s.templates))
	for _, obj := range s.templates {
		result = append(result, s.templateSummary(obj))
	}
	sort.Slice(result, func(i, j int) bool {
		return fmt.Sprint(result[i]["name"]) < fmt.Sprint(result[j]["name"])
	})
	writeJSON(w, result)
}

func (s *Server) handleGetTemplate(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	s.mu.RLock()
	obj, ok := s.templates[ns+"/"+name]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, s.templateDetail(obj))
}

func (s *Server) handleGetTemplateRuns(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")

	s.mu.RLock()
	var runs []map[string]interface{}
	for _, obj := range s.odags {
		labels := obj.GetLabels()
		if labels["wl.io/template"] == name && obj.GetNamespace() == ns {
			phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
			makespan := nestedFloat(obj.Object, "status", "makespan")
			startTime, _, _ := unstructured.NestedString(obj.Object, "status", "startTime")
			completionTime, _, _ := unstructured.NestedString(obj.Object, "status", "completionTime")
			runs = append(runs, map[string]interface{}{
				"name":           obj.GetName(),
				"namespace":      ns,
				"run":            labels["wl.io/run"],
				"phase":          defaultStr(phase, "Pending"),
				"makespan":       makespan,
				"startTime":      startTime,
				"completionTime": completionTime,
				"createdAt":      obj.GetCreationTimestamp().UTC().Format(time.RFC3339),
			})
		}
	}
	s.mu.RUnlock()

	sort.Slice(runs, func(i, j int) bool {
		return fmt.Sprint(runs[i]["createdAt"]) < fmt.Sprint(runs[j]["createdAt"])
	})
	if runs == nil {
		runs = []map[string]interface{}{}
	}
	writeJSON(w, runs)
}

func (s *Server) handleRunTemplate(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")

	s.mu.RLock()
	tmplObj, ok := s.templates[ns+"/"+name]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "template not found", http.StatusNotFound)
		return
	}

	// Extract spec from template, stripping template-only fields.
	spec, _, _ := unstructured.NestedMap(tmplObj.Object, "spec")
	delete(spec, "profiling")
	delete(spec, "defaults")
	delete(spec, "retention")
	delete(spec, "description")

	// Use generateName so K8s assigns a unique suffix. wl.io/run is stamped
	// by the controller from its SQL counter on first reconcile. Computing
	// from a live-resource list is racy — any prior run that's been deleted
	// resets the count and produces duplicate names that collide in
	// wl-history.db.
	odag := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "wl.io/v1",
			"kind":       "ODAG",
			"metadata": map[string]interface{}{
				"generateName": fmt.Sprintf("%s-run-", name),
				"namespace":    ns,
				"labels": map[string]interface{}{
					"wl.io/template": name,
				},
			},
			"spec": spec,
		},
	}

	created, err := s.dynClient.Resource(odagGVR).Namespace(ns).Create(
		context.Background(), odag, metav1.CreateOptions{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]interface{}{
		"name":    created.GetName(),
		"message": fmt.Sprintf("Created run %s from template %s", created.GetName(), name),
	})
}

func (s *Server) handleDeleteTemplate(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	if err := s.dynClient.Resource(odagTemplateGVR).Namespace(ns).Delete(
		context.Background(), name, metav1.DeleteOptions{}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"status": "deleted", "name": name})
}

// --------------------------------------------------------------------------
// Template response builders
// --------------------------------------------------------------------------

// templateSummary builds the summary for an ODAGTemplate. It counts runs by
// scanning the odags cache (more reliable than status.runCount alone).
func (s *Server) templateSummary(obj *unstructured.Unstructured) map[string]interface{} {
	tasks, _, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	sched, _, _ := unstructured.NestedString(obj.Object, "spec", "scheduler")
	desc, _, _ := unstructured.NestedString(obj.Object, "spec", "description")
	lastMakespan := nestedFloat(obj.Object, "status", "lastRunMakespan")

	name := obj.GetName()
	ns := obj.GetNamespace()

	// Count runs and find the latest one from the cache.
	runCount := 0
	var lastRunName, lastRunPhase string
	var lastCreated time.Time
	for _, odag := range s.odags {
		labels := odag.GetLabels()
		if labels["wl.io/template"] == name && odag.GetNamespace() == ns {
			runCount++
			ct := odag.GetCreationTimestamp().Time
			if ct.After(lastCreated) {
				lastCreated = ct
				lastRunName = odag.GetName()
				phase, _, _ := unstructured.NestedString(odag.Object, "status", "phase")
				lastRunPhase = defaultStr(phase, "Pending")
			}
		}
	}

	profilingEnabled := true
	if v, ok, _ := unstructured.NestedBool(obj.Object, "spec", "profiling", "enabled"); ok {
		profilingEnabled = v
	}

	return map[string]interface{}{
		"name":             name,
		"namespace":        ns,
		"description":      desc,
		"scheduler":        defaultStr(sched, "random"),
		"taskCount":        len(tasks),
		"runCount":         runCount,
		"lastRunMakespan":  lastMakespan,
		"lastRunName":      lastRunName,
		"lastRunPhase":     lastRunPhase,
		"profilingEnabled": profilingEnabled,
		"createdAt":        obj.GetCreationTimestamp().UTC().Format(time.RFC3339),
	}
}

func (s *Server) templateDetail(obj *unstructured.Unstructured) map[string]interface{} {
	summary := s.templateSummary(obj)
	specTasks, _, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	profiling, _, _ := unstructured.NestedMap(obj.Object, "spec", "profiling")
	defaults, _, _ := unstructured.NestedMap(obj.Object, "spec", "defaults")
	retention, _, _ := unstructured.NestedMap(obj.Object, "spec", "retention")
	profileSummary, _, _ := unstructured.NestedMap(obj.Object, "status", "profileSummary")

	summary["spec"] = map[string]interface{}{
		"tasks":     specTasks,
		"profiling": profiling,
		"defaults":  defaults,
		"retention": retention,
	}
	if profileSummary != nil {
		summary["profileSummary"] = profileSummary
	}
	return summary
}

// handleBatchSubmit creates multiple ODAGs with staggered delays.
func (s *Server) handleBatchSubmit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Namespace string `json:"namespace"`
		ODAGs     []struct {
			Name  string                 `json:"name"`
			Delay int                    `json:"delay"`
			Spec  map[string]interface{} `json:"spec"`
		} `json:"odags"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ns := req.Namespace
	if ns == "" {
		ns = "wl-system"
	}

	bgCtx := context.Background()

	// Delete existing ODAGs with these names.
	for _, o := range req.ODAGs {
		_ = s.dynClient.Resource(odagGVR).Namespace(ns).Delete(bgCtx, o.Name, metav1.DeleteOptions{})
	}

	// Respond immediately.
	writeJSON(w, map[string]interface{}{"status": "started", "count": len(req.ODAGs)})

	// Submit with staggered delays in background.
	go func() {
		// Wait for deletions to propagate.
		time.Sleep(3 * time.Second)

		for _, o := range req.ODAGs {
			if o.Delay > 0 {
				time.Sleep(time.Duration(o.Delay) * time.Second)
			}
			cr := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "wl.io/v1",
					"kind":       "ODAG",
					"metadata": map[string]interface{}{
						"name":      o.Name,
						"namespace": ns,
					},
					"spec": o.Spec,
				},
			}
			if _, err := s.dynClient.Resource(odagGVR).Namespace(ns).Create(bgCtx, cr, metav1.CreateOptions{}); err != nil {
				log.Printf("[ui-server] batch create failed for %s: %v", o.Name, err)
			} else {
				log.Printf("[ui-server] batch submitted %s (delay=%ds)", o.Name, o.Delay)
			}
		}
		log.Printf("[ui-server] batch submission complete (%d ODAGs)", len(req.ODAGs))
	}()
}

// handleSSE streams live resource change events to the frontend.
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan []byte, 16)
	s.sseMu.Lock()
	s.sseClients[ch] = struct{}{}
	s.sseMu.Unlock()
	defer func() {
		s.sseMu.Lock()
		delete(s.sseClients, ch)
		s.sseMu.Unlock()
	}()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Send a heartbeat comment every 15s to keep the connection alive.
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case data := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		}
	}
}

func (s *Server) broadcast(resource, eventType, name, namespace string) {
	msg := map[string]string{
		"resource":  resource,
		"eventType": eventType,
		"name":      name,
		"namespace": namespace,
	}
	data, _ := json.Marshal(msg)
	s.sseMu.Lock()
	defer s.sseMu.Unlock()
	for ch := range s.sseClients {
		select {
		case ch <- data:
		default: // drop if client is slow
		}
	}
}

// --------------------------------------------------------------------------
// SQLite history
// --------------------------------------------------------------------------

func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS dag_runs (
			id              TEXT PRIMARY KEY,
			name            TEXT NOT NULL,
			namespace       TEXT NOT NULL,
			phase           TEXT NOT NULL,
			makespan        REAL,
			start_time      TEXT,
			completion_time TEXT,
			created_at      TEXT DEFAULT (datetime('now'))
		)
	`)
	if err != nil {
		return db, err
	}
	// Backfill rows that predate startTime recording: derive from
	// completion_time − makespan when both are present, else use created_at.
	_, _ = db.Exec(`
		UPDATE dag_runs
		SET start_time = datetime(completion_time, '-' || CAST(makespan AS TEXT) || ' seconds')
		WHERE (start_time IS NULL OR start_time = '')
		  AND completion_time IS NOT NULL AND completion_time != ''
		  AND makespan IS NOT NULL AND makespan > 0
	`)
	_, _ = db.Exec(`
		UPDATE dag_runs
		SET start_time = created_at
		WHERE start_time IS NULL OR start_time = ''
	`)
	return db, nil
}

func (s *Server) recordHistory(obj *unstructured.Unstructured) {
	id := string(obj.GetUID()) + "-" + obj.GetResourceVersion()
	name := obj.GetName()
	namespace := obj.GetNamespace()
	phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
	makespan := nestedFloat(obj.Object, "status", "makespan")
	startTime, _, _ := unstructured.NestedString(obj.Object, "status", "startTime")
	completionTime, _, _ := unstructured.NestedString(obj.Object, "status", "completionTime")

	// Fallbacks so historical UIs never render "Invalid Date":
	//   1. derive from completionTime − makespan if both known
	//   2. else use the ODAG's creation timestamp
	if startTime == "" {
		if completionTime != "" && makespan > 0 {
			if ct, err := time.Parse(time.RFC3339, completionTime); err == nil {
				startTime = ct.Add(-time.Duration(makespan * float64(time.Second))).UTC().Format(time.RFC3339)
			}
		}
		if startTime == "" {
			startTime = obj.GetCreationTimestamp().UTC().Format(time.RFC3339)
		}
	}

	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO dag_runs (id, name, namespace, phase, makespan, start_time, completion_time)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, name, namespace, phase, makespan, startTime, completionTime,
	)
	if err != nil {
		log.Printf("[ui-server] error recording history for %s/%s: %v", namespace, name, err)
	}
}

type historyEntry struct {
	RunID          string  `json:"runId"`
	Phase          string  `json:"phase"`
	Makespan       float64 `json:"makespan"`
	StartTime      string  `json:"startTime"`
	CompletionTime string  `json:"completionTime"`
}

// templateHistoryEntry is a per-run record scoped to all runs of a template.
type templateHistoryEntry struct {
	Name           string  `json:"name"`
	RunID          string  `json:"runId"`
	Phase          string  `json:"phase"`
	Makespan       float64 `json:"makespan"`
	StartTime      string  `json:"startTime"`
	CompletionTime string  `json:"completionTime"`
}

// queryTemplateHistory returns all runs whose name starts with "<template>-run-".
// Mirrors the naming scheme used by createRunFromTemplate in the controller.
func (s *Server) queryTemplateHistory(namespace, templateName string) ([]templateHistoryEntry, error) {
	prefix := templateName + "-run-"
	rows, err := s.db.Query(
		`SELECT name, id, phase, COALESCE(makespan, 0), COALESCE(start_time, ''), COALESCE(completion_time, '')
		 FROM dag_runs
		 WHERE namespace = ? AND name LIKE ? AND phase = 'Succeeded'
		 ORDER BY created_at ASC`,
		namespace, prefix+"%",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]templateHistoryEntry, 0)
	for rows.Next() {
		var e templateHistoryEntry
		if err := rows.Scan(&e.Name, &e.RunID, &e.Phase, &e.Makespan, &e.StartTime, &e.CompletionTime); err != nil {
			continue
		}
		result = append(result, e)
	}
	return result, nil
}

func (s *Server) queryHistory(namespace, name string) ([]historyEntry, error) {
	rows, err := s.db.Query(
		`SELECT id, phase, COALESCE(makespan, 0), COALESCE(start_time, ''), COALESCE(completion_time, '')
		 FROM dag_runs WHERE namespace = ? AND name = ? ORDER BY created_at DESC LIMIT 50`,
		namespace, name,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []historyEntry
	for rows.Next() {
		var e historyEntry
		if err := rows.Scan(&e.RunID, &e.Phase, &e.Makespan, &e.StartTime, &e.CompletionTime); err != nil {
			continue
		}
		result = append(result, e)
	}
	if result == nil {
		result = []historyEntry{}
	}
	return result, nil
}

// --------------------------------------------------------------------------
// Response builders
// --------------------------------------------------------------------------

func odagSummary(obj *unstructured.Unstructured) map[string]interface{} {
	tasks, _, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
	sched, _, _ := unstructured.NestedString(obj.Object, "spec", "scheduler")
	makespan := nestedFloat(obj.Object, "status", "makespan")
	startTime, _, _ := unstructured.NestedString(obj.Object, "status", "startTime")
	completionTime, _, _ := unstructured.NestedString(obj.Object, "status", "completionTime")
	return map[string]interface{}{
		"name":           obj.GetName(),
		"namespace":      obj.GetNamespace(),
		"phase":          defaultStr(phase, "Pending"),
		"scheduler":      defaultStr(sched, "random"),
		"taskCount":      len(tasks),
		"makespan":       makespan,
		"startTime":      startTime,
		"completionTime": completionTime,
		"createdAt":      obj.GetCreationTimestamp().UTC().Format(time.RFC3339),
	}
}

func odagDetail(obj *unstructured.Unstructured) map[string]interface{} {
	summary := odagSummary(obj)
	taskStatuses, _, _ := unstructured.NestedSlice(obj.Object, "status", "tasks")
	specTasks, _, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	predictedTasks, _, _ := unstructured.NestedSlice(obj.Object, "status", "predictedTasks")
	predictedFlows, _, _ := unstructured.NestedSlice(obj.Object, "status", "predictedNetworkFlows")
	actualFlows, _, _ := unstructured.NestedSlice(obj.Object, "status", "actualNetworkFlows")
	summary["tasks"] = taskStatuses
	summary["spec"] = map[string]interface{}{"tasks": specTasks}
	if predictedTasks != nil {
		summary["predictedTasks"] = predictedTasks
	}
	if predictedFlows != nil {
		summary["predictedNetworkFlows"] = predictedFlows
	}
	if actualFlows != nil {
		summary["actualNetworkFlows"] = actualFlows
	}
	return summary
}

// --------------------------------------------------------------------------
// Utilities

// nestedFloat reads a status numeric field that may be stored as int64 or
// float64 depending on how the API server round-trips the CRD value.
func nestedFloat(obj map[string]interface{}, fields ...string) float64 {
	val, found, _ := unstructured.NestedFieldNoCopy(obj, fields...)
	if !found || val == nil {
		return 0
	}
	switch v := val.(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	case json.Number:
		f, _ := v.Float64()
		return f
	}
	return 0
}

// --------------------------------------------------------------------------

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	}
	return cfg, nil
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[ui-server] error encoding response: %v", err)
	}
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func defaultStr(s, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return s
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
