package main

// Run-data garbage collection for deleted runs.
//
// Retention (cleanupRunData) evicts older runs of a template that still
// exist. Nothing removed the data of a run that was itself deleted, so every
// `wayline delete` and every run removed by the run-count limit left its
// outputs on every node for good; at 4x E11 data sizes that filled workers
// and put them under disk pressure mid-experiment.
//
// Two paths close that:
//   - gcDeletedRun: when a run is deleted, delete its data on every node.
//   - sweepOrphanData: periodically, delete data whose run no longer exists,
//     for leaks from before this change or from a controller that was down.
//     A directory is deleted only after two consecutive sweeps find it
//     orphaned, so a run being created is never raced.
//
// Kept regardless: data of templates whose retention policy is "none", and
// data the cross-run cache still points at.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

const orphanSweepInterval = 5 * time.Minute

// Removing a large run on eMMC takes seconds; the 2 s shared client timed out.
var gcHTTP = &http.Client{Timeout: 2 * time.Minute}

func deleteRunDataOnNodeGC(nodeIP, odagName string) error {
	req, err := http.NewRequest(http.MethodDelete,
		fmt.Sprintf("http://%s:%d/data/%s", nodeIP, dataAgentPort, odagName), nil)
	if err != nil {
		return err
	}
	resp, err := gcHTTP.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

// cacheHolds reports whether the cross-run cache points at data of this run.
func cacheHolds(odagName string) bool {
	held := false
	cacheRegistry.Range(func(_, v any) bool {
		if v.(cacheEntry).Odag == odagName {
			held = true
			return false
		}
		return true
	})
	return held
}

// retentionKeepsData reports whether the run's template asks to keep data
// forever (policy "none").
func retentionKeepsData(dynClient dynamic.Interface, namespace, templateName string) bool {
	if templateName == "" {
		return false
	}
	tmpl, err := dynClient.Resource(odagTemplateGVR).Namespace(namespace).Get(
		context.Background(), templateName, metav1.GetOptions{})
	if err != nil {
		return false
	}
	return extractDataRetentionConfig(tmpl).Policy == "none"
}

func gcDeleteEverywhere(client *kubernetes.Clientset, odagName, why string) {
	nodes, err := getNodeInfoMap(client)
	if err != nil {
		log.Printf("[data-gc] %s: listing nodes: %v", odagName, err)
		return
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	failed := []string{}
	for _, ni := range nodes {
		if ni.ip == "" {
			continue
		}
		wg.Add(1)
		go func(ni nodeInfo) {
			defer wg.Done()
			if err := deleteRunDataOnNodeGC(ni.ip, odagName); err != nil {
				mu.Lock()
				failed = append(failed, fmt.Sprintf("%s: %v", ni.name, err))
				mu.Unlock()
			}
		}(ni)
	}
	wg.Wait()
	if len(failed) > 0 {
		log.Printf("[data-gc] %s (%s): failed on %v; the orphan sweep will retry", odagName, why, failed)
		return
	}
	log.Printf("[data-gc] %s (%s): data removed on %d node(s)", odagName, why, len(nodes))
}

// gcDeletedRun removes a deleted run's data on every node.
func gcDeletedRun(dynClient dynamic.Interface, client *kubernetes.Clientset, obj *unstructured.Unstructured) {
	name := obj.GetName()
	if cacheHolds(name) {
		log.Printf("[data-gc] %s deleted; data kept (cross-run cache refers to it)", name)
		return
	}
	if retentionKeepsData(dynClient, obj.GetNamespace(), obj.GetLabels()["wl.io/template"]) {
		log.Printf("[data-gc] %s deleted; data kept (template retention policy is none)", name)
		return
	}
	gcDeleteEverywhere(client, name, "run deleted")
}

type agentRun struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func listAgentRuns(nodeIP string) ([]agentRun, error) {
	resp, err := gcHTTP.Get(fmt.Sprintf("http://%s:%d/runs", nodeIP, dataAgentPort))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var runs []agentRun
	return runs, json.NewDecoder(resp.Body).Decode(&runs)
}

// templateOfRun recovers "<template>" from a run name "<template>-run-<id>".
func templateOfRun(name string) string {
	if i := strings.LastIndex(name, "-run-"); i > 0 {
		return name[:i]
	}
	return ""
}

// sweepOrphanData runs forever, removing data of runs that no longer exist.
func sweepOrphanData(dynClient dynamic.Interface, client *kubernetes.Clientset) {
	suspects := map[string]bool{} // node/run seen orphaned in the previous sweep
	for {
		time.Sleep(orphanSweepInterval)
		list, err := dynClient.Resource(odagGVR).Namespace("").List(context.Background(), metav1.ListOptions{})
		if err != nil {
			log.Printf("[data-gc] sweep: listing runs: %v", err)
			continue
		}
		exists := make(map[string]string, len(list.Items)) // run -> namespace
		for _, it := range list.Items {
			exists[it.GetName()] = it.GetNamespace()
		}
		nodes, err := getNodeInfoMap(client)
		if err != nil {
			continue
		}
		next := map[string]bool{}
		removed, freed := 0, int64(0)
		for _, ni := range nodes {
			if ni.ip == "" {
				continue
			}
			runs, err := listAgentRuns(ni.ip)
			if err != nil {
				continue
			}
			for _, r := range runs {
				if _, ok := exists[r.Name]; ok || strings.HasPrefix(r.Name, ".") ||
					strings.HasPrefix(r.Name, "_") || r.Name == "lost+found" || cacheHolds(r.Name) {
					continue
				}
				if retentionKeepsData(dynClient, "wl-system", templateOfRun(r.Name)) {
					continue
				}
				key := ni.name + "/" + r.Name
				if !suspects[key] {
					next[key] = true // delete only if still orphaned next sweep
					continue
				}
				if err := deleteRunDataOnNodeGC(ni.ip, r.Name); err != nil {
					next[key] = true
					continue
				}
				removed++
				freed += r.Size
			}
		}
		suspects = next
		if removed > 0 {
			log.Printf("[data-gc] sweep: removed %d orphaned run dir(s), %.1f GB", removed, float64(freed)/1e9)
		}
	}
}
