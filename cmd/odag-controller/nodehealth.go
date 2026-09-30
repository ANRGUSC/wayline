package main

// Node-loss detection.
//
// A node is lost to the workload when its data agent stops answering: no
// output can be installed there, read from there or pushed from there,
// whatever Kubernetes still says about its pods (a crashed node's pods stay
// Running for minutes). The controller probes the agents of every node an
// active run uses, each second. A node whose agent has not answered for
// WL_NODE_LOSS_GRACE_S seconds (default 5), or whose Kubernetes Ready
// condition is False or Unknown, is declared lost, and every active run is
// handed to recovery (recovery.go). A lost node whose agent answers again
// and whose Ready condition is True is usable by new placements; runs that
// were already repaired do not move back. WL_NODE_LOSS=off disables it.

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

type nodeHealth struct {
	lastOK   time.Time
	firstBad time.Time // first failed probe since the last success
	lost     bool
	lostAt   time.Time
}

var (
	healthMu sync.Mutex
	healthOf = map[string]*nodeHealth{}
)

var (
	nodeLossGrace   = envSeconds("WL_NODE_LOSS_GRACE_S", 5)
	nodeLossEnabled = strings.ToLower(os.Getenv("WL_NODE_LOSS")) != "off"
)

// probeAgentFn asks a node's data agent whether it is alive (tests replace it).
var probeAgentFn = probeAgent

var probeHTTP = &http.Client{Timeout: 800 * time.Millisecond}

func probeAgent(ip string) bool {
	resp, err := probeHTTP.Get(fmt.Sprintf("http://%s:%d/healthz", ip, dataAgentPort))
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func envSeconds(name string, def float64) time.Duration {
	if v, err := strconv.ParseFloat(os.Getenv(name), 64); err == nil && v > 0 {
		return time.Duration(v * float64(time.Second))
	}
	return time.Duration(def * float64(time.Second))
}

// nodeLost reports whether a node is currently lost.
func nodeLost(name string) bool {
	healthMu.Lock()
	defer healthMu.Unlock()
	h := healthOf[name]
	return h != nil && h.lost
}

// lostNodeSet returns the nodes currently lost.
func lostNodeSet() map[string]bool {
	healthMu.Lock()
	defer healthMu.Unlock()
	out := map[string]bool{}
	for n, h := range healthOf {
		if h.lost {
			out[n] = true
		}
	}
	return out
}

// observeNode folds one probe (agent answered?) and the node's Kubernetes
// readiness into its health, and reports a transition.
func observeNode(name string, agentOK, k8sReady bool, now time.Time) (lostNow, restoredNow bool) {
	healthMu.Lock()
	defer healthMu.Unlock()
	h := healthOf[name]
	if h == nil {
		h = &nodeHealth{lastOK: now}
		healthOf[name] = h
	}
	if agentOK {
		h.lastOK = now
		h.firstBad = time.Time{}
	} else if h.firstBad.IsZero() {
		h.firstBad = now
	}
	down := !k8sReady || (!agentOK && now.Sub(h.firstBad) >= nodeLossGrace)
	switch {
	case down && !h.lost:
		h.lost, h.lostAt = true, now
		return true, false
	case h.lost && agentOK && k8sReady:
		h.lost = false
		return false, true
	}
	return false, false
}

// activeNodes are the nodes that running runs are placed on or copy to,
// plus every lost node (so a return is noticed).
func activeNodes() map[string]string {
	out := map[string]string{}
	add := func(n string) {
		if n == "" {
			return
		}
		if ip, ok := nodeIPCache.Load(n); ok {
			out[n] = ip.(string)
		}
	}
	runningODAGs.Range(func(k, _ any) bool {
		key := k.(string)
		if raw, ok := assignmentCache.Load(key); ok {
			for _, ni := range raw.(map[string]nodeInfo) {
				add(ni.name)
			}
		}
		if raw, ok := realizationCache.Load(key); ok {
			for _, e := range raw.([]realizationEntry) {
				for _, n := range e.Copies {
					add(n)
				}
			}
		}
		return true
	})
	for n := range lostNodeSet() {
		add(n)
	}
	return out
}

// watchNodeHealth probes active nodes every second and starts recovery on
// every running run when a node is lost. While any node is lost it re-offers
// every running run to recovery every few seconds, so a run whose repair
// failed part-way (an API error) is repaired on a later tick.
func watchNodeHealth(dynClient dynamic.Interface, client *kubernetes.Clientset) {
	if !nodeLossEnabled {
		log.Printf("[health] node-loss detection disabled (WL_NODE_LOSS=off)")
		return
	}
	log.Printf("[health] node-loss detection on (grace %s)", nodeLossGrace)
	ready := map[string]bool{}
	var readyAt time.Time
	var lastSweep time.Time
	for range time.Tick(time.Second) {
		nodes := activeNodes()
		if len(nodes) == 0 {
			continue
		}
		now := time.Now()
		if now.Sub(readyAt) >= 2*time.Second {
			if r, err := nodeReadiness(client); err == nil {
				ready, readyAt = r, now
			}
		}
		results := make(map[string]bool, len(nodes))
		var mu sync.Mutex
		var wg sync.WaitGroup
		for n, ip := range nodes {
			wg.Add(1)
			go func(n, ip string) {
				defer wg.Done()
				ok := probeAgentFn(ip)
				mu.Lock()
				results[n] = ok
				mu.Unlock()
			}(n, ip)
		}
		wg.Wait()
		newlyLost := false
		for n, ok := range results {
			k8sReady, known := ready[n]
			if !known {
				k8sReady = true // not listed yet: judge by the agent alone
			}
			lostNow, restored := observeNode(n, ok, k8sReady, now)
			if lostNow {
				newlyLost = true
				log.Printf("[health] node %s LOST (agent answered: %v, Ready: %v)", n, ok, k8sReady)
			}
			if restored {
				log.Printf("[health] node %s is back (new placements may use it)", n)
			}
		}
		if newlyLost || (len(lostNodeSet()) > 0 && now.Sub(lastSweep) >= 5*time.Second) {
			lastSweep = now
			runningODAGs.Range(func(k, _ any) bool {
				parts := strings.SplitN(k.(string), "/", 2)
				if len(parts) == 2 {
					go recoverRun(dynClient, client, parts[0], parts[1])
				}
				return true
			})
		}
	}
}

// nodeReadiness maps node name -> Ready condition is True.
func nodeReadiness(client *kubernetes.Clientset) (map[string]bool, error) {
	list, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(list.Items))
	for _, n := range list.Items {
		r := false
		for _, c := range n.Status.Conditions {
			if c.Type == corev1.NodeReady {
				r = c.Status == corev1.ConditionTrue
			}
		}
		out[n.Name] = r
	}
	return out, nil
}
