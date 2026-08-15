package main

// SAGA scheduler bridge.
//
// When an ODAG's spec.scheduler is "saga/<algorithm>" (e.g. "saga/heft",
// "saga/cpop", "saga/minmin"), placement is delegated to the SAGA scheduler
// sidecar (saga-sidecar/server.py), which wraps the SAGA library of DAG
// scheduling algorithms. The sidecar runs as a second container in the
// controller pod and is reached over localhost.
//
// The wire contract is the one specified in sdk/python/wl/scheduler.py
// ({"dag": ..., "clusterState": ...} -> {"assignments": ...}), extended
// backward-compatibly with per-(task,node) runtimeProfile/dataSizeProfile
// maps densified from the controller's resolvers, so the sidecar sees the
// same information the built-in HEFT scheduler does.
//
// Only the task->node assignment from the response is load-bearing: task
// dispatch stays data-readiness-driven, and predicted times are recomputed
// by computePredictedSchedule exactly as the random branch does. On any
// sidecar error the caller falls back to the built-in HEFT scheduler, so a
// dead sidecar degrades placement quality, never availability.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

const sagaSchedulerPrefix = "saga/"

// sagaSchedulerURL returns the sidecar base URL (no trailing slash).
func sagaSchedulerURL() string {
	if v := os.Getenv("WL_SAGA_SCHEDULER_URL"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return "http://127.0.0.1:8090"
}

var sagaHTTPClient = &http.Client{Timeout: 20 * time.Second}

type sagaTaskRequest struct {
	Name            string             `json:"name"`
	Dependencies    []string           `json:"dependencies"`
	Runtime         float64            `json:"runtime,omitempty"`
	DataSize        string             `json:"dataSize,omitempty"`
	RuntimeProfile  map[string]float64 `json:"runtimeProfile,omitempty"`
	DataSizeProfile map[string]int64   `json:"dataSizeProfile,omitempty"`
	Constraints     *sagaConstraints   `json:"constraints,omitempty"`
}

type sagaConstraints struct {
	NodeNames []string `json:"nodeNames"`
}

type sagaNodeRequest struct {
	Name      string `json:"name"`
	Ready     bool   `json:"ready"`
	CPUMillis int64  `json:"cpuMillis"`
	MemBytes  int64  `json:"memBytes"`
}

type sagaBandwidthEntry struct {
	From        string  `json:"from"`
	To          string  `json:"to"`
	BytesPerSec float64 `json:"bytesPerSec"`
}

type sagaScheduleRequest struct {
	Algorithm    string `json:"algorithm"`
	DAG          struct {
		Tasks []sagaTaskRequest `json:"tasks"`
	} `json:"dag"`
	ClusterState struct {
		Nodes     []sagaNodeRequest    `json:"nodes"`
		Bandwidth []sagaBandwidthEntry `json:"bandwidth"`
	} `json:"clusterState"`
}

type sagaAssignment struct {
	Task string `json:"task"`
	Node string `json:"node"`
}

type sagaScheduleResponse struct {
	Assignments       []sagaAssignment `json:"assignments"`
	EstimatedMakespan float64          `json:"estimatedMakespan"`
	CostModelFitRMSE  float64          `json:"costModelFitRMSE"`
	Error             string           `json:"error"`
}

// sagaAssignTasks delegates placement to the SAGA sidecar. It densifies the
// pull-style resolvers into full matrices over tasks x nodes and nodes x
// nodes. Returns an error (never a partial result) on any failure so the
// caller can fall back.
func sagaAssignTasks(algorithm string, tasks []taskSpec, nodeMap map[string]nodeInfo,
	rtRes runtimeResolver, dsRes dataSizeResolver, bwRes bandwidthResolver) (map[string]nodeInfo, error) {

	nodeNames := make([]string, 0, len(nodeMap))
	for name := range nodeMap {
		nodeNames = append(nodeNames, name)
	}
	sort.Strings(nodeNames) // deterministic request bodies

	var req sagaScheduleRequest
	req.Algorithm = algorithm
	for _, t := range tasks {
		tr := sagaTaskRequest{
			Name:         t.Name,
			Dependencies: t.Dependencies,
			Runtime:      t.Runtime,
			DataSize:     t.DataSize,
		}
		if rtRes != nil {
			tr.RuntimeProfile = make(map[string]float64, len(nodeNames))
			for _, n := range nodeNames {
				tr.RuntimeProfile[n] = rtRes(t.Name, n)
			}
		} else if t.RuntimeProfile != nil {
			tr.RuntimeProfile = t.RuntimeProfile
		}
		if dsRes != nil {
			tr.DataSizeProfile = make(map[string]int64, len(nodeNames))
			for _, n := range nodeNames {
				tr.DataSizeProfile[n] = dsRes(t.Name, n)
			}
		}
		if len(t.Constraints) > 0 {
			tr.Constraints = &sagaConstraints{NodeNames: t.Constraints}
		}
		req.DAG.Tasks = append(req.DAG.Tasks, tr)
	}
	for _, n := range nodeNames {
		ni := nodeMap[n]
		req.ClusterState.Nodes = append(req.ClusterState.Nodes, sagaNodeRequest{
			Name: n, Ready: true, CPUMillis: ni.cpuMillis, MemBytes: ni.memBytes,
		})
	}
	if bwRes != nil {
		for _, u := range nodeNames {
			for _, v := range nodeNames {
				if u == v {
					continue
				}
				req.ClusterState.Bandwidth = append(req.ClusterState.Bandwidth,
					sagaBandwidthEntry{From: u, To: v, BytesPerSec: bwRes(u, v)})
			}
		}
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	resp, err := sagaHTTPClient.Post(sagaSchedulerURL()+"/schedule", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("sidecar unreachable: %w", err)
	}
	defer resp.Body.Close()

	var out sagaScheduleResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode response (HTTP %d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sidecar HTTP %d: %s", resp.StatusCode, out.Error)
	}

	assignMap := make(map[string]nodeInfo, len(tasks))
	for _, a := range out.Assignments {
		ni, ok := nodeMap[a.Node]
		if !ok {
			return nil, fmt.Errorf("sidecar assigned task %q to unknown node %q", a.Task, a.Node)
		}
		assignMap[a.Task] = ni
	}
	for _, t := range tasks {
		ni, ok := assignMap[t.Name]
		if !ok {
			return nil, fmt.Errorf("sidecar left task %q unassigned", t.Name)
		}
		// Constraint enforcement is the sidecar's job (post-override); this
		// is a belt-and-braces check so a buggy sidecar can never place a
		// pinned task off its allowed set.
		if len(t.Constraints) > 0 && !containsString(t.Constraints, ni.name) {
			return nil, fmt.Errorf("sidecar violated constraints for task %q (node %q)", t.Name, ni.name)
		}
	}
	log.Printf("[saga] %s placed %d tasks (sidecar makespan estimate %.1fs, cost-model fit RMSE %.3f)",
		algorithm, len(assignMap), out.EstimatedMakespan, out.CostModelFitRMSE)
	return assignMap, nil
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
