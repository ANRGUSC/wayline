package main

// Warm runners.
//
// A task that names a runner (spec.tasks[].runner) executes as a call on a
// long-lived runner pod on its assigned node instead of in a fresh pod. The
// task contract is unchanged: the runner gives the invocation exactly the
// environment the task's pod would have had, and the SDK talks to the same
// data-agent (Running/ComputeDone state, atomic installs, pushes, timings).
//
// Runners are ordinary pods labeled wl.io/runner=<name> (typically a
// DaemonSet) serving the runner API on port 8090:
//
//	POST /invoke                      {odag, task, function, env} -> 202 (409 if already seen)
//	GET  /invocations/<odag>/<task>   {state, exit} or 404
//
// Inside the controller an invocation is presented as an in-memory pod
// (warmPods), whose phase follows the task state the SDK writes to the
// agent. Dispatch gates, per-task statuses, completion, makespan and the
// profiler therefore treat warm and pod tasks alike. A watcher goroutine
// per invocation polls that state and re-runs dispatch the moment the task
// finishes, instead of waiting for the next 500 ms sweep.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

const (
	labelRunner      = "wl.io/runner"
	runnerPort       = 8090
	warmPollInterval = 50 * time.Millisecond
	warmLivenessTick = 2 * time.Second
)

var warmHTTP = &http.Client{Timeout: 5 * time.Second}

type warmInvocation struct {
	mu       sync.Mutex
	node     string
	nodeIP   string
	endpoint string // runner pod ip:port
	invoked  time.Time
	finished time.Time
	phase    corev1.PodPhase
	reason   string
}

// ns/odag/uid/task -> *warmInvocation. The ODAG UID keeps a recreated run
// of the same name from inheriting a previous incarnation's invocations.
var warmInvocations sync.Map

func warmKey(namespace, odagName string, uid types.UID, task string) string {
	return fmt.Sprintf("%s/%s/%s/%s", namespace, odagName, uid, task)
}

// forgetWarm drops every invocation of a deleted run ("ns/odag").
func forgetWarm(odagKey string) {
	prefix := odagKey + "/"
	warmInvocations.Range(func(k, _ any) bool {
		if strings.HasPrefix(k.(string), prefix) {
			warmInvocations.Delete(k)
		}
		return true
	})
}

type runnerEntry struct {
	endpoint string
	at       time.Time
}

var runnerCache sync.Map // "runner/node" -> runnerEntry

// runnerEndpoint finds a Running, Ready runner pod for `runner` on `node`.
func runnerEndpoint(client *kubernetes.Clientset, runner, node string) (string, error) {
	ck := runner + "/" + node
	if v, ok := runnerCache.Load(ck); ok {
		if e := v.(runnerEntry); time.Since(e.at) < 10*time.Second {
			return e.endpoint, nil
		}
	}
	pods, err := client.CoreV1().Pods("").List(context.Background(), metav1.ListOptions{
		LabelSelector: labelRunner + "=" + runner,
		FieldSelector: "spec.nodeName=" + node,
	})
	if err != nil {
		return "", err
	}
	for _, p := range pods.Items {
		if p.Status.Phase != corev1.PodRunning || p.Status.PodIP == "" || p.DeletionTimestamp != nil {
			continue
		}
		ready := false
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				ready = true
			}
		}
		if !ready {
			continue
		}
		ep := fmt.Sprintf("%s:%d", p.Status.PodIP, runnerPort)
		runnerCache.Store(ck, runnerEntry{endpoint: ep, at: time.Now()})
		return ep, nil
	}
	return "", fmt.Errorf("no ready %q runner on node %s", runner, node)
}

// resolveEnv turns the pod env the controller would inject into plain
// values: literal values as-is, and the two downward-API fields the
// controller uses resolved against the assigned node.
func resolveEnv(env []corev1.EnvVar, node, nodeIP string) map[string]string {
	out := make(map[string]string, len(env))
	for _, e := range env {
		switch {
		case e.ValueFrom == nil:
			out[e.Name] = e.Value
		case e.ValueFrom.FieldRef != nil && e.ValueFrom.FieldRef.FieldPath == "spec.nodeName":
			out[e.Name] = node
		case e.ValueFrom.FieldRef != nil && e.ValueFrom.FieldRef.FieldPath == "status.hostIP":
			out[e.Name] = nodeIP
		}
	}
	return out
}

// invokeWarm claims the task and asks the node's runner to execute it.
// Claims are atomic, so concurrent dispatch passes invoke at most once;
// a failed request releases the claim and the next pass retries.
func invokeWarm(dynClient dynamic.Interface, client *kubernetes.Clientset, namespace, odagName string,
	uid types.UID, task taskSpec, ni nodeInfo, env []corev1.EnvVar) error {

	key := warmKey(namespace, odagName, uid, task.Name)
	inv := &warmInvocation{node: ni.name, nodeIP: ni.ip, invoked: time.Now(), phase: corev1.PodPending}
	if _, loaded := warmInvocations.LoadOrStore(key, inv); loaded {
		return nil
	}
	release := func(err error) error { warmInvocations.Delete(key); return err }

	ep, err := runnerEndpoint(client, task.Runner, ni.name)
	if err != nil {
		return release(err)
	}
	fn := task.Function
	if fn == "" {
		fn = task.Name
	}
	vars := resolveEnv(env, ni.name, ni.ip)
	vars["WL_RUNNER"] = task.Runner
	vars["WL_FUNCTION"] = fn
	// The runner admits calls by CPU, like the kubelet admits pods by request.
	vars["WL_CPU_MILLIS"] = strconv.FormatInt(parseTaskCPUMillis(task.CPU), 10)
	body, _ := json.Marshal(map[string]interface{}{
		"odag": odagName, "task": task.Name, "function": fn, "env": vars,
	})
	resp, err := warmHTTP.Post("http://"+ep+"/invoke", "application/json", bytes.NewReader(body))
	if err != nil {
		runnerCache.Delete(task.Runner + "/" + ni.name)
		return release(err)
	}
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted, http.StatusConflict:
	default:
		return release(fmt.Errorf("runner %s answered %d", ep, resp.StatusCode))
	}
	inv.mu.Lock()
	inv.endpoint = ep
	inv.phase = corev1.PodRunning
	inv.mu.Unlock()
	go watchWarm(dynClient, client, namespace, odagName, task.Name, inv)
	return nil
}

// watchWarm follows one invocation to its end, then re-runs dispatch
// immediately (and a few more times while its outputs finish installing
// on consumer nodes), so successors are not held to the 500 ms sweep.
func watchWarm(dynClient dynamic.Interface, client *kubernetes.Clientset,
	namespace, odagName, taskName string, inv *warmInvocation) {

	kick := func() {
		obj, err := dynClient.Resource(odagGVR).Namespace(namespace).Get(
			context.Background(), odagName, metav1.GetOptions{})
		if err == nil {
			processReadyTasks(dynClient, client, namespace, odagName, obj)
		}
	}
	lastLive := time.Now()
	for {
		time.Sleep(warmPollInterval)
		state := queryTaskState(inv.nodeIP, odagName, taskName)
		if state == "ComputeDone" || state == "Failed" {
			fin := time.Now()
			if tm := queryTimings(inv.nodeIP, odagName, taskName); tm != nil {
				if c, ok := tm["closeUnix"].(float64); ok && c > 0 {
					fin = time.Unix(0, int64(c*1e9))
				}
			}
			inv.mu.Lock()
			inv.finished = fin
			if state == "ComputeDone" {
				inv.phase = corev1.PodSucceeded
				inv.reason = "Completed"
			} else {
				inv.phase = corev1.PodFailed
				inv.reason = "Error"
			}
			inv.mu.Unlock()
			break
		}
		if time.Since(lastLive) >= warmLivenessTick {
			lastLive = time.Now()
			if lost := runnerLost(inv.endpoint, odagName, taskName); lost != "" {
				inv.mu.Lock()
				inv.finished = time.Now()
				inv.phase = corev1.PodFailed
				inv.reason = lost
				inv.mu.Unlock()
				break
			}
		}
	}
	for _, d := range []time.Duration{0, 100 * time.Millisecond, 250 * time.Millisecond,
		500 * time.Millisecond, time.Second} {
		time.Sleep(d)
		kick()
	}
}

// runnerLost reports why an invocation can no longer finish ("" if it can):
// its runner is unreachable, has forgotten it (restarted), or saw the
// child exit without the SDK reporting a final state.
func runnerLost(endpoint, odagName, taskName string) string {
	resp, err := warmHTTP.Get(fmt.Sprintf("http://%s/invocations/%s/%s", endpoint, odagName, taskName))
	if err != nil {
		return "RunnerUnreachable"
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "RunnerRestarted"
	}
	var st struct {
		State string `json:"state"`
		Exit  *int   `json:"exit"`
	}
	if json.NewDecoder(resp.Body).Decode(&st) == nil && st.State == "exited" && st.Exit != nil && *st.Exit != 0 {
		return fmt.Sprintf("ExitCode%d", *st.Exit)
	}
	return ""
}

// warmPods presents this run's warm invocations as pods.
func warmPods(namespace, odagName string, uid types.UID, tasks []taskSpec) []corev1.Pod {
	var out []corev1.Pod
	for _, t := range tasks {
		if t.Runner == "" {
			continue
		}
		v, ok := warmInvocations.Load(warmKey(namespace, odagName, uid, t.Name))
		if !ok {
			continue
		}
		inv := v.(*warmInvocation)
		inv.mu.Lock()
		p := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-%s", odagName, t.Name),
				Namespace: namespace,
				Labels: map[string]string{
					labelODAGName: odagName, labelTaskName: t.Name, labelRunner: t.Runner,
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "wl.io/v1", Kind: "ODAG", Name: odagName, UID: uid,
				}},
			},
			Spec: corev1.PodSpec{NodeName: inv.node},
			Status: corev1.PodStatus{
				Phase:     inv.phase,
				StartTime: &metav1.Time{Time: inv.invoked},
			},
		}
		if !inv.finished.IsZero() {
			code := int32(0)
			if inv.phase == corev1.PodFailed {
				code = 1
			}
			p.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name: t.Name,
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   code,
					Reason:     inv.reason,
					StartedAt:  metav1.Time{Time: inv.invoked},
					FinishedAt: metav1.Time{Time: inv.finished},
				}},
			}}
		}
		inv.mu.Unlock()
		out = append(out, p)
	}
	return out
}
