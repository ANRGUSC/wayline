package main

import (
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func testPod(name, node, task string, cpu string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns",
			Labels: map[string]string{labelODAGName: "run", labelTaskName: task}},
		Spec: corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse(cpu)}}}}},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func resetCapacityState() {
	for _, m := range []interface{ Range(func(k, v any) bool) }{&podCache, &warmInvocations, &reservations} {
		m.Range(func(k, _ any) bool {
			switch mm := m.(type) {
			case interface{ Delete(any) }:
				mm.Delete(k)
			}
			return true
		})
	}
}

func TestPodsAndWarmCallsShareOneCPUAccount(t *testing.T) {
	resetCapacityState()
	defer resetCapacityState()
	podCache.Store("ns/p1", testPod("p1", "n1", "w1", "4", corev1.PodRunning))
	podCache.Store("ns/p0", testPod("p0", "n1", "done", "4", corev1.PodSucceeded)) // finished: free
	podCache.Store("ns/p9", testPod("p9", "n2", "other", "4", corev1.PodRunning))  // other node
	warmInvocations.Store("ns/run/uid/w2", &warmInvocation{node: "n1", taskKey: "ns/run/w2", cpuMillis: 2000})
	if got := nodeCPUInUse("n1"); got != 6000 {
		t.Fatalf("in use on n1 = %d, want 6000 (4-CPU pod + 2-CPU warm call)", got)
	}
	// 6 of 7.4 used: a 4-CPU task must wait, a 1-CPU one fits.
	if admitCPU("ns/run/w3", "n1", 4000, 7400) {
		t.Fatal("4-CPU task admitted beside 6 CPUs on a 7.4-CPU node")
	}
	if !admitCPU("ns/run/w4", "n1", 1000, 7400) {
		t.Fatal("1-CPU task refused with 1.4 CPUs free")
	}
	// The reservation counts until its pod is visible, and not twice after.
	if got := nodeCPUInUse("n1"); got != 7000 {
		t.Fatalf("with reservation = %d, want 7000", got)
	}
	podCache.Store("ns/p4", testPod("p4", "n1", "w4", "1", corev1.PodPending))
	if got := nodeCPUInUse("n1"); got != 7000 {
		t.Fatalf("after the pod appeared = %d, want 7000 (no double count)", got)
	}
	// A task larger than the node still runs, alone.
	if !admitCPU("ns/run/big", "n3", 16000, 7400) {
		t.Fatal("oversized task refused on an idle node")
	}
	// A warm call that finished before any check still clears its reservation.
	if !admitCPU("ns/run/quick", "n5", 2000, 7400) {
		t.Fatal("2-CPU task refused on an idle node")
	}
	warmInvocations.Store("ns/run/uid/quick", &warmInvocation{node: "n5", taskKey: "ns/run/quick",
		cpuMillis: 2000, finished: time.Now()})
	if got := nodeCPUInUse("n5"); got != 0 {
		t.Fatalf("finished call still counted = %d, want 0 (reservation must clear)", got)
	}
	// The warm call finishing frees its CPU.
	v, _ := warmInvocations.Load("ns/run/uid/w2")
	v.(*warmInvocation).finished = time.Now()
	if got := nodeCPUInUse("n1"); got != 5000 {
		t.Fatalf("after the warm call finished = %d, want 5000", got)
	}
}

func TestFinishedTaskPodFreesCPUBeforeKubeletReports(t *testing.T) {
	resetCapacityState()
	defer resetCapacityState()
	defer func() { taskStateFn = queryTaskState }()
	for _, m := range []*sync.Map{&computeDone, &computeQueried} {
		m.Range(func(k, _ any) bool { m.Delete(k); return true })
	}
	states := map[string]string{"src": "ComputeDone", "busy": "Running"}
	asked := 0
	taskStateFn = func(ip, odag, task string) string { asked++; return states[task] }
	pod := func(name, task, cpu string) *corev1.Pod {
		p := testPod(name, "n1", task, cpu, corev1.PodRunning)
		p.UID = types.UID("uid-" + name)
		p.Status.HostIP = "10.0.0.1"
		return p
	}
	// The source finished computing but the kubelet still reports Running.
	podCache.Store("ns/src", pod("src", "src", "1"))
	podCache.Store("ns/busy", pod("busy", "busy", "4"))
	// 5 of 6.9 counted: a 2-CPU task fits only once the source is known done.
	if !admitCPU("ns/run/embed", "n1", 2000, 6900) {
		t.Fatal("2-CPU task refused although the 1-CPU pod's task had ended")
	}
	if got := nodeCPUInUse("n1"); got != 6000 {
		t.Fatalf("in use = %d, want 6000 (busy 4 + reserved 2, source freed)", got)
	}
	// A task that still does not fit waits, and the running pod keeps counting.
	if admitCPU("ns/run/more", "n1", 2000, 6900) {
		t.Fatal("2-CPU task admitted beside 6 CPUs on a 6.9-CPU node")
	}
	// A retried task gets a new pod (new UID): it counts again.
	podCache.Delete("ns/src")
	podCache.Store("ns/src2", pod("src2", "src", "1"))
	states["src"] = "Running"
	if got := nodeCPUInUse("n1"); got != 7000 {
		t.Fatalf("after the retry = %d, want 7000 (new pod counted)", got)
	}
	if _, ok := computeDone.Load(types.UID("uid-src")); !ok {
		t.Fatal("expected the old pod's entry until the next refresh")
	}
	refreshComputeDone("n1")
	if _, ok := computeDone.Load(types.UID("uid-src")); ok {
		t.Fatal("entry for a pod no longer cached was not dropped")
	}
	if asked == 0 {
		t.Fatal("the data agent was never asked")
	}
}
