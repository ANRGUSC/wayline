package main

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
