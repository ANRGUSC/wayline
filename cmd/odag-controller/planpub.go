package main

// The plan a run was placed with, published for policies.
//
// Policies run outside the controller and decide with the numbers that
// placed the run: every task's runtime on every node it could run on, every
// output's size, every link's bandwidth, the predicted schedule and the
// current placement. The controller writes them to a ConfigMap owned by the
// run (wl-plan-<run>), once at deployment and again whenever an enacted
// placement changes where a task runs. Keeping them out of the run object
// keeps the object small: it is read on every dispatch pass.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const planConfigMapPrefix = "wl-plan-"

type publishedBandwidth struct {
	DefaultBps float64            `json:"defaultBps"`
	Links      map[string]float64 `json:"links,omitempty"` // "src>dst" -> bytes/s, where it differs
}

// publishedPlan is the run's plan model in published form. Per-task tables
// use "*" for a value that is the same on every node.
type publishedPlan struct {
	Run            string                        `json:"run"`
	UID            string                        `json:"uid"`
	DeployedAtUnix float64                       `json:"deployedAtUnix"`
	Generation     int                           `json:"generation"` // bumps on every placement change
	Placement      map[string]string             `json:"placement"`  // current task -> node
	Nodes          []string                      `json:"nodes"`      // nodes the run may use
	RuntimeS       map[string]map[string]float64 `json:"runtimeS"`
	OutputBytes    map[string]map[string]int64   `json:"outputBytes"`
	Bandwidth      publishedBandwidth            `json:"bandwidth"`
	Predicted      []predictedTaskEntry          `json:"predicted"`
	Flows          []predictedFlowEntry          `json:"flows"`
}

var (
	planPubMu       sync.Mutex
	planGenerations = map[string]int{} // run key -> last published generation
)

func planConfigMapName(run string) string {
	n := planConfigMapPrefix + run
	if len(n) > 253 {
		n = n[:253]
	}
	return n
}

// buildPublishedPlan renders a plan model.
func buildPublishedPlan(key string, uid types.UID, m *planModel) publishedPlan {
	nodes := make([]string, 0, len(m.nodes))
	for n := range m.nodes {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	p := publishedPlan{
		Run: key, UID: string(uid), Placement: map[string]string{}, Nodes: nodes,
		RuntimeS: map[string]map[string]float64{}, OutputBytes: map[string]map[string]int64{},
		Flows: m.flows,
	}
	if !m.deployed.IsZero() {
		p.DeployedAtUnix = float64(m.deployed.UnixNano()) / 1e9
	}
	for t, n := range m.assign {
		p.Placement[t] = n
	}
	for _, t := range m.order {
		rt := map[string]float64{}
		ds := map[string]int64{}
		for _, n := range nodes {
			rt[n] = round3(m.runtime(t, n))
			ds[n] = m.dataSize(t, n)
		}
		p.RuntimeS[t] = collapse(rt)
		p.OutputBytes[t] = collapse(ds)
		p.Predicted = append(p.Predicted, predictedTaskEntry{Name: t, Node: m.assign[t],
			EstStart: m.est[t], EstEnd: m.eft[t]})
	}
	// Bandwidth: the most common value is the default; list the rest.
	count := map[float64]int{}
	all := map[string]float64{}
	for _, a := range nodes {
		for _, b := range nodes {
			if a == b {
				continue
			}
			v := m.bw(a, b)
			all[linkKey(a, b)] = v
			count[v]++
		}
	}
	def, best := heftDefaultBandwidth, -1
	for v, c := range count {
		if c > best || (c == best && v > def) {
			def, best = v, c
		}
	}
	p.Bandwidth.DefaultBps = def
	for k, v := range all {
		if v != def {
			if p.Bandwidth.Links == nil {
				p.Bandwidth.Links = map[string]float64{}
			}
			p.Bandwidth.Links[k] = v
		}
	}
	return p
}

// collapse writes {"*": v} when every node has the same value.
func collapse[V comparable](byNode map[string]V) map[string]V {
	var first V
	same, seen := true, false
	for _, v := range byNode {
		if !seen {
			first, seen = v, true
		} else if v != first {
			same = false
			break
		}
	}
	if same && seen {
		return map[string]V{"*": first}
	}
	return byNode
}

func lookupNode[V any](t map[string]map[string]V, task, node string) (V, bool) {
	var zero V
	byNode, ok := t[task]
	if !ok {
		return zero, false
	}
	if v, ok := byNode[node]; ok {
		return v, true
	}
	if v, ok := byNode["*"]; ok {
		return v, true
	}
	return zero, false
}

// publishPlan writes (or rewrites) the run's plan ConfigMap.
func publishPlan(client kubernetes.Interface, namespace, name string, uid types.UID, m *planModel) {
	if client == nil || m == nil {
		return
	}
	key := namespace + "/" + name
	p := buildPublishedPlan(key, uid, m)
	planPubMu.Lock()
	planGenerations[key]++
	p.Generation = planGenerations[key]
	planPubMu.Unlock()
	data, err := json.Marshal(p)
	if err != nil {
		return
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: planConfigMapName(name), Namespace: namespace,
			Labels: map[string]string{"wl.io/plan-of": name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "wl.io/v1", Kind: "ODAG", Name: name, UID: uid,
			}},
		},
		Data: map[string]string{"plan.json": string(data)},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cms := client.CoreV1().ConfigMaps(namespace)
	_, err = cms.Update(ctx, cm, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) {
		_, err = cms.Create(ctx, cm, metav1.CreateOptions{})
	}
	if err != nil {
		log.Printf("[plan] %s: publish plan: %v", key, err)
	}
}

// loadPublishedPlan reads a run's plan ConfigMap.
func loadPublishedPlan(client kubernetes.Interface, namespace, name string) (*publishedPlan, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cm, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, planConfigMapName(name), metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	var p publishedPlan
	if err := json.Unmarshal([]byte(cm.Data["plan.json"]), &p); err != nil {
		return nil, fmt.Errorf("decode plan of %s/%s: %w", namespace, name, err)
	}
	return &p, nil
}

// modelFromPublished rebuilds a plan model from its published form. The
// node map carries each node's capacity and address from the cluster; nodes
// the plan names that the cluster no longer lists are kept by name.
func modelFromPublished(p *publishedPlan, tasks []taskSpec, cluster map[string]nodeInfo) *planModel {
	nodeMap := map[string]nodeInfo{}
	for _, n := range p.Nodes {
		ni, ok := cluster[n]
		if !ok {
			ni = nodeInfo{name: n}
		}
		nodeMap[n] = ni
	}
	assign := map[string]nodeInfo{}
	for t, n := range p.Placement {
		ni := nodeMap[n]
		ni.name = n
		assign[t] = ni
	}
	rt := func(task, node string) float64 {
		v, _ := lookupNode(p.RuntimeS, task, node)
		return v
	}
	ds := func(task, node string) int64 {
		v, _ := lookupNode(p.OutputBytes, task, node)
		return v
	}
	bw := func(src, dst string) float64 {
		if v, ok := p.Bandwidth.Links[linkKey(src, dst)]; ok {
			return v
		}
		return p.Bandwidth.DefaultBps
	}
	m := newPlanModel(tasks, assign, nodeMap, rt, ds, bw, p.Predicted, p.Flows)
	if p.DeployedAtUnix > 0 {
		sec := int64(p.DeployedAtUnix)
		m.deployed = time.Unix(sec, int64((p.DeployedAtUnix-float64(sec))*1e9))
	}
	return m
}
