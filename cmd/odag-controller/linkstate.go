package main

// Live link state: what transfers actually take, against what the plan
// priced.
//
// The plan predicts every transfer's duration from link bandwidths and the
// contention it expects, so a transfer that takes far longer than its own
// prediction is evidence that its link degraded after planning. Raw
// throughput would not be: a node pushing to four consumers at once gives
// each a quarter of its link, as planned. Evidence comes from two places:
// transfers that completed (the sending agent records each), and transfers
// still running well past their prediction (their link cannot be faster
// than size/elapsed). A link is degraded while its latest evidence says so
// and is younger than linkObsTTL; decisions that price transfers then use
// the observed bandwidth, and runs with consumers waiting on the link are
// re-planned (repairRun with a link trigger).

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"log"
	"math"
	"strings"
	"sync"
	"time"
)

const (
	linkObsTTL     = 60 * time.Second
	lateFactor     = 2.0 // a transfer is late past lateFactor x its prediction...
	lateSlackS     = 2.0 // ...plus this many seconds
	minEvidenceB   = 1 << 20
	replanInterval = 2 * time.Second // the stay-or-move estimate sharpens as a late transfer ages
)

type linkObs struct {
	bw       float64 // bytes/s the evidence implies (an upper bound when late)
	degraded bool
	at       time.Time
}

var linkLive sync.Map // "src>dst" -> linkObs

func linkKey(src, dst string) string { return src + ">" + dst }

// observeTransfer records one transfer of `bytes` from src to dst that the
// plan predicted to take predS and has taken actualS so far (finished or
// not). It reports whether the link is newly judged degraded.
func observeTransfer(src, dst string, bytes int64, predS, actualS float64, done bool, now time.Time) bool {
	if src == dst || bytes < minEvidenceB || actualS <= 0 {
		return false
	}
	late := actualS > lateFactor*predS+lateSlackS
	if !late && !done {
		return false // still within its prediction: no evidence either way
	}
	prev, had := linkLive.Load(linkKey(src, dst))
	wasDegraded := had && prev.(linkObs).degraded && now.Sub(prev.(linkObs).at) < linkObsTTL
	linkLive.Store(linkKey(src, dst), linkObs{bw: float64(bytes) / actualS, degraded: late, at: now})
	if late && !wasDegraded {
		log.Printf("[links] %s -> %s degraded: %.1f MB took %.1fs (planned %.1fs)",
			src, dst, float64(bytes)/1e6, actualS, predS)
		return true
	}
	if !late && wasDegraded {
		log.Printf("[links] %s -> %s recovered: %.1f MB in %.1fs (planned %.1fs)",
			src, dst, float64(bytes)/1e6, actualS, predS)
	}
	return false
}

// linkDegraded reports whether recent evidence says src -> dst is degraded.
func linkDegraded(src, dst string, now time.Time) bool {
	v, ok := linkLive.Load(linkKey(src, dst))
	if !ok {
		return false
	}
	o := v.(linkObs)
	return o.degraded && now.Sub(o.at) < linkObsTTL
}

// liveBandwidth wraps a model's bandwidth: a degraded link is priced at
// what its evidence implies.
func liveBandwidth(model func(src, dst string) float64) func(src, dst string) float64 {
	return func(src, dst string) float64 {
		b := model(src, dst)
		if v, ok := linkLive.Load(linkKey(src, dst)); ok {
			if o := v.(linkObs); o.degraded && time.Since(o.at) < linkObsTTL && o.bw < b {
				return o.bw
			}
		}
		return b
	}
}

// predictedTransferS is how long the plan expects the edge prod -> cons to
// take: its predicted flow, or the object's size over the model link when
// the plan had no flow there (a task that moved since).
func (m *planModel) predictedTransferS(obj, prod, cons, src, dst string) float64 {
	for _, f := range m.flows {
		if f.FromTask == prod && f.ToTask == cons && f.SrcNode == src && f.DstNode == dst {
			return math.Max(f.End-f.Start, 0)
		}
	}
	return m.transferTime(obj, src, dst)
}

// ---------------------------------------------------------------------------
// Watching a run's transfers.
// ---------------------------------------------------------------------------

type linkWatch struct {
	mu        sync.Mutex
	busy      bool
	seenFlows map[string]bool      // completed flows already observed
	closed    map[string]float64   // producer -> closeUnix
	lastRun   time.Time            // last re-plan triggered by links
	pending   map[string]time.Time // degraded link keys not yet acted on
}

var linkWatches sync.Map // run key -> *linkWatch

func watchOf(key string) *linkWatch {
	v, _ := linkWatches.LoadOrStore(key, &linkWatch{seenFlows: map[string]bool{},
		closed: map[string]float64{}, pending: map[string]time.Time{}})
	return v.(*linkWatch)
}

// observeRunFlows turns a run's completed flows into link evidence.
func observeRunFlows(key string, flows []dataAgentFlow) {
	v, ok := planModels.Load(key)
	if !ok {
		return
	}
	m := v.(*planModel)
	w := watchOf(key)
	now := time.Now()
	for _, f := range flows {
		id := fmt.Sprintf("%s>%s@%.3f", f.FromTask, f.ToTask, f.StartUnix)
		w.mu.Lock()
		seen := w.seenFlows[id]
		w.seenFlows[id] = true
		w.mu.Unlock()
		if seen || !f.Ok || strings.HasPrefix(f.ToTask, "rev-") {
			continue
		}
		obj := f.FromTask // the agent records the object key as FromTask
		pred := m.predictedTransferS(obj, producerOf(obj), f.ToTask, f.SrcNode, f.DstNode)
		if observeTransfer(f.SrcNode, f.DstNode, f.DataSize, pred, f.EndUnix-f.StartUnix, true, now) {
			w.mu.Lock()
			w.pending[linkKey(f.SrcNode, f.DstNode)] = now
			w.mu.Unlock()
		}
	}
}

// lateInputs finds the run's undelivered inputs whose transfer is running
// past its prediction, records the evidence, and returns the degraded links
// some waiting consumer depends on.
func lateInputs(key, odagName string, m *planModel, assign map[string]nodeInfo,
	done, dispatched map[string]bool, now time.Time) map[string]bool {

	w := watchOf(key)
	slow := map[string]bool{}
	for _, c := range m.order {
		if done[c] || dispatched[c] {
			continue
		}
		cn := assign[c]
		if cn.ip == "" || nodeLost(cn.name) {
			continue
		}
		for _, o := range m.inputObjects(c) {
			p := producerOf(o)
			pn := assign[p]
			if !done[p] || pn.name == cn.name || pn.ip == "" || nodeLost(pn.name) {
				continue
			}
			if isDataReady(cn.ip, odagName, o) {
				continue
			}
			st := transferStateOf(pn.ip, odagName, o, c)
			if st != "Pending" && st != "Transferring" {
				continue
			}
			w.mu.Lock()
			closed, ok := w.closed[p]
			w.mu.Unlock()
			if !ok {
				tm := queryTimings(pn.ip, odagName, p)
				c, _ := tm["closeUnix"].(float64)
				if c <= 0 {
					continue
				}
				closed = c
				w.mu.Lock()
				w.closed[p] = c
				w.mu.Unlock()
			}
			elapsed := float64(now.UnixNano())/1e9 - closed
			pred := m.predictedTransferS(o, p, c, pn.name, cn.name)
			observeTransfer(pn.name, cn.name, m.objectBytes(o), pred, elapsed, false, now)
			if linkDegraded(pn.name, cn.name, now) {
				slow[linkKey(pn.name, cn.name)] = true
			}
		}
	}
	return slow
}

// checkRunLinks gathers link evidence for one run from a status pass's
// snapshot and, when a consumer waits on a degraded link, re-plans the run
// (at most every replanInterval).
func checkRunLinks(a statusArgs, pods []corev1.Pod, assignMap map[string]nodeInfo) {
	key := a.namespace + "/" + a.odagName
	v, ok := planModels.Load(key)
	if !ok {
		return
	}
	m := v.(*planModel)
	if !m.policy.Revise {
		return // frozen: the run keeps its plan-time decisions
	}
	w := watchOf(key)
	w.mu.Lock()
	if w.busy {
		w.mu.Unlock()
		return
	}
	w.busy = true
	w.mu.Unlock()
	defer func() { w.mu.Lock(); w.busy = false; w.mu.Unlock() }()

	done, dispatched := map[string]bool{}, map[string]bool{}
	for _, p := range pods {
		t := p.Labels[labelTaskName]
		dispatched[t] = true
		if p.Status.Phase == corev1.PodSucceeded {
			done[t] = true
		}
	}
	now := time.Now()
	slow := lateInputs(key, a.odagName, m, assignMap, done, dispatched, now)
	w.mu.Lock()
	for k, at := range w.pending {
		if now.Sub(at) < linkObsTTL {
			slow[k] = true
		}
	}
	due := len(slow) > 0 && now.Sub(w.lastRun) >= replanInterval
	if due {
		w.lastRun = now
		w.pending = map[string]time.Time{}
	}
	w.mu.Unlock()
	if due {
		log.Printf("[links] %s: consumers wait on degraded link(s) %v; re-planning", key, sortedKeys(slow))
		repairRun(a.dynClient, a.client, a.namespace, a.odagName, true)
	}
}
