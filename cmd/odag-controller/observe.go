package main

// Observations, published for policies.
//
// The controller watches what policies need to react to but does not react
// itself: which nodes are lost (nodehealth.go) and which links deliver far
// less than the plan priced (linkstate.go). Both are cluster facts, so they
// go to one ConfigMap, wl-system/wl-observations, rewritten whenever either
// changes and at least every few seconds while any is current (a link
// judgement expires after linkObsTTL). Per-run facts stay on the run:
// status.tasks (with "lost": true for a task whose node was lost before it
// finished), status.recovery (what the controller enacted), and the plan
// ConfigMap (planpub.go).

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const observationsConfigMap = "wl-observations"

type observedLink struct {
	Src      string  `json:"src"`
	Dst      string  `json:"dst"`
	Degraded bool    `json:"degraded"`
	Bps      float64 `json:"bps"` // what the evidence implies (an upper bound while a transfer is late)
	AtUnix   float64 `json:"atUnix"`
}

type observations struct {
	UpdatedUnix float64            `json:"updatedUnix"`
	LostNodes   map[string]float64 `json:"lostNodes"` // node -> lost since (unix seconds)
	Links       []observedLink     `json:"links"`     // judgements younger than linkObsTTL
}

// currentObservations snapshots node health and link state.
func currentObservations(now time.Time) observations {
	o := observations{UpdatedUnix: unixSeconds(now), LostNodes: map[string]float64{}}
	healthMu.Lock()
	for n, h := range healthOf {
		if h.lost {
			o.LostNodes[n] = unixSeconds(h.lostAt)
		}
	}
	healthMu.Unlock()
	linkLive.Range(func(k, v any) bool {
		lo := v.(linkObs)
		if now.Sub(lo.at) >= linkObsTTL {
			return true
		}
		parts := strings.SplitN(k.(string), ">", 2)
		if len(parts) == 2 {
			o.Links = append(o.Links, observedLink{Src: parts[0], Dst: parts[1], Degraded: lo.degraded,
				Bps: lo.bw, AtUnix: unixSeconds(lo.at)})
		}
		return true
	})
	sort.Slice(o.Links, func(i, j int) bool {
		if o.Links[i].Src != o.Links[j].Src {
			return o.Links[i].Src < o.Links[j].Src
		}
		return o.Links[i].Dst < o.Links[j].Dst
	})
	return o
}

func unixSeconds(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return float64(t.UnixNano()) / 1e9
}

func fromUnix(s float64) time.Time {
	if s <= 0 {
		return time.Time{}
	}
	sec := int64(s)
	return time.Unix(sec, int64((s-float64(sec))*1e9))
}

var observationKick = make(chan struct{}, 1)

// kickObservations asks the publisher to write now (non-blocking).
func kickObservations() {
	select {
	case observationKick <- struct{}{}:
	default:
	}
}

// publishObservations writes the observations ConfigMap on every kick and,
// while anything is current, every 2 seconds; at most every 200 ms.
func publishObservations(client kubernetes.Interface) {
	var last string
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-observationKick:
		case <-tick.C:
		}
		o := currentObservations(time.Now())
		body, _ := json.Marshal(struct {
			LostNodes map[string]float64 `json:"lostNodes"`
			Links     []observedLink     `json:"links"`
		}{o.LostNodes, o.Links})
		if string(body) == last && len(o.LostNodes) == 0 && len(o.Links) == 0 {
			continue // nothing current, nothing changed
		}
		data, _ := json.Marshal(o)
		if err := writeObservations(client, string(data)); err != nil {
			log.Printf("[observe] write %s: %v", observationsConfigMap, err)
			continue
		}
		last = string(body)
		time.Sleep(200 * time.Millisecond)
	}
}

func writeObservations(client kubernetes.Interface, data string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: observationsConfigMap, Namespace: networkConfigMapNS},
		Data:       map[string]string{"observations.json": data},
	}
	cms := client.CoreV1().ConfigMaps(networkConfigMapNS)
	_, err := cms.Update(ctx, cm, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) {
		_, err = cms.Create(ctx, cm, metav1.CreateOptions{})
	}
	return err
}

// ---------------------------------------------------------------------------
// Reading observations (the policy process).
// ---------------------------------------------------------------------------

var (
	observedMu   sync.Mutex
	observedLast observations
)

// readObservations loads the ConfigMap and folds it into the in-memory
// node-health and link-state tables, so code shared with the controller
// (lostNodeSet, linkDegraded, liveBandwidth) sees the published state.
// It reports whether the set of lost nodes or degraded links changed.
func readObservations(client kubernetes.Interface) (observations, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cm, err := client.CoreV1().ConfigMaps(networkConfigMapNS).Get(ctx, observationsConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return observations{LostNodes: map[string]float64{}}, false, nil
	}
	if err != nil {
		return observations{}, false, err
	}
	var o observations
	if err := json.Unmarshal([]byte(cm.Data["observations.json"]), &o); err != nil {
		return observations{}, false, err
	}
	if o.LostNodes == nil {
		o.LostNodes = map[string]float64{}
	}
	applyObservations(o)

	observedMu.Lock()
	defer observedMu.Unlock()
	changed := !sameLost(observedLast.LostNodes, o.LostNodes) || degradedSet(observedLast) != degradedSet(o)
	observedLast = o
	return o, changed, nil
}

// applyObservations replaces the in-memory health and link tables.
func applyObservations(o observations) {
	healthMu.Lock()
	for n, h := range healthOf {
		if _, lost := o.LostNodes[n]; !lost {
			h.lost = false
		}
	}
	for n, since := range o.LostNodes {
		h := healthOf[n]
		if h == nil {
			h = &nodeHealth{}
			healthOf[n] = h
		}
		h.lost, h.lostAt = true, fromUnix(since)
	}
	healthMu.Unlock()
	keep := map[string]bool{}
	for _, l := range o.Links {
		k := linkKey(l.Src, l.Dst)
		keep[k] = true
		linkLive.Store(k, linkObs{bw: l.Bps, degraded: l.Degraded, at: fromUnix(l.AtUnix)})
	}
	linkLive.Range(func(k, _ any) bool {
		if !keep[k.(string)] {
			linkLive.Delete(k)
		}
		return true
	})
}

func sameLost(a, b map[string]float64) bool {
	if len(a) != len(b) {
		return false
	}
	for n := range a {
		if _, ok := b[n]; !ok {
			return false
		}
	}
	return true
}

func degradedSet(o observations) string {
	var ks []string
	for _, l := range o.Links {
		if l.Degraded {
			ks = append(ks, linkKey(l.Src, l.Dst))
		}
	}
	sort.Strings(ks)
	return strings.Join(ks, ",")
}
