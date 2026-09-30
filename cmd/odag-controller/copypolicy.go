package main

// Plan-aware copies (spec.realizationPolicy.mode: planAware).
//
// The plan places tasks; this policy uses the same plan to decide which
// outputs get an extra copy and where. For every consumer c, consider losing
// c's planned node X just before c starts. c then moves to the node where it
// would finish earliest (its fallback f, found with the planner's cost
// model). Each input o of c reaches f from the nearest copy that survives
// the loss or, when every planned copy of o was on X, only after o's
// producer runs again. The delay beyond c's slack is what copies already on
// f avoid; each copy costs one transfer of o. The policy adds the set of
// copies that avoids the most delay net of `threshold` times their transfer
// time, and none when no set pays for itself.
//
// Under push delivery a consumed object already has a copy on its producer's
// node and on every consumer's node, so a lone copy arises where the plan
// co-located a producer with all its consumers. Those are the objects a loss
// forces the run to recompute, and the ones this policy protects.
//
// The copies are written into spec.realization with origin "plan" and
// converged like any other revision (realization.go); entries a user wrote
// for the same object take precedence.

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

const (
	policyPlanAware = "planAware"
	policyStatic    = "static"

	unrecoverable = 1e9 // seconds: the delay of an input no copy or re-run can provide

	originPlan     = "plan"     // written by the plan-aware policy
	originStatic   = "static"   // written by the static replication rule
	originRecovery = "recovery" // written by node-loss recovery
)

type realizationPolicy struct {
	Mode         string
	Threshold    float64 // copy when avoided delay >= Threshold x copy time
	PodStartupS  float64 // dispatch-to-start of a pod task that must run again
	WarmStartupS float64 // the same for a warm call
	MaxCopyBytes int64   // total bytes the policy may copy per run; 0 = no limit
	MinGainS     float64 // ignore copies that avoid less delay than this
	Replicas     int     // static mode: total copies of every consumed object
	// Revise: keep the plan's decisions true as the run changes (re-plan
	// around degraded links, re-protect after a repair). false freezes the
	// copies chosen at plan time, as a plan-time-only system would.
	Revise bool
}

func defaultRealizationPolicy() realizationPolicy {
	return realizationPolicy{Threshold: 2, PodStartupS: 3, WarmStartupS: 0.1, MinGainS: 1,
		Replicas: 2, Revise: true}
}

func parseRealizationPolicy(obj *unstructured.Unstructured) realizationPolicy {
	p := defaultRealizationPolicy()
	m, found, _ := unstructured.NestedMap(obj.Object, "spec", "realizationPolicy")
	if !found {
		return p
	}
	p.Mode, _ = m["mode"].(string)
	num := func(k string, dst *float64) {
		switch v := m[k].(type) {
		case float64:
			*dst = v
		case int64:
			*dst = float64(v)
		}
	}
	num("threshold", &p.Threshold)
	num("podStartupS", &p.PodStartupS)
	num("warmStartupS", &p.WarmStartupS)
	num("minGainS", &p.MinGainS)
	var replicas float64
	num("replicas", &replicas)
	if replicas >= 1 {
		p.Replicas = int(replicas)
	}
	if v, ok := m["revise"].(bool); ok {
		p.Revise = v
	}
	if s, ok := m["maxCopyBytes"].(string); ok {
		p.MaxCopyBytes = parseDataSizeBytes(s)
	}
	return p
}

// startup is how long a task takes from dispatch to running user code.
func (p realizationPolicy) startup(t *taskSpec) float64 {
	switch {
	case t == nil || t.Type == "data":
		return 0
	case t.Runner != "":
		return p.WarmStartupS
	default:
		return p.PodStartupS
	}
}

// copyDecision records why the policy did or did not copy an object for one
// consumer; the list lands in status.realizationPolicy.decisions.
type copyDecision struct {
	Object    string  `json:"object"`
	Consumer  string  `json:"consumer"`
	Planned   string  `json:"plannedNode"`
	Fallback  string  `json:"fallback,omitempty"`
	Recompute bool    `json:"recompute"` // no copy survives: the producer must run again
	DelayS    float64 `json:"delayS"`    // input arrival on the fallback after the loss, no extra copy; -1: never
	SlackS    float64 `json:"slackS"`
	CopyS     float64 `json:"copyS"` // transfer time of the copy
	Source    string  `json:"source,omitempty"`
	Copy      bool    `json:"copy"`
	Reason    string  `json:"reason"`
}

// lossScenario answers "when can object o be on node n" after the nodes in
// `lost` fail at time `at`, given where copies were when they failed. An
// object with no surviving copy is recomputed: its producer runs again where
// it would finish earliest, recursively for its own lost inputs.
type lossScenario struct {
	m       *planModel
	lost    map[string]bool
	at      float64
	holders func(obj string) []string
	startup func(task string) float64
	tl      map[string]*nodeTimeline
	redo    map[string]redoResult // producer -> its re-run
}

type redoResult struct {
	node   string
	finish float64
}

func (s *lossScenario) arrival(obj, n string) float64 {
	best := math.Inf(1)
	for _, h := range s.holders(obj) {
		if !s.lost[h] {
			best = math.Min(best, s.at+s.m.transferTime(obj, h, n))
		}
	}
	if !math.IsInf(best, 1) {
		return best
	}
	r := s.rerun(producerOf(obj))
	if r.node == "" {
		return math.Inf(1)
	}
	return r.finish + s.m.transferTime(obj, r.node, n)
}

func (s *lossScenario) rerun(prod string) redoResult {
	if r, ok := s.redo[prod]; ok {
		return r
	}
	s.redo[prod] = redoResult{finish: math.Inf(1)} // guards a malformed cycle
	node, eft := s.m.bestNode(prod, s.lost, s.tl, s.at+s.startup(prod), s.arrival)
	r := redoResult{node: node, finish: eft}
	if node == "" {
		r.finish = math.Inf(1)
	}
	s.redo[prod] = r
	return r
}

// policyContext carries a live run's state into a re-application of the
// policy after a repair: nodes known to hold each object now, and consumers
// that no longer need protecting (finished or running).
type policyContext struct {
	holders map[string][]string
	skip    map[string]bool
}

// planAwareCopies decides the plan's extra copies. `existing` is the run's
// current spec.realization; objects a user entry governs are left to it.
func planAwareCopies(m *planModel, p realizationPolicy,
	existing []realizationEntry, ctx policyContext) ([]realizationEntry, []copyDecision) {

	userGoverned := map[string]bool{}
	for _, e := range existing {
		if e.Origin == "" {
			userGoverned[e.Object] = true
		}
	}
	startup := func(task string) float64 { return p.startup(m.byName[task]) }
	copies := map[string]map[string]bool{} // object -> nodes, decided so far
	received := map[string]int64{}         // node -> bytes of copies decided for it
	// Where an object has a copy: where the plan leaves one, plus copies
	// already decided. Consumers are visited in topological order, so an
	// upstream copy (say, of a pinned sensor's capture) already counts when
	// deciding whether its consumers' outputs need one too.
	holders := func(obj string) []string {
		set := map[string]bool{}
		for _, n := range m.plannedHolders(obj) {
			set[n] = true
		}
		for n := range copies[obj] {
			set[n] = true
		}
		for _, n := range ctx.holders[obj] {
			set[n] = true
		}
		return sortedKeys(set)
	}

	var decisions []copyDecision
	var budget int64

	for _, c := range m.order {
		inputs := m.inputObjects(c)
		if len(inputs) == 0 || ctx.skip[c] {
			continue
		}
		x := m.assign[c]
		lost := map[string]bool{x: true}
		// Every task planned on X moves too; the fallback search sees the
		// other nodes' planned load only.
		skip := map[string]bool{}
		for t, n := range m.assign {
			if n == x {
				skip[t] = true
			}
		}
		s := &lossScenario{m: m, lost: lost, at: m.est[c], holders: holders,
			startup: startup, tl: m.timelines(skip), redo: map[string]redoResult{}}
		ranked := m.rankNodes(c, lost, s.tl, s.at+startup(c), s.arrival)
		if len(ranked) == 0 || math.IsInf(ranked[0].eft, 1) {
			// Without a copy the consumer cannot run anywhere (an input's
			// producer is pinned to X). Its fallback is then where it would
			// finish earliest with its inputs copied in advance.
			copied := func(o, n string) float64 {
				best := math.Inf(1)
				for _, h := range holders(o) {
					best = math.Min(best, s.at+m.transferTime(o, h, n))
				}
				return best
			}
			ranked = m.rankNodes(c, lost, s.tl, s.at+startup(c), copied)
		}
		f := spreadFallback(ranked, s.at, received)
		slack := m.slack(c)

		type cand struct {
			obj, src     string
			delay, xfer  float64
			recompute    bool
			alreadyThere bool
		}
		var cands []cand
		for _, o := range inputs {
			d := copyDecision{Object: o, Consumer: c, Planned: x, Fallback: f, SlackS: round3(slack)}
			if f == "" {
				d.Reason = "no fallback node (constrained to the planned node)"
				decisions = append(decisions, d)
				continue
			}
			k := cand{obj: o}
			surv := false
			for _, h := range holders(o) {
				if h == f {
					k.alreadyThere = true
				}
				if !lost[h] {
					surv = true
				}
			}
			k.recompute = !surv
			// An input that cannot be recovered at all counts as a very
			// long delay (finite, so sums and differences stay numbers).
			k.delay = math.Min(unrecoverable, math.Max(0, s.arrival(o, f)-s.at))
			// The copy is made before any loss, from the planned holder
			// with the fastest link to f (X included: it is alive then).
			k.xfer = math.Inf(1)
			for _, h := range holders(o) {
				if t := m.transferTime(o, h, f); h != f && t < k.xfer {
					k.src, k.xfer = h, t
				}
			}
			cands = append(cands, k)
		}
		if len(cands) == 0 {
			continue
		}
		// Choose the prefix of inputs, by decreasing delay, that avoids the
		// most delay net of the copies' cost. The consumer waits for its
		// slowest input, so copying one input helps only as far as the next.
		sort.SliceStable(cands, func(i, j int) bool { return cands[i].delay > cands[j].delay })
		impact := func(d float64) float64 { return math.Max(0, d-slack) }
		full := impact(cands[0].delay)
		bestK, bestNet, cost := 0, 0.0, 0.0
		for k := range cands {
			if cands[k].alreadyThere {
				break // this and every later input are already on f
			}
			cost += cands[k].xfer
			rest := 0.0
			if k+1 < len(cands) {
				rest = cands[k+1].delay
			}
			avoided := full - impact(rest)
			if net := avoided - p.Threshold*cost; avoided >= p.MinGainS && avoided > 0 && net > bestNet+1e-9 {
				bestK, bestNet = k+1, net
			}
		}
		for i, k := range cands {
			d := copyDecision{Object: k.obj, Consumer: c, Planned: x, Fallback: f,
				Recompute: k.recompute, DelayS: round3(k.delay), SlackS: round3(slack),
				Source: k.src}
			if !math.IsInf(k.xfer, 1) {
				d.CopyS = round3(k.xfer)
			}
			switch {
			case k.alreadyThere:
				d.Reason = "the fallback already holds a planned copy"
			case userGoverned[k.obj]:
				d.Reason = "a user realization entry governs this object"
			case i >= bestK && impact(k.delay) < p.MinGainS:
				d.Reason = fmt.Sprintf("avoids less than %.1fs of delay", p.MinGainS)
			case i >= bestK:
				d.Reason = fmt.Sprintf("avoided delay does not pay for %.1fx the copy time", p.Threshold)
			case p.MaxCopyBytes > 0 && !copies[k.obj][f] &&
				budget+m.objectBytes(k.obj) > p.MaxCopyBytes:
				d.Reason = "copy budget exhausted"
			default:
				d.Copy = true
				d.Reason = "copy avoids more delay than it costs"
				if k.delay >= unrecoverable {
					d.Reason = "without a copy the input cannot be recovered (its producer cannot run again)"
				}
				if copies[k.obj] == nil {
					copies[k.obj] = map[string]bool{}
				}
				if !copies[k.obj][f] {
					copies[k.obj][f] = true
					budget += m.objectBytes(k.obj)
					received[f] += m.objectBytes(k.obj)
				}
			}
			decisions = append(decisions, d)
		}
	}

	var entries []realizationEntry
	for _, o := range m.objects() {
		if nodes := sortedKeys(copies[o]); len(nodes) > 0 {
			entries = append(entries, realizationEntry{Object: o, Copies: nodes, Origin: originPlan})
		}
	}
	return entries, decisions
}

// spreadFallback picks the consumer's fallback: among nodes that would let
// it finish within fallbackTieS (or 5% of the time to its finish) of the
// earliest, the one that has been given the fewest copy bytes so far. The
// earliest node alone drew every copy of a run to the one node with the
// best links, where incoming copies contended for its disk with its own
// tasks (iot: 650 MB onto one node in E16).
func spreadFallback(ranked []nodeEFT, at float64, received map[string]int64) string {
	if len(ranked) == 0 || math.IsInf(ranked[0].eft, 1) {
		return ""
	}
	tie := math.Max(fallbackTieS, 0.05*(ranked[0].eft-at))
	best := ranked[0].node
	for _, r := range ranked[1:] {
		if r.eft > ranked[0].eft+tie {
			break
		}
		if received[r.node] < received[best] {
			best = r.node
		}
	}
	return best
}

const fallbackTieS = 1.0

func round3(x float64) float64 {
	if math.IsInf(x, 0) || math.IsNaN(x) || x >= unrecoverable {
		return -1
	}
	return math.Round(x*1000) / 1000
}

// staticCopies is a plan-blind replication rule, as in Arvados's replication
// level: every consumed object gets Replicas copies in all, the extra ones on
// the first nodes of its rendezvous-hash order other than its producer's
// node. It knows neither where consumers run nor what links cost.
func staticCopies(m *planModel, p realizationPolicy, existing []realizationEntry) ([]realizationEntry, []copyDecision) {
	userGoverned := map[string]bool{}
	for _, e := range existing {
		if e.Origin == "" {
			userGoverned[e.Object] = true
		}
	}
	nodes := sortedKeys(func() map[string]bool {
		set := map[string]bool{}
		for n := range m.nodes {
			set[n] = true
		}
		return set
	}())
	var entries []realizationEntry
	var decisions []copyDecision
	for _, o := range m.objects() {
		d := copyDecision{Object: o, Planned: m.assign[producerOf(o)]}
		if userGoverned[o] {
			d.Reason = "a user realization entry governs this object"
			decisions = append(decisions, d)
			continue
		}
		ranked := append([]string{}, nodes...)
		sort.SliceStable(ranked, func(i, j int) bool {
			return rendezvous(o, ranked[i]) > rendezvous(o, ranked[j])
		})
		var extra []string
		for _, n := range ranked {
			if len(extra) >= p.Replicas-1 {
				break
			}
			if n != m.assign[producerOf(o)] {
				extra = append(extra, n)
			}
		}
		if len(extra) == 0 {
			continue
		}
		sort.Strings(extra)
		entries = append(entries, realizationEntry{Object: o, Copies: extra, Origin: originStatic})
		d.Copy, d.Fallback = true, strings.Join(extra, ",")
		d.Reason = fmt.Sprintf("static rule: %d replicas", p.Replicas)
		decisions = append(decisions, d)
	}
	return entries, decisions
}

// rendezvous is the highest-random-weight score of a node for an object.
func rendezvous(obj, node string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(obj))
	h.Write([]byte{0})
	h.Write([]byte(node))
	return h.Sum64()
}

// applyPlanPolicy computes the run's policy copies (planAware or static)
// and writes them into spec.realization, replacing the policy's earlier
// entries and keeping the user's, with the decisions in
// status.realizationPolicy. A re-application after a repair (ctx set)
// keeps the copies placed before, on nodes that are not lost.
func applyPlanPolicy(dynClient dynamic.Interface, obj *unstructured.Unstructured, m *planModel, ctx policyContext) {
	p := parseRealizationPolicy(obj)
	namespace, name := obj.GetNamespace(), obj.GetName()
	if namespace == "" {
		namespace = "default"
	}
	existing := parseRealization(obj)
	var entries []realizationEntry
	var decisions []copyDecision
	origin := originPlan
	switch p.Mode {
	case policyPlanAware:
		entries, decisions = planAwareCopies(m, p, existing, ctx)
	case policyStatic:
		origin = originStatic
		entries, decisions = staticCopies(m, p, existing)
	default:
		return
	}
	if ctx.skip != nil {
		entries = keepEarlierCopies(existing, entries, origin)
	}

	var bytes int64
	for _, e := range entries {
		bytes += m.objectBytes(e.Object) * int64(len(e.Copies))
	}
	merged := mergeRealization(existing, entries, origin)
	patchRealization(dynClient, namespace, name, merged)

	if decisions == nil {
		decisions = []copyDecision{}
	}
	record := map[string]any{
		"mode": p.Mode, "threshold": p.Threshold, "replicas": p.Replicas, "revise": p.Revise,
		"copies": countCopies(entries), "copyBytes": bytes, "decisions": decisions,
	}
	if ctx.skip != nil {
		// A re-application after a repair is recorded beside the plan-time
		// decisions, not over them: both are needed to read the run.
		record = map[string]any{"reapplication": map[string]any{
			"copies": countCopies(entries), "copyBytes": bytes, "decisions": decisions,
		}}
	}
	st, _ := json.Marshal(map[string]any{"status": map[string]any{"realizationPolicy": record}})
	_, _ = dynClient.Resource(odagGVR).Namespace(namespace).Patch(context.Background(),
		name, types.MergePatchType, st, metav1.PatchOptions{}, "status")
	log.Printf("[policy] %s/%s: %s placed %d extra cop(ies) of %d object(s), %d bytes (reapplied %v)",
		namespace, name, p.Mode, countCopies(entries), len(entries), bytes, ctx.skip != nil)
}

// keepEarlierCopies unions the policy's earlier copies (on nodes not lost)
// into its new decision, so a re-application never withdraws protection
// already in place.
func keepEarlierCopies(existing, entries []realizationEntry, origin string) []realizationEntry {
	idx := map[string]int{}
	for i, e := range entries {
		idx[e.Object] = i
	}
	for _, e := range existing {
		if e.Origin != origin {
			continue
		}
		var keep []string
		for _, n := range e.Copies {
			if !nodeLost(n) {
				keep = append(keep, n)
			}
		}
		if len(keep) == 0 {
			continue
		}
		i, ok := idx[e.Object]
		if !ok {
			idx[e.Object] = len(entries)
			entries = append(entries, realizationEntry{Object: e.Object, Copies: keep, Origin: origin})
			continue
		}
		have := map[string]bool{}
		for _, n := range entries[i].Copies {
			have[n] = true
		}
		for _, n := range keep {
			if !have[n] {
				entries[i].Copies = append(entries[i].Copies, n)
			}
		}
		sort.Strings(entries[i].Copies)
	}
	return entries
}

func countCopies(entries []realizationEntry) int {
	n := 0
	for _, e := range entries {
		n += len(e.Copies)
	}
	return n
}

// mergeRealization replaces every entry of the given origin with `ours`,
// leaving other origins' entries in place. An object keeps one entry: ours
// are dropped where another origin already governs the object, except for
// recovery, whose copies are unioned into the existing entry (a loss must
// be repaired whoever wrote the entry).
func mergeRealization(existing, ours []realizationEntry, origin string) []realizationEntry {
	var out []realizationEntry
	idx := map[string]int{}
	for _, e := range existing {
		if e.Origin == origin && origin != originRecovery {
			continue // the policy's own earlier entries are replaced
		}
		idx[e.Object] = len(out)
		out = append(out, e)
	}
	for _, e := range ours {
		i, ok := idx[e.Object]
		switch {
		case !ok:
			idx[e.Object] = len(out)
			out = append(out, e)
		case origin == originRecovery:
			have := map[string]bool{}
			for _, n := range out[i].Copies {
				have[n] = true
			}
			evict := map[string]bool{}
			for _, n := range out[i].Evict {
				evict[n] = true
			}
			for _, n := range e.Copies {
				if !have[n] {
					out[i].Copies = append(out[i].Copies, n)
					delete(evict, n) // a node cannot be both a copy and evicted
				}
			}
			out[i].Evict = sortedKeys(evict)
		}
	}
	return out
}

// patchRealization writes spec.realization (a merge patch replaces the list).
func patchRealization(dynClient dynamic.Interface, namespace, name string, entries []realizationEntry) {
	items := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		it := map[string]any{"object": e.Object}
		if len(e.Copies) > 0 {
			it["copies"] = e.Copies
		}
		if len(e.Evict) > 0 {
			it["evict"] = e.Evict
		}
		if e.ServingCopy != "" {
			it["servingCopy"] = e.ServingCopy
		}
		if e.Durable {
			it["durable"] = true
		}
		if e.Origin != "" {
			it["origin"] = e.Origin
		}
		items = append(items, it)
	}
	data, _ := json.Marshal(map[string]any{"spec": map[string]any{"realization": items}})
	if _, err := dynClient.Resource(odagGVR).Namespace(namespace).Patch(context.Background(),
		name, types.MergePatchType, data, metav1.PatchOptions{}); err != nil {
		log.Printf("[policy] %s/%s: patch spec.realization: %v", namespace, name, err)
	}
}
