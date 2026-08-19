// data-agent: per-node ODAG intermediate-data service. Implements the data
// plane that decouples task completion from data availability.
//
// State model (locked design, see project_atc2026_data_plane_state_model):
//   - Task state: <rel>/.wl-task-state ∈ {Pending, Running, ComputeDone, Failed}
//   - Local data availability: presence of <rel>/.wl-ready marker
//   - Per-successor transfer state: <rel>/transfers/<consumer>.state (added later)
//
// Data endpoints (body = raw bytes):
//   PUT /<odag>/<task>/output        — upstream task pushes output to this node;
//                                      receiver verifies X-Wayline-Content-SHA256,
//                                      then writes .wl-ready on success
//   GET /<odag>/<task>/output        — read output (requires .wl-ready unless ?unsafe=1)
//
// Task-state endpoints (body = plain text):
//   PUT /state/<odag>/<task>         — SDK/controller writes one of
//                                      {Pending, Running, ComputeDone, Failed}
//   GET /state/<odag>/<task>         — controller queries task state
//
// Data-readiness endpoints (presence-only marker, no body):
//   GET  /ready/<odag>/<task>        — body "true" if marker present, else "false"
//   DEL  /ready/<odag>/<task>        — clear the marker (used on reset)
// (PUT is intentionally not exposed: readiness is only ever set as a side
// effect of a successful atomic install via PUT /<odag>/<task>/output.)
//
// Sending endpoints (mid-transfer indicator, unchanged):
//   PUT /sending/<odag>/<task>       — body "true"/"false"
//   GET /sending/<odag>/<task>
//
// Push endpoint:
//   POST /push/<odag>/<task>         — agent pushes local output to remote
//                                      successor nodes in the background.
//                                      Body: JSON {"successors":[{"name":"x","host":"1.2.3.4","node":"anrg-5"},...]}
//
// Flow endpoints:
//   GET /flows/<odag>                — per-push flow records on this node
//
// GET /healthz  — liveness probe
package main

import (
	"bytes"
	"compress/gzip"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Wire-level headers carried on PUT /<odag>/<task>/output. Both are computed
// over the UNCOMPRESSED installed payload — gzip on the wire (fix #5) does
// not affect the digest. Receivers verify the digest after decoding the
// optional Content-Encoding, so idempotency stays canonical.
const (
	headerContentSHA256       = "X-Wayline-Content-SHA256"
	headerUncompressedLength  = "X-Wayline-Uncompressed-Length"
	// headerSourceNode is set by a pushing agent so the receiver can tell an
	// agent-to-agent install apart from a local SDK install when it records
	// the data-availability timestamp. Absent ⇒ local install.
	headerSourceNode = "X-Wayline-Source-Node"
)

const (
	pushRetries    = 5
	pushRetryDelay = 500 * time.Millisecond
	dataAgentPort  = 8082

	// pushTimeoutFor parameters. A fixed timeout of 120s used to live here,
	// but for a 500 MB push on a 50 Mbps S-link the naive wire time is
	// already ~80s — with TCP slow-start, retransmits, and HTTP overhead a
	// fixed 120s ran too close to the edge. We now scale per-payload:
	//
	//   timeout = pushTimeoutBase + pushTimeoutSafety + size / pushMinThroughput
	//
	// pushMinThroughput is set conservatively to 5 MB/s (~40 Mbps) — well
	// below the slowest link in the matrix (50 Mbps minus protocol overhead).
	pushTimeoutBase     = 60 * time.Second
	pushTimeoutSafety   = 30 * time.Second
	pushMinThroughputBs = 5 * 1024 * 1024 // 5 MB/s
)

// pushTimeoutFor returns the HTTP client timeout for one PUT of `size`
// bytes. Replaces the old fixed 120s constant (post-experiment-todos #3).
func pushTimeoutFor(size int64) time.Duration {
	if size <= 0 {
		return pushTimeoutBase + pushTimeoutSafety
	}
	expected := time.Duration(size/pushMinThroughputBs) * time.Second
	return pushTimeoutBase + pushTimeoutSafety + expected
}

var dataDir string

// maxConcurrentPushes caps how many remote successor PUTs run in parallel
// node-wide. Default 4. 0 = unbounded. Used by both the live push handler
// and the recovery path (fix H) so a restart with many queued transfers
// doesn't blast the network.
var maxConcurrentPushes int = 4

// pushCompress selects an optional compressor applied to outgoing PUT
// bodies. The X-Wayline-Content-SHA256 digest is always computed over the
// uncompressed bytes — compression is purely wire-level, transparent to
// the idempotent dedupe path. Accepted values: "none", "gzip".
var pushCompress = "none"

// pushSem is the node-wide push concurrency semaphore. nil iff
// maxConcurrentPushes is 0 (unbounded). Initialized in main().
var pushSem chan struct{}

// startTime is recorded at boot for the uptime metric.
var startTime = time.Now()

// Atomic counters surfaced via GET /metrics. Updated from the request
// handlers and the push goroutine. JSON-typed names match the response
// shape so they're easy to grep alongside the metrics endpoint.
// localNodeName is this agent's Kubernetes node name, set once in main().
// Used to stamp outgoing pushes so the receiving agent can attribute an
// install to its sending node.
var localNodeName string

var (
	metricPutTotal       atomic.Int64 // every PUT received (success + fail)
	metricPutOK          atomic.Int64 // PUT installed successfully
	metricPutMismatch    atomic.Int64 // X-Wayline-Content-SHA256 mismatch (400)
	metricPutConflict    atomic.Int64 // 409 — existing md5 differs from claimed
	metricPutIdempotent  atomic.Int64 // 200 fast-path (same md5 already installed)
	metricBytesIn        atomic.Int64 // bytes accepted via PUT
	metricPushAttempts   atomic.Int64 // pushToNode calls (live + recovery)
	metricPushSuccess    atomic.Int64 // pushToNode returned nil
	metricPushFailed     atomic.Int64 // pushToNode returned error after retries
	metricBytesOut       atomic.Int64 // bytes sent via successful pushes
)

// acquirePushSlot blocks until a push slot is available. No-op if unbounded.
func acquirePushSlot() {
	if pushSem != nil {
		pushSem <- struct{}{}
	}
}

// releasePushSlot returns a slot to the semaphore. No-op if unbounded.
func releasePushSlot() {
	if pushSem != nil {
		<-pushSem
	}
}

// nameRe constrains every URL-derived path component (ODAG name, task name,
// consumer name) before it is concatenated onto dataDir. Anything outside
// this character class is rejected with 400 to close path-traversal risk and
// to keep the on-disk layout grep-friendly.
var nameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$`)

// hostRe accepts IPv4 dotted-quad and DNS-shaped labels. IPv6 with colons
// would need a wider charset; cluster traffic is IPv4 in this deployment so
// the simple form covers every Host the controller actually emits.
var hostRe = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,253}$`)

// validHostOrIP returns true iff s is a plausible IPv4 address or DNS name.
// Used to gate /push successor entries: the host string becomes part of an
// outbound URL, so it must not contain whitespace, NULs, or path characters.
func validHostOrIP(s string) bool {
	if s == "" {
		return false
	}
	return hostRe.MatchString(s)
}

// validName returns nil iff s is a safe identifier for use as an on-disk
// path component.
func validName(s string) error {
	if s == "" {
		return fmt.Errorf("empty name")
	}
	if !nameRe.MatchString(s) {
		return fmt.Errorf("invalid name %q (must match %s)", s, nameRe.String())
	}
	return nil
}

// parsePathComponents strips the URL prefix and splits into exactly `want`
// components, validating each. Returns the components or a (status, message)
// error suitable for http.Error.
func parsePathComponents(urlPath, prefix string, want int) ([]string, int, string) {
	rest := strings.TrimPrefix(urlPath, prefix)
	rest = strings.TrimSuffix(rest, "/")
	if rest == "" {
		return nil, http.StatusBadRequest, "missing path components"
	}
	parts := strings.Split(rest, "/")
	if len(parts) != want {
		return nil, http.StatusBadRequest, fmt.Sprintf("expected %d path components, got %d", want, len(parts))
	}
	for _, p := range parts {
		if err := validName(p); err != nil {
			return nil, http.StatusBadRequest, err.Error()
		}
	}
	return parts, http.StatusOK, ""
}

// Per-task control files. Each path is keyed by <odag>/<task>.
//
//   taskStateFile  →  .wl-task-state  (text: one of {Pending, Running,
//                                       ComputeDone, Failed}; task lifecycle only,
//                                       independent of data availability)
//   readyFile      →  .wl-ready       (presence-only marker: the producer's
//                                       output is atomically installed on this
//                                       node and safe for a consumer to read)
//   sendingFile    →  .wl-sending     (true/false: data-agent is mid-transfer)
//   bytesFile      →  .wl-bytes       (size of the installed output)
func taskStateFile(rel string) string  { return filepath.Join(dataDir, filepath.Clean(rel), ".wl-task-state") }
func readyFile(rel string) string      { return filepath.Join(dataDir, filepath.Clean(rel), ".wl-ready") }
func sendingFile(rel string) string    { return filepath.Join(dataDir, filepath.Clean(rel), ".wl-sending") }
func bytesFile(rel string) string      { return filepath.Join(dataDir, filepath.Clean(rel), ".wl-bytes") }
func digestSidecarFile(rel string) string { return filepath.Join(dataDir, filepath.Clean(rel), ".wl-sha256") }
func flowsFile(odag string) string     { return filepath.Join(dataDir, filepath.Clean(odag), ".wl-flows.jsonl") }

// Instrumentation sidecars. Both are per-(odag, task) and are removed with
// the rest of the run's data by DELETE /data/<odag>.
//
//	installedFile  →  .wl-installed.json  (when this node's copy of the
//	                                        output became readable, and from
//	                                        where — the data-availability
//	                                        timestamp a scheduler's model
//	                                        implicitly assumes)
//	timingsFile    →  .wl-timings.json    (task-internal phase boundaries
//	                                        reported by the SDK: compute
//	                                        start/end, handoff duration)
func installedFile(rel string) string { return filepath.Join(dataDir, filepath.Clean(rel), ".wl-installed.json") }
func timingsFile(rel string) string   { return filepath.Join(dataDir, filepath.Clean(rel), ".wl-timings.json") }

// installRecord is written once per successful install, at the moment the
// payload becomes readable on this node (i.e. alongside the .wl-ready
// marker). Source is "local" for an SDK install on the producing node and
// "remote" for an agent-to-agent push, in which case FromNode names the
// sender.
type installRecord struct {
	UnixTime float64 `json:"unixTime"`
	Bytes    int64   `json:"bytes"`
	Source   string  `json:"source"`
	FromNode string  `json:"fromNode,omitempty"`
}

// recordInstalled durably writes the install record. Best-effort: a failure
// here must never fail an install that already succeeded, so errors are
// logged and swallowed.
func recordInstalled(rel string, rec installRecord) {
	b, err := json.Marshal(rec)
	if err != nil {
		log.Printf("[data-agent] recordInstalled marshal %s: %v", rel, err)
		return
	}
	if err := writeFile(installedFile(rel), string(b)); err != nil {
		log.Printf("[data-agent] recordInstalled write %s: %v", rel, err)
	}
}

// readInstalledDigest returns the hex SHA-256 cached for an installed
// output, or "" if no sidecar is present. Used by the idempotent fast-path
// on receive.
func readInstalledDigest(rel string) string {
	b, err := os.ReadFile(digestSidecarFile(rel))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// writeInstalledDigest persists the SHA-256 of the just-installed output.
// Durably written (temp + fsync + rename + fsync parent) so the idempotent
// fast-path stays correct after a crash.
func writeInstalledDigest(rel, hexDigest string) {
	if err := writeTextAtomic(digestSidecarFile(rel), hexDigest); err != nil {
		log.Printf("[data-agent] writeInstalledDigest %s: %v", rel, err)
	}
}

// validTaskStates enforces the locked vocabulary. Anything else is rejected
// at the state endpoint with 400.
var validTaskStates = map[string]bool{
	"Pending":     true,
	"Running":     true,
	"ComputeDone": true,
	"Failed":      true,
}

// validTransferStates is the vocabulary for per-(producer, consumer) transfer
// state files written by the PUSH handler on the producer's node.
var validTransferStates = map[string]bool{
	"Pending":      true, // queued, not yet started
	"Transferring": true, // pushToNode is running
	"ReadyRemote":  true, // push succeeded; consumer's node has the data
	"Failed":       true, // push failed after retries
}

// Per-(producer, consumer) transfer paths. All live under the producer's
// hostPath directory, so the producer's node is the authority for transfer
// state and the recovery queue (fix H).
//
//   <rel>/transfers/<consumer>.state  → transfer state (text, one of validTransferStates)
//   <rel>/transfers/<consumer>.json   → queue entry: host, node, retries, lastError
func transfersDir(rel string) string {
	return filepath.Join(dataDir, filepath.Clean(rel), "transfers")
}
func transferStateFile(rel, consumer string) string {
	return filepath.Join(transfersDir(rel), filepath.Clean(consumer)+".state")
}
func transferEntryFile(rel, consumer string) string {
	return filepath.Join(transfersDir(rel), filepath.Clean(consumer)+".json")
}

// transferEntry is the JSON sidecar for a (producer, consumer) transfer. It
// holds enough information for fix H's recovery path to resume a pending or
// in-flight transfer after the agent restarts.
type transferEntry struct {
	Consumer  string  `json:"consumer"`
	Host      string  `json:"host"`
	Node      string  `json:"node"`
	Retries   int     `json:"retries"`
	LastError string  `json:"lastError,omitempty"`
	UpdatedAt float64 `json:"updatedAt"` // seconds since epoch
}

// setTransferState writes the per-consumer transfer state file atomically and
// fsynced. Invalid values are rejected so the on-disk vocabulary stays clean.
// Durability matters here: the recovery path on agent restart relies on this
// file being correct.
func setTransferState(rel, consumer, state string) {
	if !validTransferStates[state] {
		log.Printf("[data-agent] setTransferState %s/%s: rejected invalid value %q", rel, consumer, state)
		return
	}
	if err := writeTextAtomic(transferStateFile(rel, consumer), state); err != nil {
		log.Printf("[data-agent] setTransferState %s/%s=%s: %v", rel, consumer, state, err)
	}
}

// writeTransferEntry durably persists the JSON sidecar for one consumer.
// Overwrites any prior entry. The recovery path (fix H) reads this back on
// restart, so atomicity is part of the data-plane contract.
func writeTransferEntry(rel, consumer string, e transferEntry) {
	e.UpdatedAt = float64(time.Now().UnixNano()) / 1e9
	if err := writeJSONAtomic(transferEntryFile(rel, consumer), e); err != nil {
		log.Printf("[data-agent] writeTransferEntry %s/%s: %v", rel, consumer, err)
	}
}

// readTransferEntry loads the JSON sidecar for one (producer, consumer)
// transfer, or returns nil if absent / malformed.
func readTransferEntry(rel, consumer string) *transferEntry {
	b, err := os.ReadFile(transferEntryFile(rel, consumer))
	if err != nil {
		return nil
	}
	var e transferEntry
	if err := json.Unmarshal(b, &e); err != nil {
		return nil
	}
	return &e
}

// listTransferStates returns a map of consumer name -> state value by
// scanning <rel>/transfers/*.state.
func listTransferStates(rel string) map[string]string {
	dir := transfersDir(rel)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return map[string]string{}
	}
	out := make(map[string]string)
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".state") {
			continue
		}
		consumer := strings.TrimSuffix(name, ".state")
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		out[consumer] = strings.TrimSpace(string(b))
	}
	return out
}

// flowRecord is a single completed (or failed) push from this node to one
// downstream successor. Both timestamps are captured on the sender, so clock
// drift across nodes never contaminates the duration.
type flowRecord struct {
	FromTask  string  `json:"fromTask"`
	ToTask    string  `json:"toTask"`
	SrcNode   string  `json:"srcNode"`
	DstNode   string  `json:"dstNode"`
	DataSize  int64   `json:"dataSize"`
	StartUnix float64 `json:"startUnix"` // seconds since epoch (float, sub-millisecond precision)
	EndUnix   float64 `json:"endUnix"`
	Ok        bool    `json:"ok"`
}

var flowsMu sync.Mutex

func appendFlow(odag string, rec flowRecord) {
	path := flowsFile(odag)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		log.Printf("[data-agent] appendFlow mkdir %s: %v", path, err)
		return
	}
	line, err := json.Marshal(rec)
	if err != nil {
		log.Printf("[data-agent] appendFlow marshal: %v", err)
		return
	}
	flowsMu.Lock()
	defer flowsMu.Unlock()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[data-agent] appendFlow open %s: %v", path, err)
		return
	}
	defer f.Close()
	_, _ = f.Write(line)
	_, _ = f.Write([]byte("\n"))
}

func readFlows(odag string) []flowRecord {
	path := flowsFile(odag)
	flowsMu.Lock()
	defer flowsMu.Unlock()
	data, err := os.ReadFile(path)
	if err != nil {
		return []flowRecord{}
	}
	out := make([]flowRecord, 0)
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var rec flowRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			continue
		}
		out = append(out, rec)
	}
	return out
}

// dirSize walks a directory tree and returns total bytes.
func dirSize(path string) int64 {
	var total int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}

// runInfo is returned by GET /runs — one entry per ODAG directory on this node.
type runInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"` // total bytes
}

func writeFile(path, value string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(value), 0644)
}

// writeTextAtomic writes `value` to `path` durably: temp file in the same
// directory → fsync the file → rename → fsync the parent dir. After this
// returns nil, the new value is on stable storage and the rename is in the
// parent's dirent. Used for transfer-queue files where durability is part of
// the data-plane contract.
func writeTextAtomic(path, value string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tok := make([]byte, 8)
	if _, err := cryptorand.Read(tok); err != nil {
		return err
	}
	tmp := path + ".tmp." + hex.EncodeToString(tok)
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(value); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	fsyncDir(dir)
	return nil
}

// writeJSONAtomic marshals obj as JSON and writes it durably via writeTextAtomic.
func writeJSONAtomic(path string, obj interface{}) error {
	b, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return writeTextAtomic(path, string(b))
}

// setTaskState writes a value from the locked task-state vocabulary to
// <rel>/.wl-task-state. Invalid values are rejected with a warning so callers
// can't quietly poison the file. Write is atomic+fsynced so the controller
// never observes a half-written state across a crash.
func setTaskState(rel, state string) {
	if !validTaskStates[state] {
		log.Printf("[data-agent] setTaskState %s: rejected invalid value %q", rel, state)
		return
	}
	if err := writeTextAtomic(taskStateFile(rel), state); err != nil {
		log.Printf("[data-agent] setTaskState %s=%s: %v", rel, state, err)
	}
}

// setReady creates the .wl-ready marker on this node, durably. The marker
// means the producer's output is locally installed and safe for a consumer
// to read. Idempotent.
//
// The marker is a zero-byte file. To survive a crash we fsync the parent
// directory after creation so the dirent change is on disk before any
// consumer observes it.
func setReady(rel string) {
	p := readyFile(rel)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		log.Printf("[data-agent] setReady mkdir %s: %v", p, err)
		return
	}
	f, err := os.Create(p)
	if err != nil {
		log.Printf("[data-agent] setReady create %s: %v", p, err)
		return
	}
	_ = f.Close()
	fsyncDir(filepath.Dir(p))
}

// clearReady removes the .wl-ready marker. Used on controller-driven reset.
func clearReady(rel string) {
	if err := os.Remove(readyFile(rel)); err != nil && !os.IsNotExist(err) {
		log.Printf("[data-agent] clearReady %s: %v", rel, err)
	}
}

// isReady reports whether <rel>/.wl-ready exists.
func isReady(rel string) bool {
	_, err := os.Stat(readyFile(rel))
	return err == nil
}

func setSending(rel string, sending bool) {
	val := "false"
	if sending {
		val = "true"
	}
	if err := writeFile(sendingFile(rel), val); err != nil {
		log.Printf("[data-agent] setSending %s=%s: %v", rel, val, err)
	}
}

// recoverTransfers scans <dataDir>/<odag>/<task>/transfers/*.state on
// startup and resumes any transfer left in Pending or Transferring. The
// agent may have crashed or been restarted mid-push; the idempotent receiver
// (fix D) absorbs duplicate pushes if the remote already installed the
// payload, and a real failure surfaces normally via setTransferState.
//
// Each resumed push acquires a slot from the node-wide push semaphore, so
// recovery cannot fan out beyond maxConcurrentPushes.
func recoverTransfers(nodeName string) {
	odagEntries, err := os.ReadDir(dataDir)
	if err != nil {
		log.Printf("[data-agent] recover: ReadDir %s: %v", dataDir, err)
		return
	}
	resumed := 0
	for _, ode := range odagEntries {
		if !ode.IsDir() {
			continue
		}
		odag := ode.Name()
		taskEntries, err := os.ReadDir(filepath.Join(dataDir, odag))
		if err != nil {
			continue
		}
		for _, te := range taskEntries {
			if !te.IsDir() {
				continue
			}
			task := te.Name()
			transfersPath := filepath.Join(dataDir, odag, task, "transfers")
			stateEntries, err := os.ReadDir(transfersPath)
			if err != nil {
				continue
			}
			for _, se := range stateEntries {
				if !strings.HasSuffix(se.Name(), ".state") {
					continue
				}
				consumer := strings.TrimSuffix(se.Name(), ".state")
				rel := odag + "/" + task
				stateBytes, err := os.ReadFile(filepath.Join(transfersPath, se.Name()))
				if err != nil {
					continue
				}
				state := strings.TrimSpace(string(stateBytes))
				if state != "Pending" && state != "Transferring" {
					continue
				}
				entry := readTransferEntry(rel, consumer)
				if entry == nil {
					log.Printf("[data-agent] recover: %s/%s -> %s: missing entry JSON; marking Failed", odag, task, consumer)
					setTransferState(rel, consumer, "Failed")
					continue
				}
				localFile := filepath.Join(dataDir, odag, task, "output")
				if _, err := os.Stat(localFile); err != nil {
					log.Printf("[data-agent] recover: %s/%s -> %s: no local output; marking Failed", odag, task, consumer)
					setTransferState(rel, consumer, "Failed")
					continue
				}
				log.Printf("[data-agent/%s] recover: resuming %s/%s -> %s (was %s, host=%s)",
					nodeName, odag, task, consumer, state, entry.Host)
				resumed++
				go resumeTransfer(nodeName, odag, task, rel, consumer, *entry, localFile)
			}
		}
	}
	if resumed > 0 {
		log.Printf("[data-agent/%s] recover: %d transfer(s) resumed", nodeName, resumed)
	} else {
		log.Printf("[data-agent/%s] recover: no Pending/Transferring entries found", nodeName)
	}
}

// resumeTransfer runs the same code path as a live push but for a recovered
// (producer, consumer) entry. Acquires from the node-wide push semaphore.
func resumeTransfer(nodeName, odag, task, rel, consumer string, entry transferEntry, localFile string) {
	acquirePushSlot()
	defer releasePushSlot()

	contentDigest, size, err := sha256OfFile(localFile)
	if err != nil {
		log.Printf("[data-agent/%s] recover: %s/%s -> %s: hash local file: %v", nodeName, odag, task, consumer, err)
		writeTransferEntry(rel, consumer, transferEntry{
			Consumer: consumer, Host: entry.Host, Node: entry.Node,
			Retries: entry.Retries, LastError: "recover: hash local file: " + err.Error(),
		})
		setTransferState(rel, consumer, "Failed")
		return
	}
	setTransferState(rel, consumer, "Transferring")
	start := time.Now()
	err = pushToNode(odag, task, entry.Host, localFile, contentDigest, size)
	end := time.Now()
	if err != nil {
		log.Printf("[data-agent/%s] recover: %s/%s -> %s FAILED: %v", nodeName, odag, task, consumer, err)
		writeTransferEntry(rel, consumer, transferEntry{
			Consumer: consumer, Host: entry.Host, Node: entry.Node,
			Retries: entry.Retries + pushRetries, LastError: err.Error(),
		})
		setTransferState(rel, consumer, "Failed")
	} else {
		log.Printf("[data-agent/%s] recover: %s/%s -> %s OK (%.3fs)", nodeName, odag, task, consumer, end.Sub(start).Seconds())
		writeTransferEntry(rel, consumer, transferEntry{
			Consumer: consumer, Host: entry.Host, Node: entry.Node,
		})
		setTransferState(rel, consumer, "ReadyRemote")
	}
	appendFlow(odag, flowRecord{
		FromTask:  task,
		ToTask:    consumer,
		SrcNode:   nodeName,
		DstNode:   entry.Node,
		DataSize:  size,
		StartUnix: float64(start.UnixNano()) / 1e9,
		EndUnix:   float64(end.UnixNano()) / 1e9,
		Ok:        err == nil,
	})
}

// errChecksumMismatch is returned by installAtomically when the bytes
// written to disk do not match the X-Wayline-Content-SHA256 the sender
// claimed. The temp file is removed before the error returns so the final
// path is never poisoned.
var errChecksumMismatch = errors.New("X-Wayline-Content-SHA256 mismatch")

// fsyncDir opens a directory and calls Sync on the descriptor, making any
// dirent changes (renames) durable across crash. Best-effort: errors are
// logged but not propagated.
func fsyncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		log.Printf("[data-agent] fsyncDir open %s: %v", dir, err)
		return
	}
	if err := d.Sync(); err != nil {
		log.Printf("[data-agent] fsyncDir sync %s: %v", dir, err)
	}
	_ = d.Close()
}

// installAtomically streams body into a unique temp file in the same
// directory as destPath, fsyncs it, then renames the temp into place only if
// the computed SHA-256 matches expectedDigest. The parent directory is
// fsynced so the rename is durable across crash.
//
// On digest mismatch: the temp file is removed, the final destPath is left
// untouched, and errChecksumMismatch is returned.
//
// On any I/O error before rename: the temp file is removed and the error is
// returned. The final path is never partially overwritten — a consumer
// either sees the previous version or the new complete one.
//
// expectedDigest of "" disables the check (used by GET-fallback paths that
// don't carry an X-Wayline-Content-SHA256 header).
func installAtomically(destPath string, body io.Reader, expectedDigest string) (digest string, n int64, err error) {
	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", 0, err
	}
	tok := make([]byte, 8)
	if _, err := cryptorand.Read(tok); err != nil {
		return "", 0, err
	}
	tmpPath := destPath + ".tmp." + hex.EncodeToString(tok)

	f, err := os.Create(tmpPath)
	if err != nil {
		return "", 0, err
	}
	h := sha256.New()
	n, err = io.Copy(io.MultiWriter(f, h), body)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return "", 0, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return "", 0, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", 0, err
	}

	digest = hex.EncodeToString(h.Sum(nil))
	if expectedDigest != "" && digest != expectedDigest {
		_ = os.Remove(tmpPath)
		return digest, n, fmt.Errorf("%w: claimed=%s computed=%s", errChecksumMismatch, expectedDigest, digest)
	}

	if err := os.Rename(tmpPath, destPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", 0, err
	}
	fsyncDir(dir)
	return digest, n, nil
}

// sha256OfFile streams the file once through SHA-256, returning the
// hex-encoded digest and the file size in bytes.
func sha256OfFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// pushToNode streams localFile to the remote agent via HTTP PUT. The body is
// read directly from disk (no whole-file buffer in RAM), and the headers
// X-Wayline-Content-SHA256 + X-Wayline-Uncompressed-Length carry the
// integrity digest and the canonical (uncompressed) size so the receiver
// can verify end-to-end after optional gzip decoding. On retry the file is
// reopened.
func pushToNode(odag, task, host, localFile, contentDigest string, size int64) error {
	metricPushAttempts.Add(1)
	url := fmt.Sprintf("http://%s:%d/%s/%s/output", host, dataAgentPort, odag, task)
	client := &http.Client{Timeout: pushTimeoutFor(size)}
	for attempt := 1; attempt <= pushRetries; attempt++ {
		f, err := os.Open(localFile)
		if err != nil {
			log.Printf("[data-agent] push to %s attempt %d/%d: open %s: %v", host, attempt, pushRetries, localFile, err)
			metricPushFailed.Add(1)
			return err
		}
		// Body source: the raw file by default; a streaming gzip pipe when
		// compression is on. With gzip we use chunked transfer (no
		// Content-Length) so the body can stream without pre-buffering.
		var body io.ReadCloser = f
		setLen := true
		switch pushCompress {
		case "gzip":
			pr, pw := io.Pipe()
			go func() {
				gz := gzip.NewWriter(pw)
				_, copyErr := io.Copy(gz, f)
				closeErr := gz.Close()
				_ = f.Close()
				if copyErr != nil {
					_ = pw.CloseWithError(copyErr)
				} else if closeErr != nil {
					_ = pw.CloseWithError(closeErr)
				} else {
					_ = pw.Close()
				}
			}()
			body = pr
			setLen = false
		}
		req, _ := http.NewRequest(http.MethodPut, url, body)
		if setLen {
			req.ContentLength = size
		}
		if pushCompress == "gzip" {
			req.Header.Set("Content-Encoding", "gzip")
		}
		// Digest is over the uncompressed installed payload; the receiver
		// decodes Content-Encoding before re-computing the SHA-256. The
		// uncompressed length is sent separately so the receiver can
		// distinguish "wire body short due to compression" from "truncated".
		req.Header.Set(headerUncompressedLength, fmt.Sprintf("%d", size))
		if contentDigest != "" {
			req.Header.Set(headerContentSHA256, contentDigest)
		}
		req.Header.Set(headerSourceNode, localNodeName)
		resp, err := client.Do(req)
		// File close: for the raw path, close here; for gzip, the goroutine
		// closed it already.
		if pushCompress != "gzip" {
			_ = f.Close()
		}
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			metricPushSuccess.Add(1)
			metricBytesOut.Add(size)
			return nil
		}
		if err != nil {
			log.Printf("[data-agent] push to %s attempt %d/%d: %v", host, attempt, pushRetries, err)
		} else {
			resp.Body.Close()
			log.Printf("[data-agent] push to %s attempt %d/%d: status %d", host, attempt, pushRetries, resp.StatusCode)
		}
		if attempt < pushRetries {
			time.Sleep(pushRetryDelay)
		}
	}
	metricPushFailed.Add(1)
	return fmt.Errorf("push to %s failed after %d attempts", host, pushRetries)
}

func main() {
	port := dataAgentPort
	flag.StringVar(&dataDir, "data-dir", "/data/wl-outputs", "directory to serve")
	flag.IntVar(&port, "port", port, "listen port")
	flag.IntVar(&maxConcurrentPushes, "max-concurrent-pushes", maxConcurrentPushes,
		"max parallel remote PUTs per producer task; 1 = sequential, 0 = unbounded")
	flag.StringVar(&pushCompress, "push-compress", pushCompress,
		"compressor applied to outgoing PUT bodies; one of: none, gzip")
	flag.Parse()
	switch pushCompress {
	case "none", "gzip":
	default:
		log.Fatalf("[data-agent] invalid --push-compress=%q (expected: none, gzip)", pushCompress)
	}
	if maxConcurrentPushes < 0 {
		maxConcurrentPushes = 0
	}
	if maxConcurrentPushes > 0 {
		pushSem = make(chan struct{}, maxConcurrentPushes)
	}
	log.Printf("[data-agent] max-concurrent-pushes=%d (0=unbounded)", maxConcurrentPushes)
	log.Printf("[data-agent] push-compress=%s", pushCompress)

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("[data-agent] failed to create data dir %s: %v", dataDir, err)
	}

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		nodeName = "unknown"
	}
	localNodeName = nodeName

	fs := http.FileServer(http.Dir(dataDir))

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	})

	// GET /metrics — JSON snapshot of resource and traffic counters. Used
	// for paper-time observability (CPU/memory/disk pressure, transfer
	// counts) and for verifying that the data plane is healthy before a
	// sweep. Counters are atomic so the read is consistent.
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		inflight := 0
		if pushSem != nil {
			inflight = len(pushSem)
		}
		// Count run directories under dataDir.
		runCount := 0
		if entries, err := os.ReadDir(dataDir); err == nil {
			for _, e := range entries {
				if e.IsDir() {
					runCount++
				}
			}
		}
		out := map[string]interface{}{
			"node":           nodeName,
			"uptime_seconds": int64(time.Since(startTime).Seconds()),
			"goroutines":     runtime.NumGoroutine(),
			"memory": map[string]int64{
				"alloc_bytes": int64(mem.Alloc),
				"sys_bytes":   int64(mem.Sys),
			},
			"disk": map[string]int64{
				"bytes_used": dirSize(dataDir),
				"run_count":  int64(runCount),
			},
			"transfers": map[string]int64{
				"put_total":             metricPutTotal.Load(),
				"put_ok":                metricPutOK.Load(),
				"put_checksum_mismatch": metricPutMismatch.Load(),
				"put_conflict":          metricPutConflict.Load(),
				"put_idempotent":        metricPutIdempotent.Load(),
				"bytes_in":              metricBytesIn.Load(),
			},
			"push": map[string]int64{
				"attempts":  metricPushAttempts.Load(),
				"success":   metricPushSuccess.Load(),
				"failed":    metricPushFailed.Load(),
				"inflight":  int64(inflight),
				"bytes_out": metricBytesOut.Load(),
			},
			"config": map[string]int64{
				"max_concurrent_pushes":          int64(maxConcurrentPushes),
				"push_retries":                   int64(pushRetries),
				"push_timeout_base_seconds":      int64(pushTimeoutBase / time.Second),
				"push_timeout_safety_seconds":    int64(pushTimeoutSafety / time.Second),
				"push_min_throughput_bytes_sec": int64(pushMinThroughputBs),
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})

	// POST /push/<odag>/<task>
	// Reads local output file and pushes to each remote successor in a goroutine.
	// Responds 200 immediately; the pod can exit right away.
	http.HandleFunc("/push/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		parts, status, msg := parsePathComponents(r.URL.Path, "/push/", 2)
		if status != http.StatusOK {
			http.Error(w, msg, status)
			return
		}
		odag, task := parts[0], parts[1]
		rel := odag + "/" + task

		var body struct {
			Successors []struct {
				Name string `json:"name"`
				Host string `json:"host"`
				Node string `json:"node"`
			} `json:"successors"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		// Validate every successor name before any I/O. Consumer names become
		// filesystem path components for transfers/<consumer>.{state,json};
		// host is used in URLs. Anything malformed is a 400 — we do NOT
		// accept the request partially.
		for _, succ := range body.Successors {
			if err := validName(succ.Name); err != nil {
				http.Error(w, fmt.Sprintf("successor name %q invalid: %v", succ.Name, err), http.StatusBadRequest)
				return
			}
			if succ.Node != "" {
				if err := validName(succ.Node); err != nil {
					http.Error(w, fmt.Sprintf("successor node %q invalid: %v", succ.Node, err), http.StatusBadRequest)
					return
				}
			}
			if !validHostOrIP(succ.Host) {
				http.Error(w, fmt.Sprintf("successor host %q invalid", succ.Host), http.StatusBadRequest)
				return
			}
		}

		// Durable enqueue: write the per-successor queue entry and state file
		// for every successor BEFORE responding 202. After all writes complete
		// (each is temp+fsync+rename+fsync-parent), fsync the transfers
		// directory itself so the dirents are committed. Only then is "the
		// transfer has been durably enqueued by the local agent" a true
		// statement of the response we're about to send.
		if len(body.Successors) > 0 {
			transfersPath := transfersDir(rel)
			for _, succ := range body.Successors {
				writeTransferEntry(rel, succ.Name, transferEntry{
					Consumer: succ.Name, Host: succ.Host, Node: succ.Node, Retries: 0,
				})
				setTransferState(rel, succ.Name, "Pending")
			}
			fsyncDir(transfersPath)
		}

		// Response is 202 Accepted, not 200 OK: the contract is "transfer has
		// been durably enqueued by the local agent" (now actually true — see
		// above), not "downstream data is already available." Callers observe
		// remote-readiness via /transfers/<odag>/<task> per-successor state.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":     "accepted",
			"odag":       odag,
			"task":       task,
			"successors": len(body.Successors),
		})

		if len(body.Successors) == 0 {
			log.Printf("[data-agent/%s] PUSH %s/%s: no remote successors", nodeName, odag, task)
			return
		}

		go func() {
			localFile := filepath.Join(dataDir, odag, task, "output")
			// Stream the file once through SHA-256 to get the integrity digest
			// and the exact size. The file itself stays on disk; nothing is
			// held in RAM. The digest is then reused for every successor push.
			contentDigest, size, err := sha256OfFile(localFile)
			if err != nil {
				log.Printf("[data-agent/%s] PUSH %s/%s: failed to hash local file: %v", nodeName, odag, task, err)
				// Mark every queued successor Failed since we never got to attempt.
				for _, succ := range body.Successors {
					writeTransferEntry(rel, succ.Name, transferEntry{
						Consumer: succ.Name, Host: succ.Host, Node: succ.Node,
						Retries: 0, LastError: "hash local file: " + err.Error(),
					})
					setTransferState(rel, succ.Name, "Failed")
				}
				return
			}

			// Record actual output size.
			_ = writeFile(bytesFile(rel), fmt.Sprintf("%d", size))

			setSending(rel, true)
			log.Printf("[data-agent/%s] PUSH %s/%s: pushing %d bytes (sha256=%s) to %d successor(s) (max-concurrent=%d)",
				nodeName, odag, task, size, contentDigest, len(body.Successors), maxConcurrentPushes)

			// Fan out push attempts under the node-wide semaphore (shared with
			// the recovery path so a restart with many queued transfers can't
			// fan out beyond maxConcurrentPushes either).
			var wg sync.WaitGroup
			for _, succ := range body.Successors {
				wg.Add(1)
				go func(succ struct {
					Name string `json:"name"`
					Host string `json:"host"`
					Node string `json:"node"`
				}) {
					defer wg.Done()
					acquirePushSlot()
					defer releasePushSlot()
					log.Printf("[data-agent/%s] PUSH %s/%s -> %s (%s)", nodeName, odag, task, succ.Name, succ.Host)
					setTransferState(rel, succ.Name, "Transferring")
					start := time.Now()
					err := pushToNode(odag, task, succ.Host, localFile, contentDigest, size)
					end := time.Now()
					ok := err == nil
					if err != nil {
						log.Printf("[data-agent/%s] PUSH %s/%s -> %s FAILED: %v", nodeName, odag, task, succ.Name, err)
						writeTransferEntry(rel, succ.Name, transferEntry{
							Consumer: succ.Name, Host: succ.Host, Node: succ.Node,
							Retries: pushRetries, LastError: err.Error(),
						})
						setTransferState(rel, succ.Name, "Failed")
					} else {
						log.Printf("[data-agent/%s] PUSH %s/%s -> %s OK (%.3fs)", nodeName, odag, task, succ.Name, end.Sub(start).Seconds())
						writeTransferEntry(rel, succ.Name, transferEntry{
							Consumer: succ.Name, Host: succ.Host, Node: succ.Node,
						})
						setTransferState(rel, succ.Name, "ReadyRemote")
					}
					appendFlow(odag, flowRecord{
						FromTask:  task,
						ToTask:    succ.Name,
						SrcNode:   nodeName,
						DstNode:   succ.Node,
						DataSize:  size,
						StartUnix: float64(start.UnixNano()) / 1e9,
						EndUnix:   float64(end.UnixNano()) / 1e9,
						Ok:        ok,
					})
				}(succ)
			}
			wg.Wait()

			setSending(rel, false)
		}()
	})

	// GET /bytes/<odag>/<task> — query actual output bytes for a task
	http.HandleFunc("/bytes/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		parts, status, msg := parsePathComponents(r.URL.Path, "/bytes/", 2)
		if status != http.StatusOK {
			http.Error(w, msg, status)
			return
		}
		rel := parts[0] + "/" + parts[1]
		data, err := os.ReadFile(bytesFile(rel))
		if os.IsNotExist(err) {
			fmt.Fprint(w, "0")
			return
		}
		if err != nil {
			http.Error(w, "failed to read", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Write(data)
	})

	// PUT/GET /sending/<odag>/<task>
	http.HandleFunc("/sending/", func(w http.ResponseWriter, r *http.Request) {
		parts, status, msg := parsePathComponents(r.URL.Path, "/sending/", 2)
		if status != http.StatusOK {
			http.Error(w, msg, status)
			return
		}
		rel := parts[0] + "/" + parts[1]
		sf := sendingFile(rel)
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body", http.StatusInternalServerError)
				return
			}
			if err := writeFile(sf, strings.TrimSpace(string(body))); err != nil {
				http.Error(w, "failed to write", http.StatusInternalServerError)
				return
			}
			log.Printf("[data-agent/%s] SENDING %s = %s", nodeName, rel, strings.TrimSpace(string(body)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			data, err := os.ReadFile(sf)
			if os.IsNotExist(err) {
				http.NotFound(w, r)
				return
			}
			if err != nil {
				http.Error(w, "failed to read", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/plain")
			w.Write(data)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// PUT/GET /state/<odag>/<task>  — task lifecycle state ONLY.
	// Allowed values: Pending | Running | ComputeDone | Failed.
	// Invalid values are rejected with 400.
	http.HandleFunc("/state/", func(w http.ResponseWriter, r *http.Request) {
		parts, status, msg := parsePathComponents(r.URL.Path, "/state/", 2)
		if status != http.StatusOK {
			http.Error(w, msg, status)
			return
		}
		rel := parts[0] + "/" + parts[1]
		sf := taskStateFile(rel)
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body", http.StatusInternalServerError)
				return
			}
			state := strings.TrimSpace(string(body))
			if !validTaskStates[state] {
				log.Printf("[data-agent/%s] STATE %s: rejected invalid task state %q", nodeName, rel, state)
				http.Error(w, fmt.Sprintf("invalid task state %q (expected one of: Pending, Running, ComputeDone, Failed)", state), http.StatusBadRequest)
				return
			}
			if err := writeFile(sf, state); err != nil {
				http.Error(w, "failed to write", http.StatusInternalServerError)
				return
			}
			log.Printf("[data-agent/%s] STATE %s = %s", nodeName, rel, state)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			data, err := os.ReadFile(sf)
			if os.IsNotExist(err) {
				http.NotFound(w, r)
				return
			}
			if err != nil {
				http.Error(w, "failed to read", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/plain")
			w.Write(data)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// GET/DELETE /ready/<odag>/<task>  — local-data-availability marker.
	// Presence of <rel>/.wl-ready means the producer's output is atomically
	// installed on this node and safe for a consumer to read.
	//
	// PUT is INTENTIONALLY NOT EXPOSED. The marker can only be created as a
	// side-effect of a successful atomic install via PUT /<odag>/<task>/output.
	// This enforces the invariant "the only way to create .wl-ready is
	// through an atomic install" — an external client cannot mark data ready
	// without actually installing the bytes.
	//
	// GET body is "true" or "false" (always 200), cheap to poll.
	// DELETE clears the marker; used by the controller's resetTaskState.
	http.HandleFunc("/ready/", func(w http.ResponseWriter, r *http.Request) {
		parts, status, msg := parsePathComponents(r.URL.Path, "/ready/", 2)
		if status != http.StatusOK {
			http.Error(w, msg, status)
			return
		}
		rel := parts[0] + "/" + parts[1]
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "text/plain")
			if isReady(rel) {
				_, _ = w.Write([]byte("true"))
			} else {
				_, _ = w.Write([]byte("false"))
			}
		case http.MethodDelete:
			clearReady(rel)
			log.Printf("[data-agent/%s] READY %s cleared", nodeName, rel)
			w.WriteHeader(http.StatusOK)
		default:
			// PUT is rejected — readiness is install-driven, not externally writable.
			http.Error(w, "method not allowed (readiness is install-driven; PUT /<odag>/<task>/output instead)", http.StatusMethodNotAllowed)
		}
	})

	// GET /transfers/<odag>/<task>             — list every consumer state as JSON map
	// GET /transfers/<odag>/<task>/<consumer>  — single consumer state (text)
	//
	// Files live on the producer's node only (the node that ran <task>). State
	// values come from validTransferStates. Read-only externally; writes happen
	// exclusively in the push goroutine on this same node.
	// GET /installed/<odag>/<task> — when this node's copy of <task>'s output
	// became readable, and from where. 404 until the payload is installed.
	http.HandleFunc("/installed/", func(w http.ResponseWriter, r *http.Request) {
		parts, status, msg := parsePathComponents(r.URL.Path, "/installed/", 2)
		if status != http.StatusOK {
			http.Error(w, msg, status)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		b, err := os.ReadFile(installedFile(parts[0] + "/" + parts[1]))
		if err != nil {
			http.Error(w, "no install record", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	})

	// /timings/<odag>/<task> — task-internal phase boundaries reported by the
	// SDK (PUT) and read back by the controller (GET). The body is stored
	// verbatim; the agent does not interpret it beyond a size and JSON-shape
	// check, so the SDK can add fields without an agent release.
	http.HandleFunc("/timings/", func(w http.ResponseWriter, r *http.Request) {
		parts, status, msg := parsePathComponents(r.URL.Path, "/timings/", 2)
		if status != http.StatusOK {
			http.Error(w, msg, status)
			return
		}
		rel := parts[0] + "/" + parts[1]
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
			if err != nil {
				http.Error(w, "failed to read body", http.StatusInternalServerError)
				return
			}
			var probe map[string]interface{}
			if err := json.Unmarshal(body, &probe); err != nil {
				http.Error(w, "body must be a JSON object", http.StatusBadRequest)
				return
			}
			if err := writeFile(timingsFile(rel), string(body)); err != nil {
				http.Error(w, "failed to write", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			b, err := os.ReadFile(timingsFile(rel))
			if err != nil {
				http.Error(w, "no timings", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(b)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	http.HandleFunc("/transfers/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		rest := strings.TrimPrefix(r.URL.Path, "/transfers/")
		rest = strings.TrimSuffix(rest, "/")
		parts := strings.Split(rest, "/")
		if len(parts) != 2 && len(parts) != 3 {
			http.Error(w, "expected /transfers/<odag>/<task>[/<consumer>]", http.StatusBadRequest)
			return
		}
		for _, p := range parts {
			if err := validName(p); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		rel := parts[0] + "/" + parts[1]
		if len(parts) == 2 {
			states := listTransferStates(rel)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(states)
			return
		}
		consumer := parts[2]
		b, err := os.ReadFile(transferStateFile(rel, consumer))
		if os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			http.Error(w, "failed to read", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(b)
	})

	// GET /runs — list all ODAG directories on this node with their sizes.
	// Optional query param ?prefix=<template> filters to runs matching that prefix.
	// Response: JSON array of {"name":"<odag>","size":<bytes>} sorted by name.
	http.HandleFunc("/runs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		prefix := r.URL.Query().Get("prefix")

		entries, err := os.ReadDir(dataDir)
		if err != nil {
			http.Error(w, "failed to read data dir", http.StatusInternalServerError)
			return
		}

		var runs []runInfo
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			if prefix != "" && !strings.HasPrefix(name, prefix) {
				continue
			}
			size := dirSize(filepath.Join(dataDir, name))
			runs = append(runs, runInfo{Name: name, Size: size})
		}
		if runs == nil {
			runs = []runInfo{}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(runs)
	})

	// DELETE /data/<odag> — remove all data for a specific ODAG run on this node.
	http.HandleFunc("/data/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		parts, status, msg := parsePathComponents(r.URL.Path, "/data/", 1)
		if status != http.StatusOK {
			http.Error(w, msg, status)
			return
		}
		odag := parts[0]
		target := filepath.Join(dataDir, odag)
		if _, err := os.Stat(target); os.IsNotExist(err) {
			w.WriteHeader(http.StatusOK) // idempotent
			log.Printf("[data-agent/%s] DELETE %s: not found (already clean)", nodeName, odag)
			return
		}
		if err := os.RemoveAll(target); err != nil {
			http.Error(w, "failed to remove", http.StatusInternalServerError)
			log.Printf("[data-agent/%s] DELETE %s: %v", nodeName, odag, err)
			return
		}
		log.Printf("[data-agent/%s] DELETE %s: removed", nodeName, odag)
		w.WriteHeader(http.StatusOK)
	})

	// GET /flows/<odag>    — returns all per-push flow records on this node.
	// DELETE /flows/<odag> — truncates the flow log (idempotent).
	http.HandleFunc("/flows/", func(w http.ResponseWriter, r *http.Request) {
		parts, status, msg := parsePathComponents(r.URL.Path, "/flows/", 1)
		if status != http.StatusOK {
			http.Error(w, msg, status)
			return
		}
		odag := parts[0]
		switch r.Method {
		case http.MethodGet:
			flows := readFlows(odag)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(flows)
		case http.MethodDelete:
			flowsMu.Lock()
			_ = os.Remove(flowsFile(odag))
			flowsMu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// PUT/GET /<odag>/<task>/output
	//
	// This is the catch-all handler: anything that didn't match a more
	// specific prefix lands here. Strict validation closes path-traversal
	// risk before any filesystem path is constructed. Only "output" is
	// currently accepted as the filename; other names are rejected with 400.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		cleanPath := filepath.Clean(r.URL.Path)
		segs := strings.Split(strings.TrimPrefix(cleanPath, "/"), "/")
		if len(segs) != 3 {
			http.Error(w, "expected /<odag>/<task>/output", http.StatusBadRequest)
			return
		}
		if err := validName(segs[0]); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := validName(segs[1]); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if segs[2] != "output" {
			http.Error(w, fmt.Sprintf("only 'output' filename is supported, got %q", segs[2]), http.StatusBadRequest)
			return
		}
		fullPath := filepath.Join(dataDir, segs[0], segs[1], "output")
		switch r.Method {
		case http.MethodPut:
			metricPutTotal.Add(1)
			// Idempotency fast-path: if the caller supplied an
			// X-Wayline-Content-SHA256 and the output is already installed
			// (.wl-ready present) with the same digest, return 200 without
			// touching disk. If the installed digest differs, return 409
			// Conflict instead of silently overwriting a payload that may be
			// in use by a consumer.
			//
			// Body is drained in both early-exit paths so the HTTP connection
			// stays clean for the next request.
			rel := segs[0] + "/" + segs[1]
			claimed := r.Header.Get(headerContentSHA256)
			if claimed != "" && isReady(rel) {
				existing := readInstalledDigest(rel)
				if existing != "" && existing == claimed {
					_, _ = io.Copy(io.Discard, r.Body)
					metricPutIdempotent.Add(1)
					log.Printf("[data-agent/%s] PUT %s: idempotent — already installed with sha256=%s", nodeName, r.URL.Path, claimed)
					w.WriteHeader(http.StatusOK)
					return
				}
				if existing != "" && existing != claimed {
					_, _ = io.Copy(io.Discard, r.Body)
					metricPutConflict.Add(1)
					log.Printf("[data-agent/%s] PUT %s: conflict — installed sha256=%s, claimed sha256=%s; rejected",
						nodeName, r.URL.Path, existing, claimed)
					http.Error(w, "X-Wayline-Content-SHA256 conflict with installed output", http.StatusConflict)
					return
				}
			}

			// Decode Content-Encoding if the sender wrapped the body. The
			// installed payload is always the raw file content; compression
			// is purely a wire concern, so the SHA-256 in the header (and the
			// digest we verify before rename) is over the DECOMPRESSED bytes.
			bodyReader := r.Body
			if enc := r.Header.Get("Content-Encoding"); enc != "" {
				switch enc {
				case "gzip":
					gzr, gerr := gzip.NewReader(r.Body)
					if gerr != nil {
						log.Printf("[data-agent/%s] PUT %s: gzip reader: %v", nodeName, r.URL.Path, gerr)
						http.Error(w, "invalid gzip body", http.StatusBadRequest)
						return
					}
					defer gzr.Close()
					bodyReader = gzr
				default:
					http.Error(w, fmt.Sprintf("unsupported Content-Encoding %q", enc), http.StatusBadRequest)
					return
				}
			}

			// Atomic install: stream to a temp file, fsync, verify SHA-256,
			// then rename into place. On digest mismatch the final path is
			// never touched (temp file is removed). The .wl-ready marker is
			// only set AFTER the rename succeeds, so a consumer that gates on
			// the marker can never observe a partial or stale payload.
			computedDigest, n, err := installAtomically(fullPath, bodyReader, claimed)
			if err != nil {
				if errors.Is(err, errChecksumMismatch) {
					metricPutMismatch.Add(1)
					log.Printf("[data-agent/%s] PUT %s: %v; rejected", nodeName, r.URL.Path, err)
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				log.Printf("[data-agent/%s] PUT %s: install failed: %v", nodeName, r.URL.Path, err)
				http.Error(w, "failed to install file", http.StatusInternalServerError)
				return
			}
			metricPutOK.Add(1)
			metricBytesIn.Add(n)
			// Persist the SHA-256 sidecar (so future requests can dedupe),
			// then set the data-readiness marker. Order matters — without the
			// sidecar the next request can't fast-path, but without the
			// marker no consumer reads stale bytes. Task lifecycle state
			// belongs to the producer's own node and is not touched here.
			writeInstalledDigest(rel, computedDigest)
			// Data-availability timestamp: recorded immediately before the
			// readiness marker, so it is never later than the moment a
			// consumer could first observe the payload.
			src, fromNode := "local", ""
			if sn := r.Header.Get(headerSourceNode); sn != "" && sn != nodeName {
				src, fromNode = "remote", sn
			}
			recordInstalled(rel, installRecord{
				UnixTime: float64(time.Now().UnixNano()) / 1e9,
				Bytes:    n,
				Source:   src,
				FromNode: fromNode,
			})
			setReady(rel)
			log.Printf("[data-agent/%s] PUT %s (%d bytes, sha256=%s) → ReadyLocal", nodeName, r.URL.Path, n, computedDigest)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			// Default: require .wl-ready. Files may exist on disk before
			// install completes (a previous PUT's temp file, an in-flight
			// install, or a Failed/restored state). Consumers must gate on
			// the marker. ?unsafe=1 bypasses for debugging only.
			rel := segs[0] + "/" + segs[1]
			if r.URL.Query().Get("unsafe") != "1" && !isReady(rel) {
				http.Error(w, "data not ready (output not installed); pass ?unsafe=1 to bypass", http.StatusConflict)
				return
			}
			if _, err := os.Stat(fullPath); os.IsNotExist(err) {
				http.NotFound(w, r)
				return
			}
			log.Printf("[data-agent/%s] GET %s", nodeName, r.URL.Path)
			fs.ServeHTTP(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	addr := fmt.Sprintf(":%d", port)
	log.Printf("[data-agent] node=%s serving %s on %s", nodeName, dataDir, addr)

	// Kick off recovery for any transfers stranded by a previous run of this
	// agent. Runs in a goroutine so we don't block ListenAndServe; recovery
	// uses the same push semaphore as live requests, so they coexist cleanly.
	go recoverTransfers(nodeName)

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("[data-agent] %v", err)
	}
}
