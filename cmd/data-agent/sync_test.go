package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

// Every durability mode must install atomically and verify digests; only the
// fsyncs differ.
func TestInstallAtomicallyAllSyncModes(t *testing.T) {
	payload := bytes.Repeat([]byte("wayline"), 100_000)
	sum := sha256.Sum256(payload)
	want := hex.EncodeToString(sum[:])
	for _, mode := range []string{"full", "data", "none"} {
		syncMode = mode
		dir := t.TempDir()
		dest := filepath.Join(dir, "run", "task", "output")
		got, n, err := installAtomically(dest, bytes.NewReader(payload), want)
		if err != nil || got != want || n != int64(len(payload)) {
			t.Fatalf("%s: install = (%s, %d, %v)", mode, got, n, err)
		}
		if b, _ := os.ReadFile(dest); !bytes.Equal(b, payload) {
			t.Fatalf("%s: installed bytes differ", mode)
		}
		left, _ := filepath.Glob(dest + ".tmp.*")
		if len(left) != 0 {
			t.Fatalf("%s: temp files left behind: %v", mode, left)
		}
		if _, _, err := installAtomically(filepath.Join(dir, "x", "output"), bytes.NewReader(payload), "00"); !errors.Is(err, errChecksumMismatch) {
			t.Fatalf("%s: wrong digest accepted: %v", mode, err)
		}
		if err := writeTextAtomic(filepath.Join(dir, "state"), "Running"); err != nil {
			t.Fatalf("%s: writeTextAtomic: %v", mode, err)
		}
		if b, _ := os.ReadFile(filepath.Join(dir, "state")); string(b) != "Running" {
			t.Fatalf("%s: state file = %q", mode, b)
		}
	}
	syncMode = "full"
}

func TestPerObjectDurability(t *testing.T) {
	dataDir = t.TempDir()
	defer func() { syncMode = "full" }()
	for _, mode := range []string{"full", "none"} {
		syncMode = mode
		r, _ := http.NewRequest(http.MethodPut, "/x", nil)
		if got, want := requestDurable(r), mode == "full"; got != want {
			t.Errorf("%s: no header -> %v, want node default %v", mode, got, want)
		}
		r.Header.Set(headerDurable, "1")
		if !requestDurable(r) {
			t.Errorf("%s: header 1 not durable", mode)
		}
		r.Header.Set(headerDurable, "false")
		if requestDurable(r) {
			t.Errorf("%s: header false durable", mode)
		}
		rel := "run/" + mode + ".obj"
		if got := objectDurable(rel); got != (mode == "full") {
			t.Errorf("%s: unmarked object -> %v, want node default", mode, got)
		}
		markDurable(rel, mode == "none", false) // opposite of the default
		if got := objectDurable(rel); got != (mode == "none") {
			t.Errorf("%s: marked object -> %v", mode, got)
		}
	}
}
