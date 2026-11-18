package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// The instrumentation fields round-trip through the flow log, and records
// without them (older agents, resumed transfers) still decode.
func TestFlowRecordInstrumentationRoundTrip(t *testing.T) {
	old := dataDir
	dataDir = t.TempDir()
	t.Cleanup(func() { dataDir = old })
	appendFlow("run1", flowRecord{FromTask: "a.to-b", ToTask: "b", DataSize: 3, StartUnix: 10, EndUnix: 12, Ok: true,
		RequestUnix: 8.5, HashedUnix: 9.25})
	appendFlow("run1", flowRecord{FromTask: "a.to-c", ToTask: "c", StartUnix: 10, EndUnix: 11, Ok: true})
	got := readFlows("run1")
	if len(got) != 2 || got[0].RequestUnix != 8.5 || got[0].HashedUnix != 9.25 || got[1].RequestUnix != 0 {
		t.Fatalf("flows: %+v", got)
	}
	raw, _ := os.ReadFile(flowsFile("run1"))
	if lines := strings.Split(strings.TrimSpace(string(raw)), "\n"); strings.Contains(lines[1], "requestUnix") {
		t.Fatalf("unset instrumentation fields must be omitted: %s", lines[1])
	}
}

func TestInstallRecordReceiveStart(t *testing.T) {
	old := dataDir
	dataDir = t.TempDir()
	t.Cleanup(func() { dataDir = old })
	if err := os.MkdirAll(dataDir+"/run1/a.to-b", 0o755); err != nil {
		t.Fatal(err)
	}
	recordInstalled("run1/a.to-b", installRecord{UnixTime: 5, Bytes: 7, Source: "remote", FromNode: "n1", ReceiveStartUnix: 4.5})
	b, err := os.ReadFile(installedFile("run1/a.to-b"))
	if err != nil {
		t.Fatal(err)
	}
	var rec installRecord
	if err := json.Unmarshal(b, &rec); err != nil || rec.ReceiveStartUnix != 4.5 || rec.UnixTime != 5 {
		t.Fatalf("record %s: %+v %v", b, rec, err)
	}
}
