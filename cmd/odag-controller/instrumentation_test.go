package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// redirectTransport sends every request to one test server, whatever host
// and port the URL names (the controller always dials <nodeIP>:dataAgentPort).
type redirectTransport struct{ target *url.URL }

func (r redirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme, req.URL.Host = r.target.Scheme, r.target.Host
	return http.DefaultTransport.RoundTrip(req)
}

func withFakeAgent(t *testing.T, records map[string]string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if body, ok := records[r.URL.Path]; ok {
			_, _ = w.Write([]byte(body))
			return
		}
		http.Error(w, "no install record", http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	old := httpClient
	httpClient = &http.Client{Transport: redirectTransport{u}}
	t.Cleanup(func() { httpClient = old })
}

// Per-input install records on the consumer's node: named inputs are looked
// up by object key, default outputs by producer; inputsReadyTime (the
// latest) only when every input has a record.
func TestInputInstallsPerObject(t *testing.T) {
	withFakeAgent(t, map[string]string{
		"/installed/run1/a.to-c": `{"unixTime":100.5,"bytes":10,"source":"remote","fromNode":"n1","receiveStartUnix":99.25}`,
		"/installed/run1/b":      `{"unixTime":101.75,"bytes":5,"source":"local"}`,
	})
	tasks := []taskSpec{
		{Name: "a"}, {Name: "b"},
		{Name: "c", Dependencies: []string{"a", "b"}, Inputs: []inputSpec{{Producer: "a", Object: "to-c"}}},
		{Name: "d", Dependencies: []string{"a"}, Inputs: []inputSpec{{Producer: "a", Object: "to-d"}}},
	}
	assign := map[string]nodeInfo{"c": {name: "n2", ip: "10.0.0.2"}, "d": {name: "n2", ip: "10.0.0.2"}}

	entries, latest, complete := inputInstalls("c", tasks, assign, "run1")
	if !complete || latest != 101.75 || len(entries) != 2 {
		t.Fatalf("c: entries=%v latest=%v complete=%v", entries, latest, complete)
	}
	a := entries[0]
	if a["object"] != "a.to-c" || a["producer"] != "a" || a["installedUnix"] != 100.5 ||
		a["receiveStartUnix"] != 99.25 || a["fromNode"] != "n1" || a["bytes"] != int64(10) {
		t.Fatalf("a.to-c entry: %v", a)
	}
	if entries[1]["object"] != "b" || entries[1]["source"] != "local" {
		t.Fatalf("b entry: %v", entries[1])
	}
	if _, err := json.Marshal(entries); err != nil {
		t.Fatal(err)
	}

	// A missing record: the entry is kept without times, and the run is incomplete.
	entries, _, complete = inputInstalls("d", tasks, assign, "run1")
	if complete || len(entries) != 1 || entries[0]["installedUnix"] != nil {
		t.Fatalf("d: %v complete=%v", entries, complete)
	}
	// Source tasks have no inputs.
	if entries, _, _ := inputInstalls("a", tasks, assign, "run1"); entries != nil {
		t.Fatalf("a: %v", entries)
	}
}

// Older agents send flows without the instrumentation fields; both decode.
func TestFlowRecordInstrumentationFields(t *testing.T) {
	var f dataAgentFlow
	if err := json.Unmarshal([]byte(`{"fromTask":"a.to-b","toTask":"b","startUnix":5,"endUnix":7,"ok":true}`), &f); err != nil ||
		f.RequestUnix != 0 || f.HashedUnix != 0 {
		t.Fatalf("old flow: %+v %v", f, err)
	}
	if err := json.Unmarshal([]byte(`{"fromTask":"a.to-b","startUnix":5,"endUnix":7,"requestUnix":3.5,"hashedUnix":4}`), &f); err != nil ||
		f.RequestUnix != 3.5 || f.HashedUnix != 4 {
		t.Fatalf("new flow: %+v %v", f, err)
	}
}
