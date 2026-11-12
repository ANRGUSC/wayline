package main

// One connection pool for every request to data agents and warm runners.
//
// The controller talks to every node's agent and runner many times a second
// (readiness, task state, timings, warm-call polls, pushes, cancels). Go's
// default transport keeps two idle connections per host, so beyond that each
// request opened a fresh TCP connection and left it in TIME_WAIT. A 400-task
// run on 48 nodes piled up 63,514 TIME_WAIT sockets in the controller's
// network namespace, and finding a free ephemeral port made connect() the
// controller's main cost (61% of 8.4 cores). Every http.Client here has no
// transport of its own, so replacing the default transport covers them all:
// a deep idle pool per host, and response bodies drained on Close so their
// connections go back to the pool instead of being dropped.

import (
	"io"
	"net"
	"net/http"
	"time"
)

type drainOnClose struct{ io.ReadCloser }

func (d drainOnClose) Close() error {
	_, _ = io.CopyN(io.Discard, d.ReadCloser, 256<<10)
	return d.ReadCloser.Close()
}

type drainingTransport struct{ base http.RoundTripper }

func (t drainingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(r)
	if resp != nil && resp.Body != nil {
		resp.Body = drainOnClose{resp.Body}
	}
	return resp, err
}

func init() {
	http.DefaultTransport = drainingTransport{base: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          8192,
		MaxIdleConnsPerHost:   128,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}}
}
