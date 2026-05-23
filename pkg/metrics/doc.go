// Package metrics provides utilities for collecting and exposing cluster metrics
// used by the Wayline scheduler and controllers.
//
// This includes:
//   - Node resource availability (CPU, memory) from the K8s API.
//   - Inter-node bandwidth measurements (from pathload or iperf3 results).
//   - DAG execution metrics (makespan, task durations) for historical analysis.
package metrics
