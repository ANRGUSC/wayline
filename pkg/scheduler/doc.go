// Package scheduler provides the interface and utilities for Wayline scheduling.
//
// The scheduler interface is simple: given a DAG spec and cluster state,
// return a placement assignment (task -> node) and estimated makespan.
//
// Schedulers can be:
//   - Built-in Go implementations that are linked into the controllers.
//   - External Python scripts invoked via subprocess (stdin/stdout JSON).
//   - Remote HTTP services implementing the scheduler API.
//
// The JSON schema for input/output is defined in api/scheduler/schema.json.
package scheduler
