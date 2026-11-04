package main

import (
	"testing"
	"time"
)

// Retries get longer deadlines, so pushes sharing a slow link eventually
// finish instead of timing out together forever.
func TestPushAttemptTimeoutGrows(t *testing.T) {
	base := pushTimeoutFor(550000)
	if got := pushAttemptTimeout(550000, 1); got != base {
		t.Fatalf("attempt 1 = %v, want %v", got, base)
	}
	if got := pushAttemptTimeout(550000, 2); got != 2*base {
		t.Fatalf("attempt 2 = %v, want %v", got, 2*base)
	}
	if got := pushAttemptTimeout(550000, 40); got != pushTimeoutMax {
		t.Fatalf("attempt 40 = %v, want the cap %v", got, pushTimeoutMax)
	}
	if pushTimeoutMax < 10*time.Minute {
		t.Fatal("cap too small")
	}
}
