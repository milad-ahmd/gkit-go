// Package testutil provides helpers for writing tests against gkit components.
package testutil

import (
	"net"
	"testing"
	"time"
)

// Eventually asserts that condition returns true within timeout,
// polling every tick. It calls t.Fatal on failure.
func Eventually(t testing.TB, condition func() bool, timeout, tick time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(tick)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// RequireEventually is like Eventually but marks the test as failed immediately.
func RequireEventually(t testing.TB, condition func() bool, timeout, tick time.Duration) {
	t.Helper()
	Eventually(t, condition, timeout, tick)
}

// FreePort returns a free TCP port on localhost by briefly listening on :0.
func FreePort(t testing.TB) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("testutil.FreePort: %v", err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	lis.Close()
	return port
}

// Must fails the test if err is non-nil.
func Must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// MustVal fails the test if err is non-nil, otherwise returns val.
func MustVal[T any](t testing.TB, val T, err error) T {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return val
}

// AssertNoError fails the test if err is non-nil (non-fatal).
func AssertNoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
