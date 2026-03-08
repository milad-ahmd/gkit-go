package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/miladhzz/gkit/pkg/health"
)

var errFake = errors.New("dependency down")

func TestGroup_AllHealthy(t *testing.T) {
	g := health.New()
	g.Register("db", health.CheckerFunc(func(_ context.Context) error { return nil }))
	g.Register("cache", health.CheckerFunc(func(_ context.Context) error { return nil }))

	report := g.Check(context.Background())

	if !report.Healthy {
		t.Fatal("expected healthy report")
	}
	if len(report.Checks) != 2 {
		t.Fatalf("expected 2 checks, got %d", len(report.Checks))
	}
	for _, c := range report.Checks {
		if !c.Healthy {
			t.Errorf("check %q unexpectedly unhealthy", c.Name)
		}
	}
}

func TestGroup_OneUnhealthy(t *testing.T) {
	g := health.New()
	g.Register("db", health.CheckerFunc(func(_ context.Context) error { return nil }))
	g.Register("cache", health.CheckerFunc(func(_ context.Context) error { return errFake }))

	report := g.Check(context.Background())

	if report.Healthy {
		t.Fatal("expected unhealthy report")
	}

	var cacheStatus *health.Status
	for i := range report.Checks {
		if report.Checks[i].Name == "cache" {
			cacheStatus = &report.Checks[i]
		}
	}
	if cacheStatus == nil {
		t.Fatal("cache check not found in report")
	}
	if cacheStatus.Healthy {
		t.Error("expected cache check to be unhealthy")
	}
	if cacheStatus.Error == "" {
		t.Error("expected non-empty error message")
	}
}

func TestGroup_CheckerTimeout(t *testing.T) {
	g := health.New(health.WithTimeout(20 * time.Millisecond))
	g.Register("slow", health.CheckerFunc(func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			return nil
		}
	}))

	report := g.Check(context.Background())

	if report.Healthy {
		t.Fatal("expected unhealthy due to timeout")
	}
}

func TestGroup_ReadyHandler_200(t *testing.T) {
	g := health.New()
	g.Register("ok", health.CheckerFunc(func(_ context.Context) error { return nil }))

	rec := httptest.NewRecorder()
	g.ReadyHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var report health.Report
	if err := json.NewDecoder(rec.Body).Decode(&report); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if !report.Healthy {
		t.Error("expected healthy report")
	}
}

func TestGroup_ReadyHandler_503(t *testing.T) {
	g := health.New()
	g.Register("broken", health.CheckerFunc(func(_ context.Context) error { return errFake }))

	rec := httptest.NewRecorder()
	g.ReadyHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestGroup_LiveHandler_AlwaysOK(t *testing.T) {
	g := health.New()
	// Even with no checks, liveness is always OK.
	rec := httptest.NewRecorder()
	g.LiveHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}
