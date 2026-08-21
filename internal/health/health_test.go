package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func get(t *testing.T, h http.Handler, path string) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %s: %v (body %q)", path, err, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("%s content-type = %q", path, got)
	}
	return rec.Code, body
}

// Liveness must not depend on anything. A broken dependency is a readiness
// problem; failing liveness would have the supervisor restart a process that
// is working.
func TestHealthzIgnoresFailingChecks(t *testing.T) {
	h := New(Options{
		Checks: map[string]Check{"nats": func() error { return errors.New("down") }},
	}).Handler()

	code, body := get(t, h, "/healthz")
	if code != http.StatusOK {
		t.Errorf("code = %d, want 200", code)
	}
	if body["status"] != "ok" {
		t.Errorf("status = %v", body["status"])
	}
}

func TestReadyzAllChecksPass(t *testing.T) {
	h := New(Options{
		Checks: map[string]Check{
			"nats":  func() error { return nil },
			"store": func() error { return nil },
		},
	}).Handler()

	code, body := get(t, h, "/readyz")
	if code != http.StatusOK {
		t.Fatalf("code = %d, want 200", code)
	}
	if body["status"] != "ready" {
		t.Errorf("status = %v", body["status"])
	}
	checks, ok := body["checks"].(map[string]any)
	if !ok {
		t.Fatalf("checks missing: %v", body)
	}
	for _, name := range []string{"nats", "store"} {
		if checks[name] != "ok" {
			t.Errorf("%s = %v, want ok", name, checks[name])
		}
	}
}

// One failing check must fail readiness and name itself, so the reason is in
// the response rather than only in the logs.
func TestReadyzReportsTheFailingCheck(t *testing.T) {
	h := New(Options{
		Checks: map[string]Check{
			"nats":  func() error { return fmt.Errorf("not connected to nats://x:4222") },
			"store": func() error { return nil },
		},
	}).Handler()

	code, body := get(t, h, "/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d, want 503", code)
	}
	if body["status"] != "not ready" {
		t.Errorf("status = %v", body["status"])
	}
	checks := body["checks"].(map[string]any)
	if checks["nats"] != "not connected to nats://x:4222" {
		t.Errorf("nats = %v, want the check's own error", checks["nats"])
	}
	if checks["store"] != "ok" {
		t.Errorf("a passing check was not reported: %v", checks["store"])
	}
}

func TestReadyzWithNoChecks(t *testing.T) {
	code, body := get(t, New(Options{}).Handler(), "/readyz")
	if code != http.StatusOK {
		t.Errorf("code = %d, want 200", code)
	}
	if body["status"] != "ready" {
		t.Errorf("status = %v", body["status"])
	}
}

func TestStatuszReportsDetails(t *testing.T) {
	h := New(Options{
		Version: "1.2.3",
		Details: map[string]Detail{
			"ingest": func() any { return map[string]any{"appended": 7} },
		},
	}).Handler()

	code, body := get(t, h, "/statusz")
	if code != http.StatusOK {
		t.Fatalf("code = %d, want 200", code)
	}
	if body["version"] != "1.2.3" {
		t.Errorf("version = %v", body["version"])
	}
	if body["go_version"] == "" {
		t.Error("go_version missing")
	}
	if _, ok := body["uptime_ms"]; !ok {
		t.Error("uptime_ms missing")
	}
	ingest, ok := body["ingest"].(map[string]any)
	if !ok {
		t.Fatalf("ingest detail missing: %v", body)
	}
	if ingest["appended"] != float64(7) {
		t.Errorf("appended = %v, want 7", ingest["appended"])
	}
}

// A detail that panics or lies must not be able to fail readiness, so status
// and readiness stay independent.
func TestStatuszDetailsDoNotGateReadiness(t *testing.T) {
	s := New(Options{
		Checks:  map[string]Check{"store": func() error { return nil }},
		Details: map[string]Detail{"store": func() any { return "anything at all" }},
	})

	if code, _ := get(t, s.Handler(), "/readyz"); code != http.StatusOK {
		t.Errorf("readyz code = %d, want 200", code)
	}
}

func TestNewDefaultsVersion(t *testing.T) {
	_, body := get(t, New(Options{}).Handler(), "/statusz")
	if body["version"] != "dev" {
		t.Errorf("version = %v, want dev", body["version"])
	}
}

func TestServeListensAndShutsDown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- New(Options{Version: "test"}).Serve(ctx, addr) }()

	var resp *http.Response
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err = http.Get("http://" + addr + "/healthz") //nolint:noctx // short-lived probe in a test
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Errorf("close body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("code = %d, want 200", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not return after cancel")
	}
}

func TestServeReportsABadAddress(t *testing.T) {
	err := New(Options{}).Serve(t.Context(), "127.0.0.1:-1")
	if err == nil {
		t.Fatal("want error for an unusable address")
	}
}
