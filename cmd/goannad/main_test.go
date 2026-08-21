package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mulgadc/goanna/internal/ingest"
)

// startNATS runs an in-process NATS server with JetStream enabled and
// returns its client URL.
func startNATS(t *testing.T) string {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("nats server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server did not start")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

// The daemon must come up against a real broker, acknowledge a published
// batch, and stop when its context is cancelled.
func TestRunIngestsAndShutsDown(t *testing.T) {
	url := startNATS(t)
	dir := filepath.Join(t.TempDir(), "tsdb")

	cfg := config{
		natsURL:    url,
		dataDir:    dir,
		retention:  24 * time.Hour,
		logLevel:   "error",
		streamName: ingest.DefaultStream,
		healthAddr: freeAddr(t),
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	payload, err := json.Marshal(map[string]any{
		"ts":             time.Now().UnixMilli(),
		"period_seconds": 60,
		"series": []map[string]any{
			{"name": "goanna_ec2_cpu_utilization", "value": 12.5},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Wait on the consumer's ack floor rather than on anything in the data
	// directory: the TSDB creates its WAL at open, long before a message has
	// been through the pipe.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	var acked bool
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && !acked {
		if err := nc.Publish("metrics.ec2.i-0abc123def4567890", payload); err != nil {
			t.Fatalf("publish: %v", err)
		}
		time.Sleep(250 * time.Millisecond)

		cons, err := js.Consumer(ctx, ingest.DefaultStream, ingest.DefaultDurable)
		if err != nil {
			continue
		}
		info, err := cons.Info(ctx)
		if err == nil && info.AckFloor.Stream > 0 {
			acked = true
		}
	}
	if !acked {
		t.Error("goannad did not acknowledge a published batch")
	}

	// The status endpoint must show the batch that just landed, which is how
	// a deployment is verified without shelling into the TSDB.
	status := getJSON(t, "http://"+cfg.healthAddr+"/statusz")
	stats, ok := status["ingest"].(map[string]any)
	if !ok {
		t.Fatalf("statusz has no ingest detail: %v", status)
	}
	if appended, _ := stats["appended"].(float64); appended < 1 {
		t.Errorf("statusz reports appended = %v, want at least 1", stats["appended"])
	}
	if ready := getJSON(t, "http://"+cfg.healthAddr+"/readyz"); ready["status"] != "ready" {
		t.Errorf("readyz = %v, want ready", ready["status"])
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("run: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run did not return after cancel")
	}
}

// freeAddr returns a loopback address nothing is listening on. There is a
// race between closing and rebinding, which is acceptable in a test and the
// only way to get a port without changing run's signature.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}

func getJSON(t *testing.T, url string) map[string]any {
	t.Helper()
	resp, err := http.Get(url) //nolint:noctx // short-lived probe in a test
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close body: %v", err)
		}
	}()

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
	return body
}

func TestRunRejectsBadLogLevel(t *testing.T) {
	err := run(t.Context(), config{logLevel: "chatty", dataDir: t.TempDir()})
	if err == nil {
		t.Fatal("want error for an unknown log level")
	}
}

func TestRunRejectsUnreachableNATS(t *testing.T) {
	cfg := config{
		// Port 1 is privileged and nothing listens on it, so the dial fails
		// rather than hanging on a reachable host.
		natsURL:  "nats://127.0.0.1:1",
		dataDir:  filepath.Join(t.TempDir(), "tsdb"),
		logLevel: "error",
	}
	if err := run(t.Context(), cfg); err == nil {
		t.Fatal("want error when NATS is unreachable")
	}
}

func TestRunRejectsUnusableDataDir(t *testing.T) {
	// A file where the TSDB wants a directory.
	path := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := run(t.Context(), config{logLevel: "error", dataDir: path}); err == nil {
		t.Fatal("want error when the data dir is not a directory")
	}
}

func TestParseFlagsDefaults(t *testing.T) {
	cfg, err := parseFlags(testFlagSet(), nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.natsURL != nats.DefaultURL {
		t.Errorf("nats url = %q", cfg.natsURL)
	}
	if cfg.dataDir != "/var/lib/goanna/tsdb" {
		t.Errorf("data dir = %q", cfg.dataDir)
	}
	if cfg.streamName != ingest.DefaultStream {
		t.Errorf("stream = %q", cfg.streamName)
	}
	if cfg.logLevel != "info" {
		t.Errorf("log level = %q", cfg.logLevel)
	}
	if cfg.retention != 15*24*time.Hour {
		t.Errorf("retention = %v", cfg.retention)
	}
}

func TestParseFlagsOverridesEnvironment(t *testing.T) {
	t.Setenv("GOANNA_NATS_URL", "nats://from-env:4222")
	t.Setenv("GOANNA_DATA_DIR", "/from/env")
	cfg, err := parseFlags(testFlagSet(), []string{
		"-nats-url", "nats://from-flag:4222",
		"-retention", "1h",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.natsURL != "nats://from-flag:4222" {
		t.Errorf("flag did not beat the environment: %q", cfg.natsURL)
	}
	if cfg.dataDir != "/from/env" {
		t.Errorf("environment not used where no flag was given: %q", cfg.dataDir)
	}
	if cfg.retention != time.Hour {
		t.Errorf("retention = %v, want 1h", cfg.retention)
	}
}

func TestConnectNATSUsesToken(t *testing.T) {
	logger, err := newLogger("error")
	if err != nil {
		t.Fatalf("logger: %v", err)
	}
	nc, err := connectNATS(config{natsURL: startNATS(t), natsToken: "unused-by-an-open-server"}, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	nc.Close()
}

func TestNewLogger(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "error", "ERROR"} {
		if _, err := newLogger(level); err != nil {
			t.Errorf("newLogger(%q): %v", level, err)
		}
	}
	if _, err := newLogger("verbose"); err == nil {
		t.Error("want error for an unknown level")
	}
}

func TestEnv(t *testing.T) {
	if got := env("GOANNA_TEST_UNSET", "fallback"); got != "fallback" {
		t.Errorf("= %q, want fallback", got)
	}
	t.Setenv("GOANNA_TEST_SET", "value")
	if got := env("GOANNA_TEST_SET", "fallback"); got != "value" {
		t.Errorf("= %q, want value", got)
	}
	// An empty variable is treated as unset, so a blank systemd Environment=
	// line does not wipe the default.
	t.Setenv("GOANNA_TEST_EMPTY", "")
	if got := env("GOANNA_TEST_EMPTY", "fallback"); got != "fallback" {
		t.Errorf("= %q, want fallback", got)
	}
}

func testFlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("goannad", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

func TestParseFlagsRejectsUnknownFlag(t *testing.T) {
	if _, err := parseFlags(testFlagSet(), []string{"-nope"}); err == nil {
		t.Fatal("want error for an unknown flag")
	}
}
