package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mulgadc/goanna/internal/wire"
)

const testInstance = "i-0abc123def4567890"

// startNATS runs an in-process NATS server with JetStream enabled.
func startNATS(t *testing.T) *nats.Conn {
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

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// recorder is a store that records what it was asked to append.
type recorder struct {
	mu      sync.Mutex
	batches []wire.Batch
	ids     []string
	fail    error
	calls   int
}

func (r *recorder) Append(_ context.Context, instanceID string, b wire.Batch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.fail != nil {
		return r.fail
	}
	r.ids = append(r.ids, instanceID)
	r.batches = append(r.batches, b)
	return nil
}

func (r *recorder) snapshot() ([]string, []wire.Batch, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ids...), append([]wire.Batch(nil), r.batches...), r.calls
}

func (r *recorder) waitFor(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, batches, _ := r.snapshot(); len(batches) >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, batches, calls := r.snapshot()
	t.Fatalf("timed out: appended %d batches (%d calls), want %d", len(batches), calls, n)
}

func runIngest(t *testing.T, nc *nats.Conn, store Appender) {
	t.Helper()
	in, err := New(nc, store, Config{AckWait: time.Second})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := in.Run(ctx); err != nil {
			t.Errorf("run: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	// Consumption starts asynchronously; give the stream and consumer time
	// to exist before the test publishes.
	waitForStream(t, nc)
}

func waitForStream(t *testing.T, nc *nats.Conn) {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		s, err := js.Stream(ctx, DefaultStream)
		if err == nil {
			_, cErr := s.Consumer(ctx, DefaultDurable)
			cancel()
			if cErr == nil {
				return
			}
			continue
		}
		cancel()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("stream and consumer did not appear")
}

func payload(t *testing.T, ts int64) []byte {
	t.Helper()
	b := wire.Batch{
		TS:            ts,
		PeriodSeconds: 60,
		Series: []wire.Series{{
			Name:   "goanna_ec2_cpu_utilization",
			Labels: map[string]string{"namespace": "AWS/EC2", "instance_id": testInstance},
			Value:  12.5,
			Unit:   "Percent",
		}},
	}
	data, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

// The whole zero-producer-change position rests on this: a message
// published with plain core NATS must be captured by the stream and
// delivered to the durable consumer. If this fails, spinifex has to move to
// js.Publish before Goanna can consume anything.
func TestCorePublishIsCapturedByTheStream(t *testing.T) {
	nc := startNATS(t)
	store := &recorder{}
	runIngest(t, nc, store)

	if err := nc.Publish(wire.SubjectPrefix+testInstance, payload(t, 1787284904)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	store.waitFor(t, 1)
	ids, batches, _ := store.snapshot()
	if ids[0] != testInstance {
		t.Errorf("instance id = %q, want %q", ids[0], testInstance)
	}
	if got := batches[0].Series[0].Value; got != 12.5 {
		t.Errorf("value = %v, want 12.5", got)
	}
}

// Messages published before the consumer existed must still arrive. This is
// the replay-on-reconnect property JetStream is here for.
func TestMessagesPublishedBeforeStartAreReplayed(t *testing.T) {
	nc := startNATS(t)

	// Create the stream, publish into it, then start ingest afterwards.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     DefaultStream,
		Subjects: []string{DefaultSubject},
		Storage:  jetstream.FileStorage,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if err := nc.Publish(wire.SubjectPrefix+testInstance, payload(t, 1787284904)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	store := &recorder{}
	runIngest(t, nc, store)
	store.waitFor(t, 1)
}

// A payload that will never decode must not wedge the consumer behind it.
func TestUndecodableMessageIsTerminatedNotRetried(t *testing.T) {
	nc := startNATS(t)
	store := &recorder{}
	runIngest(t, nc, store)

	if err := nc.Publish(wire.SubjectPrefix+testInstance, []byte("{not json")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.Publish(wire.SubjectPrefix+testInstance, payload(t, 1787284904)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// The good message behind the bad one still lands.
	store.waitFor(t, 1)

	// And the bad one is gone rather than redelivered: give it more than one
	// AckWait to come back, then confirm the store was never called for it.
	time.Sleep(2 * time.Second)
	_, batches, calls := store.snapshot()
	if len(batches) != 1 {
		t.Errorf("batches = %d, want 1", len(batches))
	}
	if calls != 1 {
		t.Errorf("store called %d times, want 1 — the bad message was retried", calls)
	}
}

// A failed append must come back, because it may succeed next time.
func TestFailedAppendIsRedelivered(t *testing.T) {
	nc := startNATS(t)
	store := &recorder{fail: errors.New("disk full")}
	runIngest(t, nc, store)

	if err := nc.Publish(wire.SubjectPrefix+testInstance, payload(t, 1787284904)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, calls := store.snapshot(); calls >= 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, _, calls := store.snapshot()
	t.Fatalf("store called %d times, want at least 2 redeliveries", calls)
}

func TestNewRequiresStore(t *testing.T) {
	nc := startNATS(t)
	if _, err := New(nc, nil, Config{}); err == nil {
		t.Fatal("want error when store is nil")
	}
}

func TestConfigDefaults(t *testing.T) {
	var c Config
	c.applyDefaults()
	if c.Stream != DefaultStream || c.Subject != DefaultSubject || c.Durable != DefaultDurable {
		t.Errorf("defaults not applied: %+v", c)
	}
	if c.MaxAge != DefaultMaxAge || c.AckWait != DefaultAckWait || c.MaxBytes != DefaultMaxBytes {
		t.Errorf("numeric defaults not applied: %+v", c)
	}
	if c.Logger == nil {
		t.Error("logger not defaulted")
	}
}

// Stats must separate the two failure kinds. A dropped batch is a producer
// bug, a failed one is a storage problem, and conflating them hides which.
func TestStatsCountAppendsAndDrops(t *testing.T) {
	nc := startNATS(t)
	rec := &recorder{}

	in, err := New(nc, rec, Config{AckWait: time.Second})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if got := in.Stats(); got.Appended != 0 || got.Dropped != 0 || got.Failed != 0 {
		t.Errorf("fresh stats = %+v, want zeroes", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := in.Run(ctx); err != nil {
			t.Errorf("run: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	waitForStream(t, nc)

	if err := nc.Publish(wire.SubjectPrefix+testInstance, payload(t, 1787284904)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.Publish(wire.SubjectPrefix+testInstance, []byte("{not json")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		got := in.Stats()
		if got.Appended >= 1 && got.Dropped >= 1 {
			if got.LastAppend.IsZero() {
				t.Error("LastAppend not set after a successful append")
			}
			if got.Failed != 0 {
				t.Errorf("Failed = %d, want 0: an undecodable batch is dropped, not failed", got.Failed)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Errorf("stats never reached one append and one drop: %+v", in.Stats())
}

// A store that rejects the append counts as failed, not dropped: the message
// is redelivered rather than discarded.
func TestStatsCountFailedAppends(t *testing.T) {
	nc := startNATS(t)
	rec := &recorder{fail: errors.New("disk on fire")}

	in, err := New(nc, rec, Config{AckWait: time.Second})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := in.Run(ctx); err != nil {
			t.Errorf("run: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	waitForStream(t, nc)

	if err := nc.Publish(wire.SubjectPrefix+testInstance, payload(t, 1787284904)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		got := in.Stats()
		if got.Failed >= 1 {
			if got.Appended != 0 {
				t.Errorf("Appended = %d, want 0", got.Appended)
			}
			if !got.LastAppend.IsZero() {
				t.Error("LastAppend set despite no successful append")
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Errorf("no failure recorded: %+v", in.Stats())
}
