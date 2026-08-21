// Package ingest consumes per-VM metric batches from NATS JetStream and
// hands them to the store.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mulgadc/goanna/internal/wire"
)

// Defaults for the stream Goanna owns. The producer publishes with core
// NATS, so the stream is what makes the data durable: JetStream captures a
// message by subject regardless of which API published it, which is why
// this needs no change on the spinifex side.
const (
	DefaultStream   = "GOANNA_METRICS"
	DefaultSubject  = "metrics.ec2.>"
	DefaultDurable  = "goanna-ingest"
	DefaultMaxAge   = 24 * time.Hour
	DefaultAckWait  = 30 * time.Second
	DefaultMaxBytes = 1 << 30
)

// Appender is the store side of ingest, narrowed to what ingest uses.
type Appender interface {
	Append(ctx context.Context, instanceID string, b wire.Batch) error
}

// Config configures an Ingest.
type Config struct {
	Stream   string
	Subject  string
	Durable  string
	MaxAge   time.Duration
	MaxBytes int64
	AckWait  time.Duration
	Logger   *slog.Logger
}

func (c *Config) applyDefaults() {
	if c.Stream == "" {
		c.Stream = DefaultStream
	}
	if c.Subject == "" {
		c.Subject = DefaultSubject
	}
	if c.Durable == "" {
		c.Durable = DefaultDurable
	}
	if c.MaxAge == 0 {
		c.MaxAge = DefaultMaxAge
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = DefaultMaxBytes
	}
	if c.AckWait == 0 {
		c.AckWait = DefaultAckWait
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Stats is a snapshot of what the consumer has done since it started.
type Stats struct {
	Appended   uint64    `json:"appended"`
	Dropped    uint64    `json:"dropped"`
	Failed     uint64    `json:"failed"`
	LastAppend time.Time `json:"last_append,omitzero"`
}

// Ingest runs the durable consumer.
type Ingest struct {
	cfg   Config
	store Appender
	js    jetstream.JetStream

	mu    sync.Mutex
	stats Stats
}

// Stats returns a snapshot of the counters. Safe to call from another
// goroutine while the consumer is running.
func (i *Ingest) Stats() Stats {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.stats
}

// New builds an Ingest over an existing NATS connection.
func New(nc *nats.Conn, store Appender, cfg Config) (*Ingest, error) {
	if store == nil {
		return nil, errors.New("ingest: store is required")
	}
	cfg.applyDefaults()

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("ingest: jetstream: %w", err)
	}
	return &Ingest{cfg: cfg, store: store, js: js}, nil
}

// Run creates the stream and consumer if they do not exist, then consumes
// until ctx is cancelled.
func (i *Ingest) Run(ctx context.Context) error {
	stream, err := i.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      i.cfg.Stream,
		Subjects:  []string{i.cfg.Subject},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    i.cfg.MaxAge,
		MaxBytes:  i.cfg.MaxBytes,
		Discard:   jetstream.DiscardOld,
	})
	if err != nil {
		return fmt.Errorf("ingest: create stream %s: %w", i.cfg.Stream, err)
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       i.cfg.Durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       i.cfg.AckWait,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return fmt.Errorf("ingest: create consumer %s: %w", i.cfg.Durable, err)
	}

	consuming, err := consumer.Consume(func(msg jetstream.Msg) {
		i.handle(ctx, msg)
	})
	if err != nil {
		return fmt.Errorf("ingest: consume: %w", err)
	}
	defer consuming.Stop()

	<-ctx.Done()
	return nil
}

// handle appends one message and settles it.
//
// The two failure kinds are settled differently on purpose. A payload that
// does not decode will never decode, so redelivering it forever would wedge
// the consumer behind one bad message — those are terminated. A failed
// append may well succeed next time, so those are negatively acknowledged
// and redelivered.
func (i *Ingest) handle(ctx context.Context, msg jetstream.Msg) {
	log := i.cfg.Logger.With("subject", msg.Subject())

	instanceID, ok := wire.InstanceIDFromSubject(msg.Subject())
	if !ok {
		log.Warn("dropping message on an unrecognised subject")
		i.record(func(s *Stats) { s.Dropped++ })
		i.terminate(log, msg)
		return
	}

	batch, err := wire.Decode(msg.Data())
	if err != nil {
		log.Warn("dropping undecodable batch", "error", err)
		i.record(func(s *Stats) { s.Dropped++ })
		i.terminate(log, msg)
		return
	}
	if err := batch.Validate(); err != nil {
		log.Warn("dropping empty batch", "error", err)
		i.record(func(s *Stats) { s.Dropped++ })
		i.terminate(log, msg)
		return
	}

	if err := i.store.Append(ctx, instanceID, batch); err != nil {
		log.Error("append failed, will redeliver", "instance_id", instanceID, "error", err)
		i.record(func(s *Stats) { s.Failed++ })
		if nakErr := msg.Nak(); nakErr != nil {
			log.Error("nak failed", "error", nakErr)
		}
		return
	}

	i.record(func(s *Stats) {
		s.Appended++
		s.LastAppend = time.Now().UTC()
	})

	if err := msg.Ack(); err != nil {
		log.Error("ack failed", "instance_id", instanceID, "error", err)
	}
}

func (i *Ingest) record(f func(*Stats)) {
	i.mu.Lock()
	defer i.mu.Unlock()
	f(&i.stats)
}

func (i *Ingest) terminate(log *slog.Logger, msg jetstream.Msg) {
	if err := msg.Term(); err != nil {
		log.Error("terminate failed", "error", err)
	}
}
