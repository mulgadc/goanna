// Package store wraps the embedded Prometheus TSDB that holds tenant
// metrics.
package store

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/mulgadc/goanna/internal/wire"
)

// Options configures a Store. The zero value is not usable: Dir is required.
type Options struct {
	Dir string
	// RetentionMS bounds the hot tier. Blocks older than this are dropped
	// locally; the cold tier on predastore is a later phase, so until it
	// exists this is also the total retention.
	RetentionMS int64
	Logger      *slog.Logger
}

// Store appends metric batches to a local TSDB.
type Store struct {
	db  *tsdb.DB
	log *slog.Logger
}

// Open creates or reopens the TSDB under opts.Dir.
func Open(opts Options) (*Store, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("store: Dir is required")
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	tsdbOpts := tsdb.DefaultOptions()
	if opts.RetentionMS > 0 {
		tsdbOpts.RetentionDuration = opts.RetentionMS
	}

	db, err := tsdb.Open(opts.Dir, log, nil, tsdbOpts, nil)
	if err != nil {
		return nil, fmt.Errorf("store: open tsdb at %s: %w", opts.Dir, err)
	}
	return &Store{db: db, log: log}, nil
}

// Append writes every series in one batch under a single appender, so the
// batch lands whole or not at all. A partial append would leave the caller
// unable to retry without double-counting.
func (s *Store) Append(ctx context.Context, instanceID string, b wire.Batch) error {
	if err := b.Validate(); err != nil {
		return fmt.Errorf("store: %w", err)
	}

	app := s.db.Appender(ctx)
	for _, series := range b.Series {
		// The appender accepts an empty __name__, which stores a series
		// nothing can query. Skip it rather than fail the batch: the other
		// series in it are fine and a producer bug should not lose them.
		if series.Name == "" {
			s.log.Warn("skipping unnamed series", "instance_id", instanceID)
			continue
		}
		if _, err := app.Append(0, seriesLabels(instanceID, series), b.TS, series.Value); err != nil {
			// Rollback so a failed batch leaves nothing behind for the
			// redelivery to duplicate.
			if rbErr := app.Rollback(); rbErr != nil {
				s.log.Warn("rollback after failed append", "error", rbErr)
			}
			return fmt.Errorf("store: append %s: %w", series.Name, err)
		}
	}
	if err := app.Commit(); err != nil {
		return fmt.Errorf("store: commit batch: %w", err)
	}
	return nil
}

// seriesLabels builds the label set a series is stored under.
//
// The subject's instance id wins over any instance_id label: the collector
// publishes one instance per subject, so the subject is authoritative and a
// disagreeing label would file a guest's metrics under another guest.
func seriesLabels(instanceID string, s wire.Series) labels.Labels {
	b := labels.NewBuilder(labels.EmptyLabels())
	for name, value := range s.Labels {
		if value != "" {
			b.Set(name, value)
		}
	}
	b.Set(model.MetricNameLabel, s.Name)
	if instanceID != "" {
		b.Set("instance_id", instanceID)
	}
	return b.Labels()
}

// Querier returns a querier over the given millisecond range. The caller
// closes it.
func (s *Store) Querier(mint, maxt int64) (storage.Querier, error) {
	q, err := s.db.Querier(mint, maxt)
	if err != nil {
		return nil, fmt.Errorf("store: querier: %w", err)
	}
	return q, nil
}

// Ready reports whether the TSDB can still be read. It opens and closes a
// querier over an empty range, which touches the block list without scanning
// any samples.
func (s *Store) Ready() error {
	q, err := s.Querier(0, 0)
	if err != nil {
		return err
	}
	if err := q.Close(); err != nil {
		return fmt.Errorf("store: close readiness querier: %w", err)
	}
	return nil
}

// Stats reports what the head currently holds, for the status endpoint.
func (s *Store) Stats() map[string]any {
	head := s.db.Head()
	return map[string]any{
		"series":   head.NumSeries(),
		"min_time": head.MinTime(),
		"max_time": head.MaxTime(),
		"blocks":   len(s.db.Blocks()),
	}
}

// Close flushes and closes the TSDB.
func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("store: close: %w", err)
	}
	return nil
}
