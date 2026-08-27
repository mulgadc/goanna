// Package store wraps the embedded Prometheus TSDB that holds tenant
// metrics.
package store

import (
	"context"
	"errors"
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
		if _, err := app.Append(0, seriesLabels(instanceID, series), b.TimestampMS(), series.Value); err != nil {
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

// Querier returns an unscoped querier over the given millisecond range. The
// caller closes it.
//
// This reaches every tenant's series. The CloudWatch API must not use it —
// see TenantQuerier.
func (s *Store) Querier(mint, maxt int64) (storage.Querier, error) {
	q, err := s.db.Querier(mint, maxt)
	if err != nil {
		return nil, fmt.Errorf("store: querier: %w", err)
	}
	return q, nil
}

// LabelAccountID is the tenant boundary. It is written from the publisher's
// account and, on the read path, only ever from a verified signature.
const LabelAccountID = "account_id"

// ErrNoAccount is returned when a scoped operation is attempted without an
// account. An empty account_id matcher would select every series that carries
// no account label at all, so it can never be treated as "unset".
var ErrNoAccount = errors.New("store: account id is required")

// TenantQuerier is a querier that can only see one account's series.
//
// The CloudWatch handlers are written against this type rather than
// storage.Querier so there is no code path from a handler to an unscoped read.
// Prometheus ANDs matchers, so the scope below is a ceiling: no combination of
// caller-supplied matchers can widen it.
type TenantQuerier struct {
	q     storage.Querier
	scope *labels.Matcher
}

// TenantQuerier opens a querier scoped to accountID over the millisecond range
// [mint, maxt]. The caller closes it.
func (s *Store) TenantQuerier(accountID string, mint, maxt int64) (*TenantQuerier, error) {
	if accountID == "" {
		return nil, ErrNoAccount
	}
	scope, err := labels.NewMatcher(labels.MatchEqual, LabelAccountID, accountID)
	if err != nil {
		return nil, fmt.Errorf("store: scope matcher: %w", err)
	}
	q, err := s.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &TenantQuerier{q: q, scope: scope}, nil
}

// Select runs a query with the account scope prepended to the caller's
// matchers.
func (t *TenantQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints,
	matchers ...*labels.Matcher) storage.SeriesSet {
	scoped := make([]*labels.Matcher, 0, len(matchers)+1)
	scoped = append(scoped, t.scope)
	scoped = append(scoped, matchers...)
	return t.q.Select(ctx, sortSeries, hints, scoped...)
}

// Close releases the underlying querier.
func (t *TenantQuerier) Close() error {
	if err := t.q.Close(); err != nil {
		return fmt.Errorf("store: close tenant querier: %w", err)
	}
	return nil
}

// Point is one sample to append on behalf of a tenant.
type Point struct {
	Name      string
	Labels    map[string]string
	Timestamp int64 // milliseconds
	Value     float64
}

// AppendTenant writes points on behalf of accountID, stamping the account
// label itself. Any account_id in a point's labels is discarded, so a caller
// cannot file samples under another tenant.
func (s *Store) AppendTenant(ctx context.Context, accountID string, points []Point) error {
	if accountID == "" {
		return ErrNoAccount
	}

	app := s.db.Appender(ctx)
	for _, p := range points {
		if p.Name == "" {
			continue
		}
		b := labels.NewBuilder(labels.EmptyLabels())
		for name, value := range p.Labels {
			if value != "" {
				b.Set(name, value)
			}
		}
		b.Set(model.MetricNameLabel, p.Name)
		b.Set(LabelAccountID, accountID)

		if _, err := app.Append(0, b.Labels(), p.Timestamp, p.Value); err != nil {
			if rbErr := app.Rollback(); rbErr != nil {
				s.log.Warn("rollback after failed tenant append", "error", rbErr)
			}
			return fmt.Errorf("store: append %s: %w", p.Name, err)
		}
	}
	if err := app.Commit(); err != nil {
		return fmt.Errorf("store: commit tenant points: %w", err)
	}
	return nil
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
