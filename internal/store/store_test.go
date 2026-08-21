package store

import (
	"context"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/mulgadc/goanna/internal/wire"
)

const testInstance = "i-0abc123def4567890"

// The wire carries seconds; the TSDB indexes milliseconds.
const tsMS = 1787284904 * 1000

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return s
}

func testBatch(ts int64) wire.Batch {
	l := map[string]string{
		"namespace":   "AWS/EC2",
		"instance_id": testInstance,
		"account_id":  "123456789012",
	}
	return wire.Batch{
		TS:            ts,
		PeriodSeconds: 60,
		Node:          "env13-ironbark",
		Series: []wire.Series{
			{Name: "goanna_ec2_cpu_utilization", Labels: l, Value: 12.5, Unit: "Percent"},
			{Name: "goanna_ec2_network_in_bytes", Labels: l, Value: 4096, Unit: "Bytes"},
		},
	}
}

// collect reads every sample back out through the TSDB's own querier, so
// the test asserts what a reader sees rather than what the appender was told.
func collect(t *testing.T, s *Store, mint, maxt int64) map[string][]float64 {
	t.Helper()
	q, err := s.Querier(mint, maxt)
	if err != nil {
		t.Fatalf("querier: %v", err)
	}
	defer func() {
		if err := q.Close(); err != nil {
			t.Errorf("close querier: %v", err)
		}
	}()

	out := map[string][]float64{}
	set := q.Select(t.Context(), false, nil,
		labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, "goanna_ec2_.*"))
	for set.Next() {
		series := set.At()
		it := series.Iterator(nil)
		key := series.Labels().String()
		for it.Next() != 0 {
			_, v := it.At()
			out[key] = append(out[key], v)
		}
		if err := it.Err(); err != nil {
			t.Fatalf("iterate: %v", err)
		}
	}
	if err := set.Err(); err != nil {
		t.Fatalf("select: %v", err)
	}
	return out
}

func TestAppendRoundTrips(t *testing.T) {
	s := openTestStore(t)
	const ts = 1787284904 // Unix seconds, as the producer emits

	if err := s.Append(context.Background(), testInstance, testBatch(ts)); err != nil {
		t.Fatalf("append: %v", err)
	}

	got := collect(t, s, tsMS-1000, tsMS+1000)
	if len(got) != 2 {
		t.Fatalf("series stored = %d, want 2: %v", len(got), got)
	}

	var found bool
	for key, values := range got {
		if len(values) != 1 {
			t.Errorf("%s: samples = %d, want 1", key, len(values))
		}
		if values[0] == 12.5 {
			found = true
		}
	}
	if !found {
		t.Errorf("cpu utilization sample not returned: %v", got)
	}
}

func TestAppendStoresEveryLabel(t *testing.T) {
	s := openTestStore(t)
	const ts = 1787284904 // Unix seconds, as the producer emits

	if err := s.Append(context.Background(), testInstance, testBatch(ts)); err != nil {
		t.Fatalf("append: %v", err)
	}

	for key := range collect(t, s, tsMS-1000, tsMS+1000) {
		for _, want := range []string{"namespace=", "account_id=", "instance_id=", "__name__="} {
			if !contains(key, want) {
				t.Errorf("label %q missing from %s", want, key)
			}
		}
	}
}

// The subject names the instance. A label claiming a different one must not
// win, or one guest's metrics land under another's id.
func TestSubjectInstanceIDOverridesLabel(t *testing.T) {
	s := openTestStore(t)
	const ts = 1787284904 // Unix seconds, as the producer emits

	b := testBatch(ts)
	b.Series[0].Labels = map[string]string{"instance_id": "i-someone-else"}
	if err := s.Append(context.Background(), testInstance, b); err != nil {
		t.Fatalf("append: %v", err)
	}

	for key := range collect(t, s, tsMS-1000, tsMS+1000) {
		if contains(key, "i-someone-else") {
			t.Errorf("label overrode the subject's instance id: %s", key)
		}
	}
}

func TestAppendRejectsInvalidBatch(t *testing.T) {
	s := openTestStore(t)
	if err := s.Append(context.Background(), testInstance, wire.Batch{}); err == nil {
		t.Fatal("want error for a batch with no timestamp")
	}
}

func TestAppendIsAtomic(t *testing.T) {
	s := openTestStore(t)
	const ts = 1787284904 // Unix seconds, as the producer emits

	// Land a batch, then send a second one whose last series contradicts a
	// committed sample. The appender refuses that, and the new series ahead
	// of it in the same batch must go back with it.
	if err := s.Append(context.Background(), testInstance, testBatch(ts)); err != nil {
		t.Fatalf("first append: %v", err)
	}

	conflicting := testBatch(ts)
	conflicting.Series = []wire.Series{
		{Name: "goanna_ec2_disk_read_ops", Labels: testBatch(ts).Series[0].Labels, Value: 7},
		{Name: "goanna_ec2_cpu_utilization", Labels: testBatch(ts).Series[0].Labels, Value: 99.9},
	}
	if err := s.Append(context.Background(), testInstance, conflicting); err == nil {
		t.Fatal("want error for a sample contradicting a committed one")
	}

	for key, values := range collect(t, s, tsMS-1000, tsMS+1000) {
		if contains(key, "disk_read_ops") {
			t.Errorf("rolled-back batch left %s behind", key)
		}
		if contains(key, "cpu_utilization") && values[0] != 12.5 {
			t.Errorf("committed sample overwritten: %v", values)
		}
	}
}

// An unnamed series is unqueryable, but it must not cost the batch the
// series that were named.
func TestAppendSkipsUnnamedSeries(t *testing.T) {
	s := openTestStore(t)
	const ts = 1787284904 // Unix seconds, as the producer emits

	b := testBatch(ts)
	b.Series[1].Name = ""
	if err := s.Append(context.Background(), testInstance, b); err != nil {
		t.Fatalf("append: %v", err)
	}

	got := collect(t, s, tsMS-1000, tsMS+1000)
	if len(got) != 1 {
		t.Errorf("series stored = %d, want 1 (the named one): %v", len(got), got)
	}
}

func TestOpenRequiresDir(t *testing.T) {
	if _, err := Open(Options{}); err == nil {
		t.Fatal("want error when Dir is empty")
	}
}

func TestReopenKeepsData(t *testing.T) {
	dir := t.TempDir()
	const ts = 1787284904 // Unix seconds, as the producer emits

	first, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := first.Append(context.Background(), testInstance, testBatch(ts)); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	second, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if err := second.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	if got := collect(t, second, tsMS-1000, tsMS+1000); len(got) != 2 {
		t.Errorf("series after reopen = %d, want 2", len(got))
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func TestReady(t *testing.T) {
	s := openTestStore(t)
	if err := s.Ready(); err != nil {
		t.Errorf("Ready() on an open store: %v", err)
	}
}

func TestStatsCountsSeries(t *testing.T) {
	s := openTestStore(t)
	const ts = 1787284904 // Unix seconds, as the producer emits

	if got := s.Stats()["series"]; got != uint64(0) {
		t.Errorf("series before any append = %v, want 0", got)
	}
	if err := s.Append(context.Background(), testInstance, testBatch(ts)); err != nil {
		t.Fatalf("append: %v", err)
	}

	stats := s.Stats()
	if stats["series"] != uint64(2) {
		t.Errorf("series = %v, want 2", stats["series"])
	}
	if stats["max_time"] != int64(tsMS) {
		t.Errorf("max_time = %v, want %d", stats["max_time"], tsMS)
	}
	if _, ok := stats["blocks"]; !ok {
		t.Error("blocks missing")
	}
}
