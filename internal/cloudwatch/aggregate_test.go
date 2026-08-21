package cloudwatch

import (
	"errors"
	"testing"
	"time"
)

func wantAPIError(t *testing.T, err error, code string) {
	t.Helper()
	var apiErr *apiError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error = %v, want an apiError with code %s", err, code)
	}
	if apiErr.Code != code {
		t.Errorf("code = %s, want %s", apiErr.Code, code)
	}
}

func TestValidatePeriod(t *testing.T) {
	tests := []struct {
		period int
		ok     bool
	}{
		{60, true}, {300, true}, {86400, true},
		{0, false}, {-60, false},
		// Sub-minute periods are only legal for high-resolution metrics, and
		// nothing goanna stores is one.
		{1, false}, {30, false}, {90, false},
	}
	for _, tt := range tests {
		err := validatePeriod(tt.period)
		if tt.ok && err != nil {
			t.Errorf("period %d: unexpected error %v", tt.period, err)
		}
		if !tt.ok && err == nil {
			t.Errorf("period %d: want rejection", tt.period)
		}
	}
}

func TestValidateStatistics(t *testing.T) {
	if err := validateStatistics([]string{StatAverage, StatSum, StatMinimum, StatMaximum, StatSampleCount}); err != nil {
		t.Fatalf("all five statistics rejected: %v", err)
	}
	// A percentile must fail rather than be dropped: a response that silently
	// omits what was asked for looks complete and is not.
	wantAPIError(t, validateStatistics([]string{"p99"}), "InvalidParameterValue")
	wantAPIError(t, validateStatistics([]string{StatAverage, "average"}), "InvalidParameterValue")
}

func TestAlignDownUsesAbsoluteEpochBoundaries(t *testing.T) {
	const minute = 60_000
	tests := []struct {
		ts   int64
		want int64
	}{
		{0, 0},
		{minute, minute},
		{minute + 1, minute},
		{2*minute - 1, minute},
		// Pre-epoch must floor, not truncate toward zero.
		{-1, -minute},
		{-minute, -minute},
		{-minute - 1, -2 * minute},
	}
	for _, tt := range tests {
		if got := alignDown(tt.ts, minute); got != tt.want {
			t.Errorf("alignDown(%d) = %d, want %d", tt.ts, got, tt.want)
		}
	}
}

// Our samples land on a per-instance stagger offset, so bucketing from the
// request's StartTime would put a different sample count in the first bucket
// than in the rest. CloudWatch buckets on absolute epoch boundaries.
func TestAggregatorBucketsOnEpochNotStartTime(t *testing.T) {
	agg := newAggregator(5 * time.Minute)

	// A staggered 60s series across ten minutes, starting 137s past a
	// five-minute boundary.
	const base = 1_800_000_000_000 // divisible by 300_000
	for i := range 10 {
		agg.add(base+137_000+int64(i)*60_000, 1)
	}

	points := agg.datapoints(UnitCount)
	if len(points) != 3 {
		t.Fatalf("buckets = %d, want 3", len(points))
	}
	for _, p := range points {
		if p.Timestamp.UnixMilli()%300_000 != 0 {
			t.Errorf("bucket %s is not on a five-minute epoch boundary", p.Timestamp)
		}
	}
	// 137s..197s..257s in the first, then five, then the remainder.
	wantCounts := []float64{3, 5, 2}
	for i, p := range points {
		if p.SampleCount != wantCounts[i] {
			t.Errorf("bucket %d sample count = %v, want %v", i, p.SampleCount, wantCounts[i])
		}
	}
}

func TestAggregatorComputesEveryStatistic(t *testing.T) {
	agg := newAggregator(time.Minute)
	for _, v := range []float64{4, 1, 10, 5} {
		agg.add(60_000, v)
	}

	points := agg.datapoints(UnitPercent)
	if len(points) != 1 {
		t.Fatalf("buckets = %d, want 1", len(points))
	}
	p := points[0]

	for _, tc := range []struct {
		stat string
		want float64
	}{
		{StatSampleCount, 4},
		{StatSum, 20},
		{StatMinimum, 1},
		{StatMaximum, 10},
		{StatAverage, 5},
	} {
		got, err := p.statValue(tc.stat)
		if err != nil {
			t.Fatalf("%s: %v", tc.stat, err)
		}
		if got != tc.want {
			t.Errorf("%s = %v, want %v", tc.stat, got, tc.want)
		}
	}
	if p.Unit != UnitPercent {
		t.Errorf("unit = %s, want %s", p.Unit, UnitPercent)
	}
	if _, err := p.statValue("p50"); err == nil {
		t.Error("want an error for an unsupported statistic")
	}
}

// Zero is a real value for a delta metric, so a period with no samples has to
// be absent rather than reported as zero.
func TestAggregatorOmitsEmptyPeriods(t *testing.T) {
	agg := newAggregator(time.Minute)
	agg.add(60_000, 1)
	agg.add(300_000, 0)

	points := agg.datapoints(UnitCount)
	if len(points) != 2 {
		t.Fatalf("buckets = %d, want 2 (the gap omitted)", len(points))
	}
	if !points[0].Timestamp.Before(points[1].Timestamp) {
		t.Error("datapoints must be ascending")
	}
	if points[1].Sum != 0 {
		t.Errorf("a real zero sample was dropped: %v", points[1])
	}
}

func TestCheckDatapointBudget(t *testing.T) {
	start := time.Unix(0, 0)

	if err := checkDatapointBudget(start, start.Add(24*time.Hour), time.Minute); err != nil {
		t.Errorf("1440 datapoints rejected: %v", err)
	}
	err := checkDatapointBudget(start, start.Add(48*time.Hour), time.Minute)
	wantAPIError(t, err, "InvalidParameterCombination")
}
