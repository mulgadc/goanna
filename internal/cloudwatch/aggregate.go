package cloudwatch

import (
	"fmt"
	"net/http"
	"sort"
	"time"
)

// The five statistics CloudWatch computes from raw datapoints. Extended
// statistics (percentiles) need the full sample distribution per period, which
// a one-datapoint-per-period producer cannot supply.
const (
	StatSampleCount = "SampleCount"
	StatAverage     = "Average"
	StatSum         = "Sum"
	StatMinimum     = "Minimum"
	StatMaximum     = "Maximum"
)

// maxDatapoints is CloudWatch's per-request datapoint cap. It also bounds how
// much a single query can make the server hold.
const maxDatapoints = 1440

var validStatistics = map[string]struct{}{
	StatSampleCount: {}, StatAverage: {}, StatSum: {}, StatMinimum: {}, StatMaximum: {},
}

// validateStatistics rejects anything outside the supported five. An unknown
// statistic must fail rather than be silently dropped: a caller asking for p99
// would otherwise get a response that looks complete and is not.
func validateStatistics(stats []string) error {
	for _, s := range stats {
		if _, ok := validStatistics[s]; !ok {
			return invalidParameter("Statistics",
				fmt.Sprintf("%q is not a supported statistic", s))
		}
	}
	return nil
}

// validatePeriod enforces CloudWatch's standard-resolution rule. Every series
// goanna stores is standard resolution, so the sub-minute periods AWS allows
// for high-resolution metrics are rejected rather than silently rounded.
func validatePeriod(period int) error {
	if period <= 0 {
		return invalidParameter("Period", "period must be greater than zero")
	}
	if period%60 != 0 {
		return invalidParameter("Period", "period must be a multiple of 60 seconds")
	}
	return nil
}

// datapoint is one aggregated period.
type datapoint struct {
	Timestamp   time.Time
	SampleCount float64
	Sum         float64
	Minimum     float64
	Maximum     float64
	Average     float64
	Unit        string
}

// bucket accumulates the samples that fall in one period.
type bucket struct {
	count float64
	sum   float64
	min   float64
	max   float64
}

// aggregator buckets samples by period.
//
// Buckets align to absolute epoch boundaries, which is what CloudWatch does.
// Aligning to the request's StartTime instead would put a different number of
// samples in the first bucket than in the rest — our samples land on a
// per-instance stagger offset — and report a wrong SampleCount for it.
type aggregator struct {
	periodMS int64
	buckets  map[int64]*bucket
}

func newAggregator(period time.Duration) *aggregator {
	return &aggregator{periodMS: period.Milliseconds(), buckets: map[int64]*bucket{}}
}

// alignDown returns the start of the period containing tsMS. Go's integer
// division truncates toward zero, so pre-1970 timestamps need the floor
// correction; the TSDB will not hold any, but the arithmetic should not depend
// on that.
func alignDown(tsMS, periodMS int64) int64 {
	q := tsMS / periodMS
	if tsMS < 0 && q*periodMS != tsMS {
		q--
	}
	return q * periodMS
}

func (a *aggregator) add(tsMS int64, value float64) {
	key := alignDown(tsMS, a.periodMS)
	b, ok := a.buckets[key]
	if !ok {
		a.buckets[key] = &bucket{count: 1, sum: value, min: value, max: value}
		return
	}
	b.count++
	b.sum += value
	if value < b.min {
		b.min = value
	}
	if value > b.max {
		b.max = value
	}
}

// len reports how many non-empty periods have been accumulated.
func (a *aggregator) len() int { return len(a.buckets) }

// datapoints returns the aggregated periods in ascending time order. Empty
// periods are omitted rather than reported as zero: zero is a real value for a
// delta metric and "no data" is not.
func (a *aggregator) datapoints(unit string) []datapoint {
	out := make([]datapoint, 0, len(a.buckets))
	for start, b := range a.buckets {
		out = append(out, datapoint{
			Timestamp:   time.UnixMilli(start).UTC(),
			SampleCount: b.count,
			Sum:         b.sum,
			Minimum:     b.min,
			Maximum:     b.max,
			Average:     b.sum / b.count,
			Unit:        unit,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.Before(out[j].Timestamp) })
	return out
}

// statValue projects a datapoint onto one statistic, for GetMetricData which
// returns a single series per query rather than all five.
func (d datapoint) statValue(stat string) (float64, error) {
	switch stat {
	case StatSampleCount:
		return d.SampleCount, nil
	case StatAverage:
		return d.Average, nil
	case StatSum:
		return d.Sum, nil
	case StatMinimum:
		return d.Minimum, nil
	case StatMaximum:
		return d.Maximum, nil
	default:
		return 0, invalidParameter("Stat", fmt.Sprintf("%q is not a supported statistic", stat))
	}
}

// checkDatapointBudget rejects a window/period combination that would exceed
// the response cap, the way CloudWatch does, instead of truncating silently.
func checkDatapointBudget(start, end time.Time, period time.Duration) error {
	if period <= 0 {
		return nil
	}
	if periods := int64(end.Sub(start) / period); periods > maxDatapoints {
		return senderError("InvalidParameterCombination",
			fmt.Sprintf("The requested time range and period would return %d datapoints, "+
				"which exceeds the maximum of %d. Reduce the range or increase the period.",
				periods, maxDatapoints), http.StatusBadRequest)
	}
	return nil
}
