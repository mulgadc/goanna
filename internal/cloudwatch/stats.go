package cloudwatch

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"time"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// ScanBy orderings for GetMetricData. CloudWatch defaults to descending.
const (
	scanTimestampDescending = "TimestampDescending"
	scanTimestampAscending  = "TimestampAscending"
)

// validQueryID matches CloudWatch's rule for a MetricDataQuery id.
var validQueryID = regexp.MustCompile(`^[a-z][a-zA-Z0-9_]*$`)

// getMetricStatistics aggregates one metric into periods.
func (s *Server) getMetricStatistics(ctx context.Context, form params, identity Identity,
	requestID string) (any, error) {
	start, end, err := form.window("StartTime", "EndTime")
	if err != nil {
		return nil, err
	}

	period, ok, err := form.int("Period")
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, missingParameter("Period")
	}
	if err := validatePeriod(period); err != nil {
		return nil, err
	}
	interval := time.Duration(period) * time.Second
	if err := checkDatapointBudget(start, end, interval); err != nil {
		return nil, err
	}

	// Percentiles need the full sample distribution inside each period. The
	// collector publishes one datapoint per period, so there is no
	// distribution to compute them from.
	if ext := form.strings("ExtendedStatistics"); len(ext) > 0 {
		return nil, invalidParameter("ExtendedStatistics",
			"extended statistics are not supported")
	}
	stats := form.strings("Statistics")
	if len(stats) == 0 {
		return nil, missingParameter("Statistics")
	}
	if err := validateStatistics(stats); err != nil {
		return nil, err
	}

	metricName := form.get("MetricName")
	t, err := resolveTarget(form.get("Namespace"), metricName, form.get("Unit"),
		form.dimensions("Dimensions"))
	if err != nil {
		return nil, err
	}

	resp := getMetricStatisticsResponse{Namespace: xmlNamespace}
	resp.Result.Label = metricName
	resp.Metadata.RequestID = requestID
	if t.empty {
		return resp, nil
	}

	points, err := s.aggregate(ctx, identity, t, start, end, interval)
	if err != nil {
		return nil, err
	}
	resp.Result.Datapoints = toXMLDatapoints(points, stats)
	return resp, nil
}

// metricDataQuery is one member of a GetMetricData request.
type metricDataQuery struct {
	id         string
	label      string
	stat       string
	period     time.Duration
	target     target
	returnData bool
}

// getMetricData answers several metric queries in one round trip.
func (s *Server) getMetricData(ctx context.Context, form params, identity Identity,
	requestID string) (any, error) {
	start, end, err := form.window("StartTime", "EndTime")
	if err != nil {
		return nil, err
	}

	scanBy := form.get("ScanBy")
	switch scanBy {
	case "", scanTimestampAscending, scanTimestampDescending:
	default:
		return nil, invalidParameter("ScanBy", fmt.Sprintf("%q is not a valid ordering", scanBy))
	}

	limit := maxDatapoints
	if v, ok, err := form.int("MaxDatapoints"); err != nil {
		return nil, err
	} else if ok {
		if v <= 0 {
			return nil, invalidParameter("MaxDatapoints", "must be greater than zero")
		}
		limit = min(v, maxDatapoints)
	}

	queries, err := parseMetricDataQueries(form, start, end)
	if err != nil {
		return nil, err
	}

	resp := getMetricDataResponse{Namespace: xmlNamespace}
	resp.Metadata.RequestID = requestID
	for _, q := range queries {
		if !q.returnData {
			continue
		}
		result, err := s.runMetricDataQuery(ctx, identity, q, start, end, scanBy, limit)
		if err != nil {
			return nil, err
		}
		resp.Result.MetricDataResults = append(resp.Result.MetricDataResults, result)
	}
	return resp, nil
}

func (s *Server) runMetricDataQuery(ctx context.Context, identity Identity, q metricDataQuery,
	start, end time.Time, scanBy string, limit int) (xmlMetricDataResult, error) {
	result := xmlMetricDataResult{ID: q.id, Label: q.label, StatusCode: "Complete"}
	if q.target.empty {
		return result, nil
	}

	points, err := s.aggregate(ctx, identity, q.target, start, end, q.period)
	if err != nil {
		return xmlMetricDataResult{}, err
	}
	if scanBy != scanTimestampAscending {
		sort.Slice(points, func(i, j int) bool { return points[i].Timestamp.After(points[j].Timestamp) })
	}
	if len(points) > limit {
		points = points[:limit]
	}

	for _, p := range points {
		value, err := p.statValue(q.stat)
		if err != nil {
			return xmlMetricDataResult{}, err
		}
		result.Timestamps = append(result.Timestamps, p.Timestamp)
		result.Values = append(result.Values, value)
	}
	return result, nil
}

// parseMetricDataQueries decodes the MetricDataQueries list.
func parseMetricDataQueries(form params, start, end time.Time) ([]metricDataQuery, error) {
	const prefix = "MetricDataQueries"
	indexes := form.memberIndexes(prefix)
	if len(indexes) == 0 {
		return nil, missingParameter(prefix)
	}

	seen := make(map[string]struct{}, len(indexes))
	out := make([]metricDataQuery, 0, len(indexes))
	for _, n := range indexes {
		base := form.memberPrefix(prefix, n)

		q, err := parseMetricDataQuery(form, base, start, end)
		if err != nil {
			return nil, err
		}
		if _, dup := seen[q.id]; dup {
			return nil, invalidParameter(base+".Id",
				fmt.Sprintf("id %q is used more than once", q.id))
		}
		seen[q.id] = struct{}{}
		out = append(out, q)
	}
	return out, nil
}

func parseMetricDataQuery(form params, base string, start, end time.Time) (metricDataQuery, error) {
	id := form.get(base + ".Id")
	if id == "" {
		return metricDataQuery{}, missingParameter(base + ".Id")
	}
	if !validQueryID.MatchString(id) {
		return metricDataQuery{}, invalidParameter(base+".Id",
			"an id must start with a lowercase letter and contain only letters, digits and underscores")
	}

	// Metric maths would need an expression evaluator over the results below.
	// The action ships without one rather than accepting an expression and
	// returning something that is not what was asked for.
	if expr := form.get(base + ".Expression"); expr != "" {
		return metricDataQuery{}, invalidParameter(base+".Expression",
			"metric maths expressions are not supported")
	}

	stat := base + ".MetricStat"
	period, ok, err := form.int(stat + ".Period")
	if err != nil {
		return metricDataQuery{}, err
	}
	if !ok {
		return metricDataQuery{}, missingParameter(stat + ".Period")
	}
	if err := validatePeriod(period); err != nil {
		return metricDataQuery{}, err
	}
	interval := time.Duration(period) * time.Second
	if err := checkDatapointBudget(start, end, interval); err != nil {
		return metricDataQuery{}, err
	}

	statistic := form.get(stat + ".Stat")
	if statistic == "" {
		return metricDataQuery{}, missingParameter(stat + ".Stat")
	}
	if err := validateStatistics([]string{statistic}); err != nil {
		return metricDataQuery{}, err
	}

	metricPath := stat + ".Metric"
	metricName := form.get(metricPath + ".MetricName")
	t, err := resolveTarget(form.get(metricPath+".Namespace"), metricName,
		form.get(stat+".Unit"), form.dimensions(metricPath+".Dimensions"))
	if err != nil {
		return metricDataQuery{}, err
	}

	label := form.get(base + ".Label")
	if label == "" {
		label = metricName
	}
	returnData := true
	if v, ok := form.bool(base + ".ReturnData"); ok {
		returnData = v
	}

	return metricDataQuery{
		id: id, label: label, stat: statistic,
		period: interval, target: t, returnData: returnData,
	}, nil
}

// aggregate reads the samples a target selects and buckets them into periods.
func (s *Server) aggregate(ctx context.Context, identity Identity, t target,
	start, end time.Time, period time.Duration) ([]datapoint, error) {
	// CloudWatch treats StartTime as inclusive and EndTime as exclusive; the
	// TSDB's maxt is inclusive, so the window is closed one millisecond early.
	mint := start.UnixMilli()
	maxt := end.UnixMilli() - 1
	if maxt < mint {
		return nil, nil
	}

	q, err := s.querier(identity, mint, maxt)
	if err != nil {
		return nil, err
	}
	defer s.closeQuerier(q)

	agg := newAggregator(period)
	unit := t.unit
	set := q.Select(ctx, false, &storage.SelectHints{Start: mint, End: maxt}, t.matchers...)
	for set.Next() {
		series := set.At()
		if !t.matchesDimensionSet(series.Labels()) {
			continue
		}
		if unit == "" {
			unit = series.Labels().Get(labelUnit)
		}

		it := series.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, value := it.At()
			if ts < mint || ts > maxt {
				continue
			}
			agg.add(ts, value)
		}
		if err := it.Err(); err != nil {
			return nil, internalError(s.log, "aggregate", err)
		}
		if agg.len() > maxDatapoints {
			return nil, senderError("InvalidParameterCombination",
				fmt.Sprintf("The query returned more than %d datapoints. "+
					"Reduce the time range or increase the period.", maxDatapoints),
				http.StatusBadRequest)
		}
	}
	if err := set.Err(); err != nil {
		return nil, internalError(s.log, "aggregate", err)
	}

	if unit == "" {
		unit = UnitNone
	}
	return agg.datapoints(unit), nil
}

// window reads and validates a StartTime/EndTime pair.
func (p params) window(startKey, endKey string) (time.Time, time.Time, error) {
	start, ok, err := p.timestamp(startKey)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	if !ok {
		return time.Time{}, time.Time{}, missingParameter(startKey)
	}

	end, ok, err := p.timestamp(endKey)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	if !ok {
		return time.Time{}, time.Time{}, missingParameter(endKey)
	}

	if !end.After(start) {
		return time.Time{}, time.Time{}, senderError("InvalidParameterCombination",
			"The parameter EndTime must be greater than StartTime.", http.StatusBadRequest)
	}
	return start, end, nil
}
