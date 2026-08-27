package cloudwatch

import "time"

// The JSON protocol answers with the result members directly, without the
// Response/Result envelope and ResponseMetadata the query protocol wraps them
// in. Timestamps go out as epoch seconds, which is what AWS JSON uses.

type jsonDimension struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

type jsonMetric struct {
	Namespace  string          `json:"Namespace"`
	MetricName string          `json:"MetricName"`
	Dimensions []jsonDimension `json:"Dimensions"`
}

type jsonListMetrics struct {
	Metrics []jsonMetric `json:"Metrics"`
}

type jsonDatapoint struct {
	Timestamp   float64  `json:"Timestamp"`
	SampleCount *float64 `json:"SampleCount,omitempty"`
	Average     *float64 `json:"Average,omitempty"`
	Sum         *float64 `json:"Sum,omitempty"`
	Minimum     *float64 `json:"Minimum,omitempty"`
	Maximum     *float64 `json:"Maximum,omitempty"`
	Unit        string   `json:"Unit,omitempty"`
}

type jsonGetMetricStatistics struct {
	Label      string          `json:"Label"`
	Datapoints []jsonDatapoint `json:"Datapoints"`
}

type jsonMetricDataResult struct {
	ID         string    `json:"Id"`
	Label      string    `json:"Label"`
	Timestamps []float64 `json:"Timestamps"`
	Values     []float64 `json:"Values"`
	StatusCode string    `json:"StatusCode"`
}

type jsonGetMetricData struct {
	MetricDataResults []jsonMetricDataResult `json:"MetricDataResults"`
	Messages          []struct{}             `json:"Messages"`
}

// epochSeconds renders a timestamp the way AWS JSON does, with millisecond
// resolution preserved as a fraction.
func epochSeconds(t time.Time) float64 {
	return float64(t.UnixMilli()) / 1000
}

// toJSONResponse converts a query-protocol document into its JSON equivalent.
// Handlers build one shape and this maps it, so the two protocols cannot drift
// apart in what they report.
func toJSONResponse(doc any) any {
	switch v := doc.(type) {
	case listMetricsResponse:
		return jsonListMetrics{Metrics: toJSONMetrics(v.Result.Metrics)}
	case getMetricStatisticsResponse:
		return jsonGetMetricStatistics{
			Label:      v.Result.Label,
			Datapoints: toJSONDatapoints(v.Result.Datapoints),
		}
	case getMetricDataResponse:
		return jsonGetMetricData{
			MetricDataResults: toJSONMetricDataResults(v.Result.MetricDataResults),
			Messages:          []struct{}{},
		}
	case putMetricDataResponse:
		return struct{}{}
	default:
		return struct{}{}
	}
}

func toJSONMetrics(metrics []xmlMetric) []jsonMetric {
	out := make([]jsonMetric, 0, len(metrics))
	for _, m := range metrics {
		dims := make([]jsonDimension, 0, len(m.Dimensions))
		for _, d := range m.Dimensions {
			dims = append(dims, jsonDimension(d))
		}
		out = append(out, jsonMetric{
			Namespace:  m.Namespace,
			MetricName: m.MetricName,
			Dimensions: dims,
		})
	}
	return out
}

func toJSONDatapoints(points []xmlDatapoint) []jsonDatapoint {
	out := make([]jsonDatapoint, 0, len(points))
	for _, p := range points {
		out = append(out, jsonDatapoint{
			Timestamp:   epochSeconds(p.Timestamp),
			SampleCount: p.SampleCount,
			Average:     p.Average,
			Sum:         p.Sum,
			Minimum:     p.Minimum,
			Maximum:     p.Maximum,
			Unit:        p.Unit,
		})
	}
	return out
}

func toJSONMetricDataResults(results []xmlMetricDataResult) []jsonMetricDataResult {
	out := make([]jsonMetricDataResult, 0, len(results))
	for _, r := range results {
		stamps := make([]float64, 0, len(r.Timestamps))
		for _, t := range r.Timestamps {
			stamps = append(stamps, epochSeconds(t))
		}
		values := r.Values
		if values == nil {
			values = []float64{}
		}
		out = append(out, jsonMetricDataResult{
			ID:         r.ID,
			Label:      r.Label,
			Timestamps: stamps,
			Values:     values,
			StatusCode: r.StatusCode,
		})
	}
	return out
}
