package cloudwatch

import (
	"encoding/xml"
	"time"
)

// responseMetadata is the trailer every AWS query-protocol response carries.
type responseMetadata struct {
	RequestID string `xml:"RequestId"`
}

type xmlDimension struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

type xmlMetric struct {
	Namespace  string         `xml:"Namespace"`
	MetricName string         `xml:"MetricName"`
	Dimensions []xmlDimension `xml:"Dimensions>member"`
}

type listMetricsResponse struct {
	XMLName   xml.Name `xml:"ListMetricsResponse"`
	Namespace string   `xml:"xmlns,attr"`
	Result    struct {
		Metrics []xmlMetric `xml:"Metrics>member"`
	} `xml:"ListMetricsResult"`
	Metadata responseMetadata `xml:"ResponseMetadata"`
}

// xmlDatapoint omits the statistics the caller did not ask for, which is what
// CloudWatch does — a client that requested Average alone must not have to
// distinguish a real Sum from a zero one.
type xmlDatapoint struct {
	Timestamp   time.Time `xml:"Timestamp"`
	SampleCount *float64  `xml:"SampleCount,omitempty"`
	Average     *float64  `xml:"Average,omitempty"`
	Sum         *float64  `xml:"Sum,omitempty"`
	Minimum     *float64  `xml:"Minimum,omitempty"`
	Maximum     *float64  `xml:"Maximum,omitempty"`
	Unit        string    `xml:"Unit,omitempty"`
}

type getMetricStatisticsResponse struct {
	XMLName   xml.Name `xml:"GetMetricStatisticsResponse"`
	Namespace string   `xml:"xmlns,attr"`
	Result    struct {
		Label      string         `xml:"Label"`
		Datapoints []xmlDatapoint `xml:"Datapoints>member"`
	} `xml:"GetMetricStatisticsResult"`
	Metadata responseMetadata `xml:"ResponseMetadata"`
}

type xmlMetricDataResult struct {
	ID         string      `xml:"Id"`
	Label      string      `xml:"Label"`
	Timestamps []time.Time `xml:"Timestamps>member"`
	Values     []float64   `xml:"Values>member"`
	StatusCode string      `xml:"StatusCode"`
}

type getMetricDataResponse struct {
	XMLName   xml.Name `xml:"GetMetricDataResponse"`
	Namespace string   `xml:"xmlns,attr"`
	Result    struct {
		MetricDataResults []xmlMetricDataResult `xml:"MetricDataResults>member"`
	} `xml:"GetMetricDataResult"`
	Metadata responseMetadata `xml:"ResponseMetadata"`
}

type putMetricDataResponse struct {
	XMLName   xml.Name         `xml:"PutMetricDataResponse"`
	Namespace string           `xml:"xmlns,attr"`
	Metadata  responseMetadata `xml:"ResponseMetadata"`
}

// toXMLDatapoints renders aggregated periods, keeping only the requested
// statistics.
func toXMLDatapoints(points []datapoint, stats []string) []xmlDatapoint {
	want := make(map[string]struct{}, len(stats))
	for _, s := range stats {
		want[s] = struct{}{}
	}

	out := make([]xmlDatapoint, 0, len(points))
	for _, p := range points {
		dp := xmlDatapoint{Timestamp: p.Timestamp, Unit: p.Unit}
		if _, ok := want[StatSampleCount]; ok {
			dp.SampleCount = ptr(p.SampleCount)
		}
		if _, ok := want[StatAverage]; ok {
			dp.Average = ptr(p.Average)
		}
		if _, ok := want[StatSum]; ok {
			dp.Sum = ptr(p.Sum)
		}
		if _, ok := want[StatMinimum]; ok {
			dp.Minimum = ptr(p.Minimum)
		}
		if _, ok := want[StatMaximum]; ok {
			dp.Maximum = ptr(p.Maximum)
		}
		out = append(out, dp)
	}
	return out
}

func ptr[T any](v T) *T { return &v }
