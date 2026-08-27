package cloudwatch

import (
	"net/http"
	"net/url"
	"testing"
	"time"
)

func putQuery(extra ...string) url.Values {
	v := url.Values{
		"Action":                                        {"PutMetricData"},
		"Namespace":                                     {"Tenant/App"},
		"MetricData.member.1.MetricName":                {"Latency"},
		"MetricData.member.1.Value":                     {"12.5"},
		"MetricData.member.1.Unit":                      {"Milliseconds"},
		"MetricData.member.1.Timestamp":                 {seedStart.Format(time.RFC3339)},
		"MetricData.member.1.Dimensions.member.1.Name":  {"Service"},
		"MetricData.member.1.Dimensions.member.1.Value": {"api"},
	}
	for i := 0; i+1 < len(extra); i += 2 {
		v.Set(extra[i], extra[i+1])
	}
	return v
}

func TestPutMetricDataRoundTrips(t *testing.T) {
	f := newFixture(t)

	var put putMetricDataResponse
	status, body := f.call(t, testAccountA, putQuery())
	decode(t, status, body, &put)

	var list listMetricsResponse
	status, body = f.call(t, testAccountA, url.Values{
		"Action": {"ListMetrics"}, "Namespace": {"Tenant/App"},
	})
	decode(t, status, body, &list)
	if len(list.Result.Metrics) != 1 {
		t.Fatalf("metrics = %v, want the one just written", list.Result.Metrics)
	}
	m := list.Result.Metrics[0]
	if m.Namespace != "Tenant/App" || m.MetricName != "Latency" {
		t.Errorf("metric = %s/%s", m.Namespace, m.MetricName)
	}
	if len(m.Dimensions) != 1 || m.Dimensions[0].Name != "Service" || m.Dimensions[0].Value != "api" {
		t.Errorf("dimensions = %v", m.Dimensions)
	}

	var stats getMetricStatisticsResponse
	status, body = f.call(t, testAccountA, url.Values{
		"Action":                    {"GetMetricStatistics"},
		"Namespace":                 {"Tenant/App"},
		"MetricName":                {"Latency"},
		"Dimensions.member.1.Name":  {"Service"},
		"Dimensions.member.1.Value": {"api"},
		"StartTime":                 {seedStart.Format(time.RFC3339)},
		"EndTime":                   {testNow.Format(time.RFC3339)},
		"Period":                    {"300"},
		"Statistics.member.1":       {StatSum},
	})
	decode(t, status, body, &stats)
	if len(stats.Result.Datapoints) != 1 {
		t.Fatalf("datapoints = %v, want 1", stats.Result.Datapoints)
	}
	if got := stats.Result.Datapoints[0]; got.Sum == nil || *got.Sum != 12.5 {
		t.Errorf("sum = %v, want 12.5", got.Sum)
	}
	if got := stats.Result.Datapoints[0].Unit; got != "Milliseconds" {
		t.Errorf("unit = %s, want the one it was published with", got)
	}
}

// Without this a tenant could inject fabricated AWS/EC2 datapoints for their
// own instances that nothing downstream could tell from collector data.
func TestPutMetricDataRejectsReservedNamespaces(t *testing.T) {
	f := newFixture(t)

	for _, ns := range []string{NamespaceEC2, "AWS/Lambda", NamespaceGoannaEC2} {
		t.Run(ns, func(t *testing.T) {
			status, body := f.call(t, testAccountA, putQuery("Namespace", ns))
			if status != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", status)
			}
			if got := errorCode(t, body); got != "InvalidParameterValue" {
				t.Errorf("code = %s, want InvalidParameterValue", got)
			}
		})
	}
}

// A forged AWS/EC2 datapoint must be impossible even by another route: the
// projection reads only the collector's series names, and nothing a caller
// sends can produce one.
func TestPutMetricDataCannotForgeACollectorSeries(t *testing.T) {
	f := newFixture(t)

	status, _ := f.call(t, testAccountA, putQuery(
		"Namespace", "Tenant/Evil",
		"MetricData.member.1.MetricName", "goanna_ec2_cpu_utilization",
	))
	if status != http.StatusOK {
		t.Fatalf("status = %d; the write itself is legitimate", status)
	}

	var resp getMetricStatisticsResponse
	status, body := f.call(t, testAccountA, statsQuery(testInstanceA))
	decode(t, status, body, &resp)
	if len(resp.Result.Datapoints) != 0 {
		t.Errorf("a tenant write surfaced as AWS/EC2 CPUUtilization: %v", resp.Result.Datapoints)
	}
}

func TestPutMetricDataValidation(t *testing.T) {
	f := newFixture(t)

	tests := []struct {
		name    string
		mutate  func(url.Values)
		wantErr string
	}{
		{"no namespace", func(v url.Values) { v.Del("Namespace") }, "MissingParameter"},
		{"no metric data", func(v url.Values) {
			for key := range v {
				if key != "Action" && key != "Namespace" {
					v.Del(key)
				}
			}
		}, "MissingParameter"},
		{"no metric name", func(v url.Values) { v.Del("MetricData.member.1.MetricName") }, "MissingParameter"},
		{"no value", func(v url.Values) { v.Del("MetricData.member.1.Value") }, "MissingParameter"},
		{"a non-numeric value", func(v url.Values) {
			v.Set("MetricData.member.1.Value", "high")
		}, "InvalidParameterValue"},
		{"an infinite value", func(v url.Values) {
			v.Set("MetricData.member.1.Value", "Inf")
		}, "InvalidParameterValue"},
		{"a statistic set", func(v url.Values) {
			v.Set("MetricData.member.1.StatisticValues.Sum", "10")
		}, "InvalidParameterValue"},
		{"a value array", func(v url.Values) {
			v.Set("MetricData.member.1.Values.member.1", "10")
		}, "InvalidParameterValue"},
		{"a dimension name a label cannot hold", func(v url.Values) {
			v.Set("MetricData.member.1.Dimensions.member.1.Name", "my-service")
		}, "InvalidParameterValue"},
		{"a dimension with no value", func(v url.Values) {
			v.Del("MetricData.member.1.Dimensions.member.1.Value")
		}, "InvalidParameterValue"},
		{"a timestamp from last year", func(v url.Values) {
			v.Set("MetricData.member.1.Timestamp", testNow.AddDate(-1, 0, 0).Format(time.RFC3339))
		}, "InvalidParameterValue"},
		{"a timestamp from tomorrow", func(v url.Values) {
			v.Set("MetricData.member.1.Timestamp", testNow.AddDate(0, 0, 1).Format(time.RFC3339))
		}, "InvalidParameterValue"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := putQuery()
			tt.mutate(q)
			_, body := f.call(t, testAccountA, q)
			if got := errorCode(t, body); got != tt.wantErr {
				t.Errorf("code = %s, want %s (%s)", got, tt.wantErr, body)
			}
		})
	}
}

// An absent timestamp defaults to now, which the fixture pins.
func TestPutMetricDataDefaultsTheTimestamp(t *testing.T) {
	f := newFixture(t)

	q := putQuery()
	q.Del("MetricData.member.1.Timestamp")
	status, body := f.call(t, testAccountA, q)
	if status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", status, body)
	}

	var stats getMetricStatisticsResponse
	status, body = f.call(t, testAccountA, url.Values{
		"Action":                    {"GetMetricStatistics"},
		"Namespace":                 {"Tenant/App"},
		"MetricName":                {"Latency"},
		"Dimensions.member.1.Name":  {"Service"},
		"Dimensions.member.1.Value": {"api"},
		"StartTime":                 {testNow.Add(-time.Minute).Format(time.RFC3339)},
		"EndTime":                   {testNow.Add(time.Minute).Format(time.RFC3339)},
		"Period":                    {"60"},
		"Statistics.member.1":       {StatSampleCount},
	})
	decode(t, status, body, &stats)
	if len(stats.Result.Datapoints) != 1 {
		t.Errorf("datapoints = %v, want the sample stamped at now", stats.Result.Datapoints)
	}
}
