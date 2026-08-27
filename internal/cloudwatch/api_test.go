package cloudwatch

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mulgadc/goanna/internal/store"
)

const (
	testAccountA  = "111122223333"
	testAccountB  = "999988887777"
	testInstanceA = "i-0aaaaaaaaaaaaaaaa"
	testInstanceB = "i-0bbbbbbbbbbbbbbbb"
)

// testNow anchors every window in the tests, so the 14-day ListMetrics lookup
// and the seeded samples cannot drift apart.
var testNow = time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)

// seedStart is one hour before testNow. The TSDB head only accepts appends
// inside half a block of its maximum, so samples are written ascending from
// here.
var seedStart = testNow.Add(-time.Hour)

type fixture struct {
	server *Server
	store  *store.Store
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	s, err := store.Open(store.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	server, err := New(Options{Store: s, Now: func() time.Time { return testNow }})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return &fixture{server: server, store: s}
}

// seedCPU writes a 60s CPU series for one instance under one account.
func (f *fixture) seedCPU(t *testing.T, account, instance string, values ...float64) {
	t.Helper()
	points := make([]store.Point, 0, len(values))
	for i, v := range values {
		points = append(points, store.Point{
			Name: "goanna_ec2_cpu_utilization",
			Labels: map[string]string{
				labelInstanceID: instance,
				labelNamespace:  NamespaceEC2,
			},
			Timestamp: seedStart.Add(time.Duration(i) * time.Minute).UnixMilli(),
			Value:     v,
		})
	}
	if err := f.store.AppendTenant(t.Context(), account, points); err != nil {
		t.Fatalf("seed %s/%s: %v", account, instance, err)
	}
}

// call drives one action as account and returns the status and body.
func (f *fixture) call(t *testing.T, account string, values url.Values) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(values.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if account != "" {
		req = req.WithContext(WithIdentity(req.Context(), Identity{
			AccountID: account, AccessKeyID: "AKIATEST", UserName: "tester",
		}))
	}

	rec := httptest.NewRecorder()
	f.server.ServeHTTP(rec, req)

	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return rec.Code, string(body)
}

// decode unmarshals a successful response, failing the test on a non-200.
func decode[T any](t *testing.T, status int, body string, doc *T) {
	t.Helper()
	if status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", status, body)
	}
	if err := xml.Unmarshal([]byte(body), doc); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
}

// errorCode pulls the Code out of an ErrorResponse.
func errorCode(t *testing.T, body string) string {
	t.Helper()
	var resp errorResponse
	if err := xml.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("unmarshal error response %s: %v", body, err)
	}
	return resp.Error.Code
}

func statsQuery(instance string, extra ...string) url.Values {
	v := url.Values{
		"Action":                    {"GetMetricStatistics"},
		"Namespace":                 {NamespaceEC2},
		"MetricName":                {"CPUUtilization"},
		"Dimensions.member.1.Name":  {DimensionInstanceID},
		"Dimensions.member.1.Value": {instance},
		"StartTime":                 {seedStart.Format(time.RFC3339)},
		"EndTime":                   {testNow.Format(time.RFC3339)},
		"Period":                    {"300"},
		"Statistics.member.1":       {StatAverage},
		"Statistics.member.2":       {StatSum},
		"Statistics.member.3":       {StatSampleCount},
	}
	for i := 0; i+1 < len(extra); i += 2 {
		v.Set(extra[i], extra[i+1])
	}
	return v
}

func dataQuery(instance string, extra ...string) url.Values {
	v := url.Values{
		"Action":                        {"GetMetricData"},
		"StartTime":                     {seedStart.Format(time.RFC3339)},
		"EndTime":                       {testNow.Format(time.RFC3339)},
		"MetricDataQueries.member.1.Id": {"m1"},
		"MetricDataQueries.member.1.MetricStat.Metric.Namespace":                 {NamespaceEC2},
		"MetricDataQueries.member.1.MetricStat.Metric.MetricName":                {"CPUUtilization"},
		"MetricDataQueries.member.1.MetricStat.Metric.Dimensions.member.1.Name":  {DimensionInstanceID},
		"MetricDataQueries.member.1.MetricStat.Metric.Dimensions.member.1.Value": {instance},
		"MetricDataQueries.member.1.MetricStat.Period":                           {"300"},
		"MetricDataQueries.member.1.MetricStat.Stat":                             {StatAverage},
	}
	for i := 0; i+1 < len(extra); i += 2 {
		v.Set(extra[i], extra[i+1])
	}
	return v
}

// --- dispatch ---

func TestUnauthenticatedRequestIsRefused(t *testing.T) {
	f := newFixture(t)
	status, body := f.call(t, "", url.Values{"Action": {"ListMetrics"}})
	if status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", status)
	}
	if got := errorCode(t, body); got != "AccessDenied" {
		t.Errorf("code = %s, want AccessDenied", got)
	}
}

func TestUnknownAndMissingAction(t *testing.T) {
	f := newFixture(t)

	_, body := f.call(t, testAccountA, url.Values{"Action": {"DeleteAlarms"}})
	if got := errorCode(t, body); got != "InvalidAction" {
		t.Errorf("code = %s, want InvalidAction", got)
	}
	_, body = f.call(t, testAccountA, url.Values{})
	if got := errorCode(t, body); got != "MissingParameter" {
		t.Errorf("code = %s, want MissingParameter", got)
	}
}

// --- ListMetrics ---

func TestListMetrics(t *testing.T) {
	f := newFixture(t)
	f.seedCPU(t, testAccountA, testInstanceA, 10, 20)

	var resp listMetricsResponse
	status, body := f.call(t, testAccountA, url.Values{
		"Action": {"ListMetrics"}, "Namespace": {NamespaceEC2},
	})
	decode(t, status, body, &resp)

	if len(resp.Result.Metrics) != 1 {
		t.Fatalf("metrics = %v, want one", resp.Result.Metrics)
	}
	m := resp.Result.Metrics[0]
	if m.Namespace != NamespaceEC2 || m.MetricName != "CPUUtilization" {
		t.Errorf("metric = %s/%s", m.Namespace, m.MetricName)
	}
	if len(m.Dimensions) != 1 || m.Dimensions[0].Name != DimensionInstanceID ||
		m.Dimensions[0].Value != testInstanceA {
		t.Errorf("dimensions = %v", m.Dimensions)
	}
}

func TestListMetricsFilters(t *testing.T) {
	f := newFixture(t)
	f.seedCPU(t, testAccountA, testInstanceA, 10)

	tests := []struct {
		name  string
		query url.Values
		want  int
	}{
		{"by metric name", url.Values{"Action": {"ListMetrics"}, "MetricName": {"CPUUtilization"}}, 1},
		{"by another metric name", url.Values{"Action": {"ListMetrics"}, "MetricName": {"NetworkIn"}}, 0},
		{"by namespace", url.Values{"Action": {"ListMetrics"}, "Namespace": {NamespaceGoannaEC2}}, 0},
		{"by instance", url.Values{
			"Action":                    {"ListMetrics"},
			"Dimensions.member.1.Name":  {DimensionInstanceID},
			"Dimensions.member.1.Value": {testInstanceA},
		}, 1},
		{"by an unpublished dimension", url.Values{
			"Action":                    {"ListMetrics"},
			"Dimensions.member.1.Name":  {"InstanceType"},
			"Dimensions.member.1.Value": {"t3.micro"},
		}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resp listMetricsResponse
			status, body := f.call(t, testAccountA, tt.query)
			decode(t, status, body, &resp)
			if len(resp.Result.Metrics) != tt.want {
				t.Errorf("metrics = %v, want %d", resp.Result.Metrics, tt.want)
			}
		})
	}
}

// --- GetMetricStatistics ---

func TestGetMetricStatistics(t *testing.T) {
	f := newFixture(t)
	// Five minutes of one-minute samples, so a 300s period is one full bucket.
	f.seedCPU(t, testAccountA, testInstanceA, 10, 20, 30, 40, 50)

	var resp getMetricStatisticsResponse
	status, body := f.call(t, testAccountA, statsQuery(testInstanceA))
	decode(t, status, body, &resp)

	if resp.Result.Label != "CPUUtilization" {
		t.Errorf("label = %s", resp.Result.Label)
	}
	if len(resp.Result.Datapoints) != 1 {
		t.Fatalf("datapoints = %v, want 1", resp.Result.Datapoints)
	}

	dp := resp.Result.Datapoints[0]
	if dp.Average == nil || *dp.Average != 30 {
		t.Errorf("average = %v, want 30", dp.Average)
	}
	if dp.Sum == nil || *dp.Sum != 150 {
		t.Errorf("sum = %v, want 150", dp.Sum)
	}
	if dp.SampleCount == nil || *dp.SampleCount != 5 {
		t.Errorf("sample count = %v, want 5", dp.SampleCount)
	}
	// Minimum and Maximum were not requested, so they must be absent rather
	// than reported as zero.
	if dp.Minimum != nil || dp.Maximum != nil {
		t.Errorf("unrequested statistics returned: min=%v max=%v", dp.Minimum, dp.Maximum)
	}
	if dp.Unit != UnitPercent {
		t.Errorf("unit = %s, want %s", dp.Unit, UnitPercent)
	}
	if dp.Timestamp.UnixMilli()%300_000 != 0 {
		t.Errorf("datapoint %s is not on an epoch period boundary", dp.Timestamp)
	}
}

func TestGetMetricStatisticsValidation(t *testing.T) {
	f := newFixture(t)

	tests := []struct {
		name    string
		mutate  func(url.Values)
		wantErr string
	}{
		{"no period", func(v url.Values) { v.Del("Period") }, "MissingParameter"},
		{"sub-minute period", func(v url.Values) { v.Set("Period", "30") }, "InvalidParameterValue"},
		{"no statistics", func(v url.Values) {
			v.Del("Statistics.member.1")
			v.Del("Statistics.member.2")
			v.Del("Statistics.member.3")
		}, "MissingParameter"},
		{"unknown statistic", func(v url.Values) { v.Set("Statistics.member.1", "p99") }, "InvalidParameterValue"},
		{"extended statistics", func(v url.Values) {
			v.Set("ExtendedStatistics.member.1", "p99")
		}, "InvalidParameterValue"},
		{"no start time", func(v url.Values) { v.Del("StartTime") }, "MissingParameter"},
		{"end before start", func(v url.Values) {
			v.Set("EndTime", seedStart.Add(-time.Hour).Format(time.RFC3339))
		}, "InvalidParameterCombination"},
		{"too many datapoints", func(v url.Values) {
			v.Set("StartTime", testNow.Add(-90*24*time.Hour).Format(time.RFC3339))
			v.Set("Period", "60")
		}, "InvalidParameterCombination"},
		{"no namespace", func(v url.Values) { v.Del("Namespace") }, "MissingParameter"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := statsQuery(testInstanceA)
			tt.mutate(q)
			_, body := f.call(t, testAccountA, q)
			if got := errorCode(t, body); got != tt.wantErr {
				t.Errorf("code = %s, want %s (%s)", got, tt.wantErr, body)
			}
		})
	}
}

// AWS identifies a metric by its complete dimension set, so a query that omits
// InstanceId asks for a metric the collector never published.
func TestGetMetricStatisticsRequiresTheFullDimensionSet(t *testing.T) {
	f := newFixture(t)
	f.seedCPU(t, testAccountA, testInstanceA, 10, 20)

	q := statsQuery(testInstanceA)
	q.Del("Dimensions.member.1.Name")
	q.Del("Dimensions.member.1.Value")

	var resp getMetricStatisticsResponse
	status, body := f.call(t, testAccountA, q)
	decode(t, status, body, &resp)
	if len(resp.Result.Datapoints) != 0 {
		t.Errorf("datapoints = %v, want none", resp.Result.Datapoints)
	}
}

func TestGetMetricStatisticsUnitFilter(t *testing.T) {
	f := newFixture(t)
	f.seedCPU(t, testAccountA, testInstanceA, 10, 20)

	var resp getMetricStatisticsResponse
	status, body := f.call(t, testAccountA, statsQuery(testInstanceA, "Unit", UnitPercent))
	decode(t, status, body, &resp)
	if len(resp.Result.Datapoints) == 0 {
		t.Error("the matching unit returned nothing")
	}

	resp = getMetricStatisticsResponse{}
	status, body = f.call(t, testAccountA, statsQuery(testInstanceA, "Unit", UnitBytes))
	decode(t, status, body, &resp)
	if len(resp.Result.Datapoints) != 0 {
		t.Errorf("a mismatched unit returned %v", resp.Result.Datapoints)
	}
}

// --- GetMetricData ---

func TestGetMetricData(t *testing.T) {
	f := newFixture(t)
	f.seedCPU(t, testAccountA, testInstanceA, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100)

	var resp getMetricDataResponse
	status, body := f.call(t, testAccountA, dataQuery(testInstanceA))
	decode(t, status, body, &resp)

	if len(resp.Result.MetricDataResults) != 1 {
		t.Fatalf("results = %v, want 1", resp.Result.MetricDataResults)
	}
	r := resp.Result.MetricDataResults[0]
	if r.ID != "m1" || r.Label != "CPUUtilization" || r.StatusCode != "Complete" {
		t.Errorf("result = %+v", r)
	}
	if len(r.Values) != len(r.Timestamps) {
		t.Fatalf("values (%d) and timestamps (%d) disagree", len(r.Values), len(r.Timestamps))
	}
	if len(r.Values) < 2 {
		t.Fatalf("values = %v, want at least two buckets", r.Values)
	}
	// CloudWatch defaults to TimestampDescending.
	if !r.Timestamps[0].After(r.Timestamps[1]) {
		t.Errorf("timestamps = %v, want descending", r.Timestamps)
	}
}

func TestGetMetricDataScanByAscending(t *testing.T) {
	f := newFixture(t)
	f.seedCPU(t, testAccountA, testInstanceA, 10, 20, 30, 40, 50, 60, 70)

	var resp getMetricDataResponse
	status, body := f.call(t, testAccountA, dataQuery(testInstanceA, "ScanBy", scanTimestampAscending))
	decode(t, status, body, &resp)

	r := resp.Result.MetricDataResults[0]
	if len(r.Timestamps) < 2 || !r.Timestamps[0].Before(r.Timestamps[1]) {
		t.Errorf("timestamps = %v, want ascending", r.Timestamps)
	}
}

func TestGetMetricDataMaxDatapoints(t *testing.T) {
	f := newFixture(t)
	f.seedCPU(t, testAccountA, testInstanceA, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100)

	var resp getMetricDataResponse
	status, body := f.call(t, testAccountA, dataQuery(testInstanceA, "MaxDatapoints", "1"))
	decode(t, status, body, &resp)
	if got := len(resp.Result.MetricDataResults[0].Values); got != 1 {
		t.Errorf("values = %d, want 1", got)
	}
}

func TestGetMetricDataReturnDataFalseIsOmitted(t *testing.T) {
	f := newFixture(t)
	f.seedCPU(t, testAccountA, testInstanceA, 10, 20)

	var resp getMetricDataResponse
	status, body := f.call(t, testAccountA,
		dataQuery(testInstanceA, "MetricDataQueries.member.1.ReturnData", "false"))
	decode(t, status, body, &resp)
	if len(resp.Result.MetricDataResults) != 0 {
		t.Errorf("results = %v, want none", resp.Result.MetricDataResults)
	}
}

func TestGetMetricDataValidation(t *testing.T) {
	f := newFixture(t)

	tests := []struct {
		name    string
		mutate  func(url.Values)
		wantErr string
	}{
		{"no queries", func(v url.Values) {
			for key := range v {
				if strings.HasPrefix(key, "MetricDataQueries") {
					v.Del(key)
				}
			}
		}, "MissingParameter"},
		{"no id", func(v url.Values) { v.Del("MetricDataQueries.member.1.Id") }, "MissingParameter"},
		{"malformed id", func(v url.Values) {
			v.Set("MetricDataQueries.member.1.Id", "M1")
		}, "InvalidParameterValue"},
		{"an expression", func(v url.Values) {
			v.Set("MetricDataQueries.member.1.Expression", "SUM(m1)")
		}, "InvalidParameterValue"},
		{"no stat", func(v url.Values) {
			v.Del("MetricDataQueries.member.1.MetricStat.Stat")
		}, "MissingParameter"},
		{"unknown stat", func(v url.Values) {
			v.Set("MetricDataQueries.member.1.MetricStat.Stat", "p99")
		}, "InvalidParameterValue"},
		{"no period", func(v url.Values) {
			v.Del("MetricDataQueries.member.1.MetricStat.Period")
		}, "MissingParameter"},
		{"bad scan order", func(v url.Values) { v.Set("ScanBy", "Whatever") }, "InvalidParameterValue"},
		{"zero max datapoints", func(v url.Values) { v.Set("MaxDatapoints", "0") }, "InvalidParameterValue"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := dataQuery(testInstanceA)
			tt.mutate(q)
			_, body := f.call(t, testAccountA, q)
			if got := errorCode(t, body); got != tt.wantErr {
				t.Errorf("code = %s, want %s (%s)", got, tt.wantErr, body)
			}
		})
	}
}

func TestGetMetricDataRejectsDuplicateIDs(t *testing.T) {
	f := newFixture(t)
	q := dataQuery(testInstanceA)
	for key, values := range dataQuery(testInstanceA) {
		if after, ok := strings.CutPrefix(key, "MetricDataQueries.member.1."); ok {
			q.Set("MetricDataQueries.member.2."+after, values[0])
		}
	}

	_, body := f.call(t, testAccountA, q)
	if got := errorCode(t, body); got != "InvalidParameterValue" {
		t.Errorf("code = %s, want InvalidParameterValue (%s)", got, body)
	}
}
