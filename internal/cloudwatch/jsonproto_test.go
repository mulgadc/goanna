package cloudwatch

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// callJSON posts an AWS JSON 1.0 request the way a current AWS SDK does.
func (f *fixture) callJSON(t *testing.T, account, action, body string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", jsonContentType)
	req.Header.Set(targetHeader, "GraniteServiceVersion20100801."+action)
	req.Header.Set(queryModeHeader, "true")
	if account != "" {
		req = req.WithContext(WithIdentity(req.Context(), Identity{
			AccountID: account, AccessKeyID: "AKIATEST", UserName: "tester",
		}))
	}

	rec := httptest.NewRecorder()
	f.server.ServeHTTP(rec, req)

	out, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return rec.Code, string(out)
}

func decodeJSON[T any](t *testing.T, status int, body string, doc *T) {
	t.Helper()
	if status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", status, body)
	}
	if err := json.Unmarshal([]byte(body), doc); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
}

func TestJSONListMetrics(t *testing.T) {
	f := newFixture(t)
	f.seedCPU(t, testAccountA, testInstanceA, 10, 20, 30)

	var resp jsonListMetrics
	status, body := f.callJSON(t, testAccountA, "ListMetrics", `{}`)
	decodeJSON(t, status, body, &resp)

	if len(resp.Metrics) != 1 {
		t.Fatalf("metrics = %v, want 1", resp.Metrics)
	}
	got := resp.Metrics[0]
	if got.Namespace != NamespaceEC2 || got.MetricName != "CPUUtilization" {
		t.Errorf("metric = %+v", got)
	}
	if len(got.Dimensions) != 1 || got.Dimensions[0].Name != DimensionInstanceID ||
		got.Dimensions[0].Value != testInstanceA {
		t.Errorf("dimensions = %+v", got.Dimensions)
	}
}

// The JSON body must reach the same parsing the query protocol uses, including
// nested objects and lists.
func TestJSONGetMetricStatistics(t *testing.T) {
	f := newFixture(t)
	f.seedCPU(t, testAccountA, testInstanceA, 10, 20, 30)

	body := fmt.Sprintf(`{
	  "Namespace": %q,
	  "MetricName": "CPUUtilization",
	  "Dimensions": [{"Name": %q, "Value": %q}],
	  "StartTime": %d,
	  "EndTime": %d,
	  "Period": 3600,
	  "Statistics": ["Sum", "SampleCount", "Maximum"]
	}`, NamespaceEC2, DimensionInstanceID, testInstanceA,
		seedStart.Unix(), testNow.Add(time.Minute).Unix())

	var resp jsonGetMetricStatistics
	status, out := f.callJSON(t, testAccountA, "GetMetricStatistics", body)
	decodeJSON(t, status, out, &resp)

	var sum, count float64
	for _, dp := range resp.Datapoints {
		if dp.Sum == nil || dp.SampleCount == nil {
			t.Fatalf("datapoint missing a requested statistic: %+v", dp)
		}
		if dp.Average != nil {
			t.Errorf("Average was not requested but was returned: %+v", dp)
		}
		if dp.Timestamp <= 0 {
			t.Errorf("timestamp = %v, want epoch seconds", dp.Timestamp)
		}
		sum += *dp.Sum
		count += *dp.SampleCount
	}
	if sum != 60 || count != 3 {
		t.Errorf("sum = %v, count = %v; want 60 and 3", sum, count)
	}
}

func TestJSONGetMetricData(t *testing.T) {
	f := newFixture(t)
	f.seedCPU(t, testAccountA, testInstanceA, 10, 20, 30)

	body := fmt.Sprintf(`{
	  "StartTime": %d,
	  "EndTime": %d,
	  "MetricDataQueries": [{
	    "Id": "m1",
	    "MetricStat": {
	      "Metric": {
	        "Namespace": %q,
	        "MetricName": "CPUUtilization",
	        "Dimensions": [{"Name": %q, "Value": %q}]
	      },
	      "Period": 3600,
	      "Stat": "Sum"
	    },
	    "ReturnData": true
	  }]
	}`, seedStart.Unix(), testNow.Add(time.Minute).Unix(),
		NamespaceEC2, DimensionInstanceID, testInstanceA)

	var resp jsonGetMetricData
	status, out := f.callJSON(t, testAccountA, "GetMetricData", body)
	decodeJSON(t, status, out, &resp)

	if len(resp.MetricDataResults) != 1 {
		t.Fatalf("results = %+v", resp.MetricDataResults)
	}
	got := resp.MetricDataResults[0]
	if got.ID != "m1" {
		t.Errorf("id = %q", got.ID)
	}
	if len(got.Values) == 0 || len(got.Values) != len(got.Timestamps) {
		t.Fatalf("values = %v, timestamps = %v", got.Values, got.Timestamps)
	}
	var total float64
	for _, v := range got.Values {
		total += v
	}
	if total != 60 {
		t.Errorf("total = %v, want 60", total)
	}
}

// A JSON write must be readable by a query-protocol read: one store, two
// protocols.
func TestJSONPutMetricDataIsReadableOverQuery(t *testing.T) {
	f := newFixture(t)

	body := `{
	  "Namespace": "Tenant/App",
	  "MetricData": [{
	    "MetricName": "Latency",
	    "Dimensions": [{"Name": "Service", "Value": "api"}],
	    "Unit": "Milliseconds",
	    "Value": 42
	  }]
	}`
	status, out := f.callJSON(t, testAccountA, "PutMetricData", body)
	if status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", status, out)
	}

	var resp listMetricsResponse
	qstatus, qbody := f.call(t, testAccountA, url.Values{
		"Action":    {"ListMetrics"},
		"Namespace": {"Tenant/App"},
	})
	decode(t, qstatus, qbody, &resp)
	if len(resp.Result.Metrics) != 1 {
		t.Fatalf("metrics = %v, want the JSON write", resp.Result.Metrics)
	}
	if resp.Result.Metrics[0].MetricName != "Latency" {
		t.Errorf("metric = %+v", resp.Result.Metrics[0])
	}
}

// An error has to come back in the caller's protocol, or the SDK cannot read
// it.
func TestJSONErrorsAreJSON(t *testing.T) {
	f := newFixture(t)

	status, body := f.callJSON(t, testAccountA, "GetMetricStatistics", `{"Namespace": "AWS/EC2"}`)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", status, body)
	}
	var resp jsonError
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	if resp.Type == "" || resp.Message == "" {
		t.Errorf("error = %+v", resp)
	}
}

// The account filter is the security boundary in both protocols, so the JSON
// path gets the same test the query path has.
func TestJSONTenantIsolation(t *testing.T) {
	f := newFixture(t)
	f.seedCPU(t, testAccountA, testInstanceA, 10, 20, 30)
	f.seedCPU(t, testAccountB, testInstanceB, 70, 80, 90)

	body := fmt.Sprintf(`{
	  "Namespace": %q,
	  "MetricName": "CPUUtilization",
	  "Dimensions": [{"Name": %q, "Value": %q}],
	  "StartTime": %d,
	  "EndTime": %d,
	  "Period": 3600,
	  "Statistics": ["Sum"]
	}`, NamespaceEC2, DimensionInstanceID, testInstanceB,
		seedStart.Unix(), testNow.Add(time.Minute).Unix())

	var resp jsonGetMetricStatistics
	status, out := f.callJSON(t, testAccountA, "GetMetricStatistics", body)
	decodeJSON(t, status, out, &resp)
	if len(resp.Datapoints) != 0 {
		t.Errorf("account A read account B over JSON: %+v", resp.Datapoints)
	}
}

func TestWireOfDetectsTheProtocol(t *testing.T) {
	form := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=ListMetrics"))
	if got := wireOf(form); got != wireQuery {
		t.Errorf("form request = %v, want wireQuery", got)
	}

	js := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	js.Header.Set(targetHeader, "GraniteServiceVersion20100801.ListMetrics")
	if got := wireOf(js); got != wireJSON {
		t.Errorf("json request = %v, want wireJSON", got)
	}
	if got := jsonAction(js); got != "ListMetrics" {
		t.Errorf("action = %q", got)
	}
}

func TestFlattenJSONMatchesQueryKeys(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{
	  "Namespace": "Tenant/App",
	  "MetricData": [
	    {"MetricName": "a", "Value": 1.5, "Dimensions": [{"Name": "S", "Value": "api"}]},
	    {"MetricName": "b", "Value": 2, "StorageResolution": 60}
	  ],
	  "Flag": true,
	  "Absent": null
	}`))
	form, err := decodeJSONBody(req)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	want := map[string]string{
		"Namespace":                                     "Tenant/App",
		"MetricData.member.1.MetricName":                "a",
		"MetricData.member.1.Value":                     "1.5",
		"MetricData.member.1.Dimensions.member.1.Name":  "S",
		"MetricData.member.1.Dimensions.member.1.Value": "api",
		"MetricData.member.2.MetricName":                "b",
		"MetricData.member.2.Value":                     "2",
		"MetricData.member.2.StorageResolution":         "60",
		"Flag":                                          "true",
	}
	for key, value := range want {
		if got := form.get(key); got != value {
			t.Errorf("%s = %q, want %q", key, got, value)
		}
	}
	if got := form.get("Absent"); got != "" {
		t.Errorf("a null became %q", got)
	}
}
