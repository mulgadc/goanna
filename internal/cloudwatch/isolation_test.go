package cloudwatch

import (
	"net/url"
	"testing"
	"time"
)

// The account filter is the security boundary: a query must not reach another
// tenant's series regardless of the dimensions it asks for. Every action is
// exercised as account A against data belonging to account B.

// twoTenants seeds an identically-named metric for both accounts, so a leak
// shows up as an extra result rather than as a different metric.
func twoTenants(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t)
	f.seedCPU(t, testAccountA, testInstanceA, 10, 20, 30)
	f.seedCPU(t, testAccountB, testInstanceB, 70, 80, 90)
	return f
}

func TestTenantIsolation_ListMetrics(t *testing.T) {
	f := twoTenants(t)

	var resp listMetricsResponse
	status, body := f.call(t, testAccountA, url.Values{"Action": {"ListMetrics"}})
	decode(t, status, body, &resp)

	for _, m := range resp.Result.Metrics {
		for _, d := range m.Dimensions {
			if d.Value == testInstanceB {
				t.Fatalf("account A listed account B's instance: %v", m)
			}
		}
	}
	if len(resp.Result.Metrics) != 1 {
		t.Errorf("metrics = %v, want only account A's", resp.Result.Metrics)
	}
}

// Naming the other tenant's instance directly is the attack the type-level
// scope exists to stop.
func TestTenantIsolation_ListMetricsByForeignDimension(t *testing.T) {
	f := twoTenants(t)

	var resp listMetricsResponse
	status, body := f.call(t, testAccountA, url.Values{
		"Action":                    {"ListMetrics"},
		"Dimensions.member.1.Name":  {DimensionInstanceID},
		"Dimensions.member.1.Value": {testInstanceB},
	})
	decode(t, status, body, &resp)
	if len(resp.Result.Metrics) != 0 {
		t.Errorf("account A reached account B's metrics: %v", resp.Result.Metrics)
	}
}

func TestTenantIsolation_GetMetricStatistics(t *testing.T) {
	f := twoTenants(t)

	var resp getMetricStatisticsResponse
	status, body := f.call(t, testAccountA, statsQuery(testInstanceB))
	decode(t, status, body, &resp)
	if len(resp.Result.Datapoints) != 0 {
		t.Errorf("account A read account B's datapoints: %v", resp.Result.Datapoints)
	}

	// The same query as the owner does return data, so the empty result above
	// is the scope and not a broken fixture.
	resp = getMetricStatisticsResponse{}
	status, body = f.call(t, testAccountB, statsQuery(testInstanceB))
	decode(t, status, body, &resp)
	if len(resp.Result.Datapoints) == 0 {
		t.Error("the owning account read nothing; the fixture is not proving isolation")
	}
}

func TestTenantIsolation_GetMetricData(t *testing.T) {
	f := twoTenants(t)

	var resp getMetricDataResponse
	status, body := f.call(t, testAccountA, dataQuery(testInstanceB))
	decode(t, status, body, &resp)

	if len(resp.Result.MetricDataResults) != 1 {
		t.Fatalf("results = %v, want one empty result", resp.Result.MetricDataResults)
	}
	if got := resp.Result.MetricDataResults[0]; len(got.Values) != 0 {
		t.Errorf("account A read account B's values: %v", got.Values)
	}
}

// A dimension name may never become a raw label name, so no spelling of
// account_id can reach another tenant.
func TestTenantIsolation_AccountIDIsNotADimension(t *testing.T) {
	f := twoTenants(t)

	for _, name := range []string{labelAccountID, "AccountId", "AWSAccountId", "Account"} {
		t.Run(name, func(t *testing.T) {
			var resp listMetricsResponse
			status, body := f.call(t, testAccountA, url.Values{
				"Action":                    {"ListMetrics"},
				"Dimensions.member.1.Name":  {name},
				"Dimensions.member.1.Value": {testAccountB},
			})
			decode(t, status, body, &resp)
			if len(resp.Result.Metrics) != 0 {
				t.Errorf("dimension %s reached %v", name, resp.Result.Metrics)
			}
		})
	}
}

// PutMetricData stamps the account from the signature, so a payload naming
// another tenant writes into the caller's own account.
func TestTenantIsolation_PutMetricDataIgnoresAPayloadAccount(t *testing.T) {
	f := newFixture(t)

	q := putQuery(
		"MetricData.member.1.Dimensions.member.2.Name", "account_id",
		"MetricData.member.1.Dimensions.member.2.Value", testAccountB,
	)
	status, body := f.call(t, testAccountA, q)
	if status != 200 {
		t.Fatalf("status = %d, body = %s", status, body)
	}

	listCustom := url.Values{"Action": {"ListMetrics"}, "Namespace": {"Tenant/App"}}

	var asB listMetricsResponse
	status, body = f.call(t, testAccountB, listCustom)
	decode(t, status, body, &asB)
	if len(asB.Result.Metrics) != 0 {
		t.Errorf("account B saw a write it did not make: %v", asB.Result.Metrics)
	}

	var asA listMetricsResponse
	status, body = f.call(t, testAccountA, listCustom)
	decode(t, status, body, &asA)
	if len(asA.Result.Metrics) != 1 {
		t.Errorf("account A did not see its own write: %v", asA.Result.Metrics)
	}
}

// Two tenants publishing the same custom metric with the same dimensions must
// still read back only their own samples.
func TestTenantIsolation_CustomMetricsWithIdenticalIdentity(t *testing.T) {
	f := newFixture(t)

	for i, account := range []string{testAccountA, testAccountB} {
		q := putQuery("MetricData.member.1.Value", []string{"11", "22"}[i])
		if status, body := f.call(t, account, q); status != 200 {
			t.Fatalf("put as %s: %d %s", account, status, body)
		}
	}

	read := url.Values{
		"Action":                    {"GetMetricStatistics"},
		"Namespace":                 {"Tenant/App"},
		"MetricName":                {"Latency"},
		"Dimensions.member.1.Name":  {"Service"},
		"Dimensions.member.1.Value": {"api"},
		"StartTime":                 {seedStart.Format(time.RFC3339)},
		"EndTime":                   {testNow.Format(time.RFC3339)},
		"Period":                    {"300"},
		"Statistics.member.1":       {StatSum},
		"Statistics.member.2":       {StatSampleCount},
	}

	for i, account := range []string{testAccountA, testAccountB} {
		var resp getMetricStatisticsResponse
		status, body := f.call(t, account, read)
		decode(t, status, body, &resp)
		if len(resp.Result.Datapoints) != 1 {
			t.Fatalf("%s: datapoints = %v, want 1", account, resp.Result.Datapoints)
		}
		dp := resp.Result.Datapoints[0]
		if *dp.SampleCount != 1 {
			t.Errorf("%s: sample count = %v; the other tenant's sample was included", account, *dp.SampleCount)
		}
		if want := []float64{11, 22}[i]; *dp.Sum != want {
			t.Errorf("%s: sum = %v, want %v", account, *dp.Sum, want)
		}
	}
}
