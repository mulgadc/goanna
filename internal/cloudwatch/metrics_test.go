package cloudwatch

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

// The table is the schema seam between the collector and the API. Both
// directions must agree, or a metric could be listed under one name and
// queried under another.
func TestMetricTableRoundTrips(t *testing.T) {
	for _, m := range metricTable {
		byName, ok := lookupMetric(m.Namespace, m.Name)
		if !ok {
			t.Fatalf("%s/%s not resolvable by request", m.Namespace, m.Name)
		}
		if byName.Series != m.Series {
			t.Errorf("%s/%s resolves to series %s, want %s", m.Namespace, m.Name, byName.Series, m.Series)
		}

		bySeriesName, ok := lookupSeries(m.Series)
		if !ok {
			t.Fatalf("series %s not resolvable back to a metric", m.Series)
		}
		if bySeriesName.Name != m.Name || bySeriesName.Namespace != m.Namespace {
			t.Errorf("series %s resolves to %s/%s, want %s/%s",
				m.Series, bySeriesName.Namespace, bySeriesName.Name, m.Namespace, m.Name)
		}
		if m.Unit == "" {
			t.Errorf("%s/%s has no unit", m.Namespace, m.Name)
		}
	}
}

// The collector's series set is locked. If a name here stops matching what it
// publishes, every query for that metric silently returns nothing.
func TestMetricTableCoversTheCollectorSeries(t *testing.T) {
	published := []string{
		"goanna_ec2_cpu_utilization",
		"goanna_ec2_network_in_bytes",
		"goanna_ec2_network_out_bytes",
		"goanna_ec2_disk_read_bytes",
		"goanna_ec2_disk_write_bytes",
		"goanna_ec2_disk_read_ops",
		"goanna_ec2_disk_write_ops",
		"goanna_ec2_memory_actual_bytes",
	}
	for _, series := range published {
		if _, ok := lookupSeries(series); !ok {
			t.Errorf("collector publishes %s but the table does not project it", series)
		}
	}
	if len(metricTable) != len(published) {
		t.Errorf("table has %d metrics, the collector publishes %d", len(metricTable), len(published))
	}
}

// Guest memory has no AWS/EC2 equivalent, so publishing it there would put a
// metric real tooling cannot recognise into a reserved namespace.
func TestMemoryIsNotProjectedIntoAWSNamespace(t *testing.T) {
	m, ok := lookupSeries("goanna_ec2_memory_actual_bytes")
	if !ok {
		t.Fatal("memory series missing from the table")
	}
	if m.Namespace != NamespaceGoannaEC2 {
		t.Errorf("memory namespace = %s, want %s", m.Namespace, NamespaceGoannaEC2)
	}
	for _, ec2Metric := range metricsInNamespace(NamespaceEC2) {
		if ec2Metric.Series == "goanna_ec2_memory_actual_bytes" {
			t.Error("memory is projected into AWS/EC2")
		}
	}
}

func TestServedNamespaces(t *testing.T) {
	got := servedNamespaces()
	if len(got) != 2 || got[0] != NamespaceEC2 || got[1] != NamespaceGoannaEC2 {
		t.Errorf("namespaces = %v, want [%s %s]", got, NamespaceEC2, NamespaceGoannaEC2)
	}
}

func TestIsReservedNamespace(t *testing.T) {
	for _, ns := range []string{"AWS/EC2", "AWS/Anything", "Goanna/EC2", "Goanna/New"} {
		if !isReservedNamespace(ns) {
			t.Errorf("%s should be reserved", ns)
		}
	}
	for _, ns := range []string{"Tenant/App", "MyNamespace", "awsome/thing", ""} {
		if isReservedNamespace(ns) {
			t.Errorf("%s should not be reserved", ns)
		}
	}
}

func matcherMap(t *testing.T, matchers []*labels.Matcher) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, m := range matchers {
		if m.Type != labels.MatchEqual {
			t.Errorf("matcher %s is not an equality match", m)
		}
		out[m.Name] = m.Value
	}
	return out
}

func TestResolveLockedTarget(t *testing.T) {
	got, err := resolveTarget(NamespaceEC2, "CPUUtilization", "",
		[]dimension{{Name: DimensionInstanceID, Value: "i-abc"}})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.empty {
		t.Fatal("a published metric resolved to empty")
	}
	if got.unit != UnitPercent {
		t.Errorf("unit = %s, want %s", got.unit, UnitPercent)
	}

	m := matcherMap(t, got.matchers)
	if m[model.MetricNameLabel] != "goanna_ec2_cpu_utilization" {
		t.Errorf("series matcher = %q", m[model.MetricNameLabel])
	}
	if m[labelInstanceID] != "i-abc" {
		t.Errorf("instance matcher = %q", m[labelInstanceID])
	}
	if _, ok := m[labelAccountID]; ok {
		t.Error("resolution produced an account_id matcher; only the store may set one")
	}
}

// A dimension name reaches a label name only through the allowlist, so no
// caller input can name a label the scope depends on.
func TestResolveRejectsUnknownDimensions(t *testing.T) {
	tests := []struct {
		name string
		dims []dimension
	}{
		{"no dimensions at all", nil},
		{"an unpublished dimension", []dimension{{Name: "InstanceType", Value: "t3.micro"}}},
		{"the account label spelled as a dimension", []dimension{{Name: labelAccountID, Value: "999988887777"}}},
		{"InstanceId plus another", []dimension{
			{Name: DimensionInstanceID, Value: "i-abc"},
			{Name: "ImageId", Value: "ami-1"},
		}},
		{"an empty instance id", []dimension{{Name: DimensionInstanceID, Value: ""}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveTarget(NamespaceEC2, "CPUUtilization", "", tt.dims)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if !got.empty {
				t.Errorf("resolved to %v, want an empty target", matcherMap(t, got.matchers))
			}
		})
	}
}

func TestResolveUnknownMetricIsEmptyNotAnError(t *testing.T) {
	dims := []dimension{{Name: DimensionInstanceID, Value: "i-abc"}}
	got, err := resolveTarget(NamespaceEC2, "StatusCheckFailed", "", dims)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !got.empty {
		t.Error("an unpublished AWS/EC2 metric should resolve to empty")
	}
}

func TestResolveRequiresNamespaceAndMetricName(t *testing.T) {
	_, err := resolveTarget("", "CPUUtilization", "", nil)
	wantAPIError(t, err, "MissingParameter")
	_, err = resolveTarget(NamespaceEC2, "", "", nil)
	wantAPIError(t, err, "MissingParameter")
}

// The collector's unit is fixed per metric, so filtering on any other one can
// only match nothing.
func TestResolveLockedUnitFilter(t *testing.T) {
	dims := []dimension{{Name: DimensionInstanceID, Value: "i-abc"}}

	got, err := resolveTarget(NamespaceEC2, "CPUUtilization", UnitPercent, dims)
	if err != nil || got.empty {
		t.Fatalf("matching unit rejected: %v empty=%v", err, got.empty)
	}
	got, err = resolveTarget(NamespaceEC2, "CPUUtilization", UnitBytes, dims)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !got.empty {
		t.Error("a mismatched unit filter should resolve to empty")
	}
}

func TestResolveCustomTarget(t *testing.T) {
	got, err := resolveTarget("Tenant/App", "Latency", UnitCount,
		[]dimension{{Name: "Service", Value: "api"}})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.empty || !got.custom {
		t.Fatalf("custom metric resolved to empty=%v custom=%v", got.empty, got.custom)
	}

	m := matcherMap(t, got.matchers)
	want := map[string]string{
		model.MetricNameLabel:       customSeries,
		labelNamespace:              "Tenant/App",
		labelMetricName:             "Latency",
		labelUnit:                   UnitCount,
		dimensionPrefix + "Service": "api",
	}
	for name, value := range want {
		if m[name] != value {
			t.Errorf("matcher %s = %q, want %q", name, m[name], value)
		}
	}
	if got.dimCount != 1 {
		t.Errorf("dimCount = %d, want 1", got.dimCount)
	}
}

func TestResolveCustomRejectsRepeatedDimension(t *testing.T) {
	_, err := resolveTarget("Tenant/App", "Latency", "", []dimension{
		{Name: "Service", Value: "api"},
		{Name: "Service", Value: "web"},
	})
	wantAPIError(t, err, "InvalidParameterValue")
}

// A dimension name that could not have been written matches nothing rather
// than becoming a raw label name.
func TestResolveCustomRejectsInvalidDimensionName(t *testing.T) {
	got, err := resolveTarget("Tenant/App", "Latency", "",
		[]dimension{{Name: "my-service", Value: "api"}})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !got.empty {
		t.Error("an unstorable dimension name should resolve to empty")
	}
}

// CloudWatch identifies a metric by its complete dimension set, so a stored
// series with extra dimensions is a different metric.
func TestMatchesDimensionSet(t *testing.T) {
	oneDim, err := resolveTarget("Tenant/App", "Latency", "",
		[]dimension{{Name: "Service", Value: "api"}})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	exact := labels.FromStrings(model.MetricNameLabel, customSeries, dimensionPrefix+"Service", "api")
	extra := labels.FromStrings(model.MetricNameLabel, customSeries,
		dimensionPrefix+"Service", "api", dimensionPrefix+"Region", "ap-southeast-2")

	if !oneDim.matchesDimensionSet(exact) {
		t.Error("the exact dimension set was rejected")
	}
	if oneDim.matchesDimensionSet(extra) {
		t.Error("a series with an extra dimension was accepted")
	}
}

func TestDimensionsOf(t *testing.T) {
	locked := labels.FromStrings(labelInstanceID, "i-abc", labelAccountID, "111122223333")
	got := dimensionsOf(locked, false)
	if len(got) != 1 || got[0].Name != DimensionInstanceID || got[0].Value != "i-abc" {
		t.Errorf("locked dimensions = %v", got)
	}
	// account_id is not a dimension and must never be projected as one.
	for _, d := range got {
		if d.Name == labelAccountID {
			t.Error("account_id was projected as a dimension")
		}
	}

	custom := labels.FromStrings(dimensionPrefix+"Service", "api",
		labelNamespace, "Tenant/App", labelAccountID, "111122223333")
	got = dimensionsOf(custom, true)
	if len(got) != 1 || got[0].Name != "Service" || got[0].Value != "api" {
		t.Errorf("custom dimensions = %v", got)
	}
}
