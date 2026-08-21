// Package cloudwatch serves the CloudWatch metrics API over the local TSDB.
package cloudwatch

import (
	"regexp"
	"sort"
	"strings"
)

// Namespaces goanna serves. NamespaceEC2 mirrors AWS; NamespaceGoannaEC2
// carries the guest-visible metrics AWS has no equivalent for.
const (
	NamespaceEC2       = "AWS/EC2"
	NamespaceGoannaEC2 = "Goanna/EC2"
)

// Labels the collector writes. account_id is the tenant boundary and is never
// derived from caller input; see tenantQuerier.
const (
	labelInstanceID = "instance_id"
	labelAccountID  = "account_id"
)

// DimensionInstanceID is the only CloudWatch dimension the EC2 metrics carry.
// AWS also publishes AutoScalingGroupName, ImageId and InstanceType; the
// collector does not emit them, so a query on one must match nothing.
const DimensionInstanceID = "InstanceId"

// metric binds a CloudWatch (namespace, name) to the series the collector
// stores it under. One table drives both directions so a request name and a
// stored name cannot drift apart.
type metric struct {
	Namespace string
	Name      string
	Series    string
	Unit      string
}

// metricTable is the schema seam. The collector's series set is locked, so
// adding a row here without the producer emitting it yields a metric that
// lists but never has data.
var metricTable = []metric{
	{NamespaceEC2, "CPUUtilization", "goanna_ec2_cpu_utilization", UnitPercent},
	{NamespaceEC2, "NetworkIn", "goanna_ec2_network_in_bytes", UnitBytes},
	{NamespaceEC2, "NetworkOut", "goanna_ec2_network_out_bytes", UnitBytes},
	{NamespaceEC2, "DiskReadBytes", "goanna_ec2_disk_read_bytes", UnitBytes},
	{NamespaceEC2, "DiskWriteBytes", "goanna_ec2_disk_write_bytes", UnitBytes},
	{NamespaceEC2, "DiskReadOps", "goanna_ec2_disk_read_ops", UnitCount},
	{NamespaceEC2, "DiskWriteOps", "goanna_ec2_disk_write_ops", UnitCount},

	// Guest memory has no AWS/EC2 equivalent — the hypervisor cannot see it,
	// which is why AWS ships an in-guest agent. Publishing it as AWS/EC2 under
	// an invented name would put a metric real tooling cannot recognise into a
	// reserved namespace.
	{NamespaceGoannaEC2, "MemoryUsed", "goanna_ec2_memory_actual_bytes", UnitBytes},
}

// CloudWatch units used by the table.
const (
	UnitPercent = "Percent"
	UnitBytes   = "Bytes"
	UnitCount   = "Count"
	UnitNone    = "None"
)

// customSeries is the single __name__ every PutMetricData sample is stored
// under, with its namespace, metric name and dimensions carried as labels.
//
// Keeping tenant writes off the goanna_ec2_* names is what makes a published
// metric unforgeable: the AWS/EC2 projection reads only those names, and
// nothing a caller can send produces one.
const customSeries = "goanna_custom"

// Labels the custom encoding uses. A caller's dimension "Foo" is stored as
// "dim_Foo", so a dimension can never collide with a reserved label.
const (
	labelNamespace  = "namespace"
	labelMetricName = "metric_name"
	labelUnit       = "unit"
	dimensionPrefix = "dim_"
)

// reservedNamespacePrefixes may not be written by PutMetricData. AWS reserves
// its own; Goanna/ is reserved for the same reason, so a tenant cannot forge
// collector-published guest metrics.
var reservedNamespacePrefixes = []string{"AWS/", "Goanna/"}

// isReservedNamespace reports whether PutMetricData must refuse the namespace.
func isReservedNamespace(namespace string) bool {
	for _, prefix := range reservedNamespacePrefixes {
		if strings.HasPrefix(namespace, prefix) {
			return true
		}
	}
	return false
}

// validDimensionName bounds custom dimension names to what a Prometheus label
// name accepts. AWS is more permissive, so this is a divergence: a dimension
// with a hyphen or a dot is rejected at PutMetricData rather than stored under
// a mangled name that a later query could not address unambiguously.
var validDimensionName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// byRequest indexes the table by the (namespace, metric name) a caller asks
// for; bySeries indexes it by the stored series name.
var (
	byRequest = map[[2]string]metric{}
	bySeries  = map[string]metric{}
)

func init() {
	for _, m := range metricTable {
		byRequest[[2]string{m.Namespace, m.Name}] = m
		bySeries[m.Series] = m
	}
}

// lookupMetric resolves a requested metric to its stored series. Unknown
// namespaces and names are not errors: CloudWatch answers a query for a metric
// it has never seen with an empty result, not a failure.
func lookupMetric(namespace, name string) (metric, bool) {
	m, ok := byRequest[[2]string{namespace, name}]
	return m, ok
}

// lookupSeries resolves a stored series back to the metric it projects as.
func lookupSeries(series string) (metric, bool) {
	m, ok := bySeries[series]
	return m, ok
}

// servedNamespaces returns the namespaces goanna knows about, sorted.
func servedNamespaces() []string {
	seen := map[string]struct{}{}
	for _, m := range metricTable {
		seen[m.Namespace] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for ns := range seen {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

// metricsInNamespace returns every metric in a namespace, in table order. An
// empty namespace means all of them.
func metricsInNamespace(namespace string) []metric {
	out := make([]metric, 0, len(metricTable))
	for _, m := range metricTable {
		if namespace == "" || m.Namespace == namespace {
			out = append(out, m)
		}
	}
	return out
}
