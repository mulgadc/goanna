package cloudwatch

import (
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

// target is a caller's (namespace, metric, dimensions) resolved onto stored
// series.
//
// empty means the request named something that cannot exist — an unknown
// metric, or a dimension the collector does not publish. CloudWatch answers
// those with no datapoints rather than an error, so the handlers return an
// empty result instead of failing.
type target struct {
	metric metric
	// unit is what the response reports. A locked metric's unit is fixed by
	// the table; a custom metric carries its own, so this is empty until a
	// matching series supplies one.
	unit     string
	matchers []*labels.Matcher
	// dimCount is the number of dim_* labels a custom series must carry to
	// match. CloudWatch matches the complete dimension set, so a series with
	// extra dimensions is a different metric, and equality matchers alone
	// cannot express that.
	dimCount int
	custom   bool
	empty    bool
}

// resolveTarget maps a request onto the series that answer it. Caller input
// reaches label matchers only through this function, and only through the
// allowlist below — a dimension name can never become a raw label name, and
// so can never become account_id.
func resolveTarget(namespace, metricName, unit string, dims []dimension) (target, error) {
	if namespace == "" {
		return target{}, missingParameter("Namespace")
	}
	if metricName == "" {
		return target{}, missingParameter("MetricName")
	}

	if m, ok := lookupMetric(namespace, metricName); ok {
		return resolveLockedTarget(m, unit, dims)
	}
	// An unknown metric in a namespace goanna owns has no data by definition.
	if isReservedNamespace(namespace) {
		return target{empty: true}, nil
	}
	return resolveCustomTarget(namespace, metricName, unit, dims)
}

// resolveLockedTarget resolves a collector-published metric. The collector
// emits exactly one dimension, so any other dimension set names a metric that
// was never published.
func resolveLockedTarget(m metric, unit string, dims []dimension) (target, error) {
	// The collector's unit is fixed per metric, so a filter on any other unit
	// selects nothing.
	if unit != "" && unit != m.Unit {
		return target{empty: true}, nil
	}

	t := target{metric: m, unit: m.Unit, matchers: []*labels.Matcher{
		mustEqual(model.MetricNameLabel, m.Series),
	}}

	if len(dims) != 1 || dims[0].Name != DimensionInstanceID {
		// Matches AWS: a metric is identified by its complete dimension set,
		// so an incomplete or unrecognised one selects nothing.
		return target{empty: true}, nil
	}
	if dims[0].Value == "" {
		return target{empty: true}, nil
	}

	instance, err := labels.NewMatcher(labels.MatchEqual, labelInstanceID, dims[0].Value)
	if err != nil {
		return target{}, invalidParameter("Dimensions", err.Error())
	}
	t.matchers = append(t.matchers, instance)
	return t, nil
}

// resolveCustomTarget resolves a metric written by PutMetricData.
func resolveCustomTarget(namespace, metricName, unit string, dims []dimension) (target, error) {
	t := target{
		custom:   true,
		unit:     unit,
		dimCount: len(dims),
		metric:   metric{Namespace: namespace, Name: metricName, Series: customSeries},
		matchers: []*labels.Matcher{
			mustEqual(model.MetricNameLabel, customSeries),
			mustEqual(labelNamespace, namespace),
			mustEqual(labelMetricName, metricName),
		},
	}
	// A custom metric carries the unit it was published with, so a filter is
	// an ordinary label match rather than a table comparison.
	if unit != "" {
		t.matchers = append(t.matchers, mustEqual(labelUnit, unit))
	}

	seen := make(map[string]struct{}, len(dims))
	for _, d := range dims {
		if !validDimensionName.MatchString(d.Name) {
			// Nothing could have been written under this name, so it selects
			// nothing rather than failing the read.
			return target{empty: true}, nil
		}
		if _, dup := seen[d.Name]; dup {
			return target{}, invalidParameter("Dimensions",
				fmt.Sprintf("dimension %s is repeated", d.Name))
		}
		seen[d.Name] = struct{}{}

		m, err := labels.NewMatcher(labels.MatchEqual, dimensionPrefix+d.Name, d.Value)
		if err != nil {
			return target{}, invalidParameter("Dimensions", err.Error())
		}
		t.matchers = append(t.matchers, m)
	}
	return t, nil
}

// matchesDimensionSet reports whether a stored series carries exactly the
// dimensions the request named. Only custom series need it: the locked series
// carry a fixed label set already pinned by the matchers.
func (t target) matchesDimensionSet(lset labels.Labels) bool {
	if !t.custom {
		return true
	}
	var count int
	lset.Range(func(l labels.Label) {
		if len(l.Name) > len(dimensionPrefix) && l.Name[:len(dimensionPrefix)] == dimensionPrefix {
			count++
		}
	})
	return count == t.dimCount
}

// dimensionsOf projects a stored series back onto CloudWatch dimensions.
func dimensionsOf(lset labels.Labels, custom bool) []dimension {
	if !custom {
		if id := lset.Get(labelInstanceID); id != "" {
			return []dimension{{Name: DimensionInstanceID, Value: id}}
		}
		return nil
	}

	var out []dimension
	lset.Range(func(l labels.Label) {
		if name, ok := cutDimensionPrefix(l.Name); ok {
			out = append(out, dimension{Name: name, Value: l.Value})
		}
	})
	return out
}

func cutDimensionPrefix(name string) (string, bool) {
	if len(name) <= len(dimensionPrefix) || name[:len(dimensionPrefix)] != dimensionPrefix {
		return "", false
	}
	return name[len(dimensionPrefix):], true
}

// mustEqual builds an equality matcher for a name this package controls.
// labels.NewMatcher only fails on an invalid regexp, which an equality matcher
// has none of.
func mustEqual(name, value string) *labels.Matcher {
	m, err := labels.NewMatcher(labels.MatchEqual, name, value)
	if err != nil {
		panic(fmt.Sprintf("cloudwatch: matcher %s=%q: %v", name, value, err))
	}
	return m
}
