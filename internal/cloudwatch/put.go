package cloudwatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/prometheus/prometheus/storage"

	"github.com/mulgadc/goanna/internal/store"
)

// CloudWatch's own limits on a PutMetricData call.
const (
	maxMetricDataMembers = 1000
	maxPutDimensions     = 30
	maxNameLength        = 255
	// A sample may not be older than this or further ahead than
	// maxFutureTimestamp. The TSDB head enforces a tighter bound of its own,
	// but rejecting here names the parameter instead of surfacing a storage
	// error.
	maxPastTimestamp   = 14 * 24 * time.Hour
	maxFutureTimestamp = 2 * time.Hour
)

// putMetricData stores custom metrics on behalf of the caller's account.
func (s *Server) putMetricData(ctx context.Context, form params, identity Identity,
	requestID string) (any, error) {
	namespace := form.get("Namespace")
	if namespace == "" {
		return nil, missingParameter("Namespace")
	}
	if len(namespace) > maxNameLength {
		return nil, invalidParameter("Namespace", "must be 255 characters or fewer")
	}
	// AWS reserves its own namespaces; Goanna/ is reserved for the same
	// reason. Without this a tenant could inject fabricated AWS/EC2 datapoints
	// for their own instances that nothing downstream could tell from
	// collector data.
	if isReservedNamespace(namespace) {
		return nil, invalidParameter("Namespace",
			fmt.Sprintf("%s is a reserved namespace", namespace))
	}

	points, err := s.parseMetricData(form, namespace)
	if err != nil {
		return nil, err
	}

	if len(points) > 0 {
		if err := s.store.AppendTenant(ctx, identity.AccountID, points); err != nil {
			return nil, storeWriteError(s.log, err)
		}
	}

	resp := putMetricDataResponse{Namespace: xmlNamespace}
	resp.Metadata.RequestID = requestID
	return resp, nil
}

func (s *Server) parseMetricData(form params, namespace string) ([]store.Point, error) {
	const prefix = "MetricData"
	indexes := form.memberIndexes(prefix)
	if len(indexes) == 0 {
		return nil, missingParameter(prefix)
	}
	if len(indexes) > maxMetricDataMembers {
		return nil, invalidParameter(prefix,
			fmt.Sprintf("a request may carry at most %d metrics", maxMetricDataMembers))
	}

	now := s.now().UTC()
	points := make([]store.Point, 0, len(indexes))
	for _, n := range indexes {
		base := form.memberPrefix(prefix, n)
		point, err := parseMetricDatum(form, base, namespace, now)
		if err != nil {
			return nil, err
		}
		points = append(points, point)
	}
	return points, nil
}

func parseMetricDatum(form params, base, namespace string, now time.Time) (store.Point, error) {
	name := form.get(base + ".MetricName")
	if name == "" {
		return store.Point{}, missingParameter(base + ".MetricName")
	}
	if len(name) > maxNameLength {
		return store.Point{}, invalidParameter(base+".MetricName", "must be 255 characters or fewer")
	}

	// A pre-aggregated summary cannot be stored as one float, and expanding
	// Values/Counts would append several samples to one timestamp, which the
	// TSDB rejects. Both are refused rather than silently reduced to a number
	// the caller did not send.
	if len(form.memberIndexes(base+".Values")) > 0 {
		return store.Point{}, invalidParameter(base+".Values",
			"value arrays are not supported; send one Value per datapoint")
	}
	for _, field := range []string{"SampleCount", "Sum", "Minimum", "Maximum"} {
		if form.get(base+".StatisticValues."+field) != "" {
			return store.Point{}, invalidParameter(base+".StatisticValues",
				"statistic sets are not supported; send one Value per datapoint")
		}
	}

	value, ok, err := form.float(base + ".Value")
	if err != nil {
		return store.Point{}, err
	}
	if !ok {
		return store.Point{}, missingParameter(base + ".Value")
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return store.Point{}, invalidParameter(base+".Value", "must be a finite number")
	}

	ts, ok, err := form.timestamp(base + ".Timestamp")
	if err != nil {
		return store.Point{}, err
	}
	if !ok {
		ts = now
	}
	if ts.Before(now.Add(-maxPastTimestamp)) || ts.After(now.Add(maxFutureTimestamp)) {
		return store.Point{}, invalidParameter(base+".Timestamp",
			"must be within the last two weeks and no more than two hours ahead")
	}

	lset, err := datumLabels(form, base, namespace, name)
	if err != nil {
		return store.Point{}, err
	}

	return store.Point{
		Name:      customSeries,
		Labels:    lset,
		Timestamp: ts.UnixMilli(),
		Value:     value,
	}, nil
}

// datumLabels builds the label set one datum is stored under. account_id is
// deliberately absent: AppendTenant stamps it from the verified signature, so
// nothing a caller sends can influence which tenant the sample lands under.
func datumLabels(form params, base, namespace, name string) (map[string]string, error) {
	lset := map[string]string{
		labelNamespace:  namespace,
		labelMetricName: name,
	}
	if unit := form.get(base + ".Unit"); unit != "" {
		lset[labelUnit] = unit
	}

	dims := form.dimensions(base + ".Dimensions")
	if len(dims) > maxPutDimensions {
		return nil, invalidParameter(base+".Dimensions",
			fmt.Sprintf("a metric may carry at most %d dimensions", maxPutDimensions))
	}
	for _, d := range dims {
		if !validDimensionName.MatchString(d.Name) {
			return nil, invalidParameter(base+".Dimensions",
				fmt.Sprintf("dimension name %q must start with a letter or underscore and "+
					"contain only letters, digits and underscores", d.Name))
		}
		if d.Value == "" {
			return nil, invalidParameter(base+".Dimensions",
				fmt.Sprintf("dimension %s has no value", d.Name))
		}
		key := dimensionPrefix + d.Name
		if _, dup := lset[key]; dup {
			return nil, invalidParameter(base+".Dimensions",
				fmt.Sprintf("dimension %s is repeated", d.Name))
		}
		lset[key] = d.Value
	}
	return lset, nil
}

// storeWriteError maps a rejected append onto the parameter that caused it.
// Timestamp problems are the caller's; anything else is ours.
func storeWriteError(log *slog.Logger, err error) error {
	switch {
	case errors.Is(err, storage.ErrOutOfBounds),
		errors.Is(err, storage.ErrOutOfOrderSample),
		errors.Is(err, storage.ErrDuplicateSampleForTimestamp):
		return senderError("InvalidParameterValue",
			"The parameter Timestamp is not valid: the datapoint is outside the writable "+
				"window or duplicates one already stored.", http.StatusBadRequest)
	default:
		return internalError(log, "PutMetricData", err)
	}
}
