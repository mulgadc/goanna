// Package wire decodes the metric batches spinifex publishes on
// metrics.ec2.<instance-id>.
//
// These structs deliberately duplicate spinifex's own. The contract across
// this boundary is the JSON on the subject, not a Go type: importing
// spinifex to share the definition would put a module edge on the stack's
// most volatile repo in exchange for two struct declarations, and the
// agreement it buys is already covered by round-trip tests on both sides.
package wire

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// SubjectPrefix is the subject family the collector publishes on. The
// instance id is the token after it.
const SubjectPrefix = "metrics.ec2."

// Errors returned by Decode and Validate.
var (
	ErrNoSeries            = errors.New("batch carries no series")
	ErrNoTimestamp         = errors.New("batch has no timestamp")
	ErrTimestampNotSeconds = errors.New("batch timestamp is not Unix seconds")
)

// The producer emits time.Time.Unix(), so a plausible batch timestamp is a
// second count. These bounds exist to catch the producer switching units:
// milliseconds would sail through as a year-58000 timestamp, and every
// sample would be stored somewhere no query looks.
const (
	minPlausibleTS = 1_000_000_000  // 2001-09-09
	maxPlausibleTS = 10_000_000_000 // 2286-11-20
)

// Series is one CloudWatch-mappable datapoint.
type Series struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
	Value  float64           `json:"value"`
	Unit   string            `json:"unit,omitempty"`
}

// Batch is one collection tick for one instance.
type Batch struct {
	// TS is Unix SECONDS, which is what the collector publishes. The TSDB
	// appender wants milliseconds, so nothing may pass this straight to it —
	// use TimestampMS.
	TS            int64    `json:"ts"`
	PeriodSeconds int      `json:"period_seconds"`
	Node          string   `json:"node,omitempty"`
	Series        []Series `json:"series"`
}

// Decode parses a published payload.
//
// Unknown fields are ignored on purpose. The producer can start emitting a
// ninth series before Goanna has heard of it, and rejecting the batch over
// one unrecognised key would discard the eight that were understood.
func Decode(payload []byte) (Batch, error) {
	var b Batch
	if err := json.Unmarshal(payload, &b); err != nil {
		return Batch{}, fmt.Errorf("decode metric batch: %w", err)
	}
	return b, nil
}

// Validate reports whether a decoded batch is worth appending. It is
// separate from Decode so a malformed payload and an empty one can be
// told apart in the logs.
func (b Batch) Validate() error {
	if b.TS <= 0 {
		return ErrNoTimestamp
	}
	if b.TS < minPlausibleTS || b.TS > maxPlausibleTS {
		return fmt.Errorf("%w: %d", ErrTimestampNotSeconds, b.TS)
	}
	if len(b.Series) == 0 {
		return ErrNoSeries
	}
	return nil
}

// Time is the batch's collection time.
func (b Batch) Time() time.Time {
	return time.Unix(b.TS, 0).UTC()
}

// TimestampMS is the batch time in milliseconds, the unit the TSDB indexes
// on. Appending b.TS directly stores every sample in 1970.
func (b Batch) TimestampMS() int64 {
	return b.TS * 1000
}

// InstanceIDFromSubject returns the instance the subject carries metrics
// for. The collector publishes one instance per subject, so this is the
// authoritative id even when a series omits the label.
func InstanceIDFromSubject(subject string) (string, bool) {
	id, ok := strings.CutPrefix(subject, SubjectPrefix)
	if !ok || id == "" || strings.Contains(id, ".") {
		return "", false
	}
	return id, true
}
