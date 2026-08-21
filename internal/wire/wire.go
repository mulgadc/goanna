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
	ErrNoSeries    = errors.New("batch carries no series")
	ErrNoTimestamp = errors.New("batch has no timestamp")
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
	// TS is milliseconds since the epoch, matching what the TSDB appender
	// wants and what the producer emits.
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
	if len(b.Series) == 0 {
		return ErrNoSeries
	}
	return nil
}

// Time is the batch's collection time.
func (b Batch) Time() time.Time {
	return time.UnixMilli(b.TS)
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
