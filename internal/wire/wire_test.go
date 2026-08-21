package wire

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
)

// The fixture is a payload captured off the live metrics.ec2.> subject, not
// one built from spinifex's types. A hand-built fixture agrees on field
// names while saying nothing about their units, which is how a seconds
// timestamp got fed to a millisecond appender. Recapture from a running
// node when the producer changes; the diff is the drift.
func loadFixture(t *testing.T) []byte {
	t.Helper()
	payload, err := os.ReadFile("testdata/batch.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return payload
}

func TestDecodeProducerPayload(t *testing.T) {
	b, err := Decode(loadFixture(t))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := b.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	if b.PeriodSeconds != 300 {
		t.Errorf("period = %d, want 300", b.PeriodSeconds)
	}
	if b.Node != "ironbark" {
		t.Errorf("node = %q, want ironbark", b.Node)
	}
	if got, want := len(b.Series), 7; got != want {
		t.Fatalf("series = %d, want %d", got, want)
	}

	first := b.Series[0]
	if first.Name != "goanna_ec2_cpu_utilization" {
		t.Errorf("name = %q", first.Name)
	}
	if first.Value != 5.719990771824487 {
		t.Errorf("value = %v, want 5.719990771824487", first.Value)
	}
	if first.Unit != "Percent" {
		t.Errorf("unit = %q, want Percent", first.Unit)
	}
	for _, key := range []string{"namespace", "instance_id", "account_id"} {
		if first.Labels[key] == "" {
			t.Errorf("label %q missing", key)
		}
	}
}

// Every series the producer emits must survive the decode. A silently
// dropped series is the failure this whole boundary risks.
func TestDecodeKeepsEverySeries(t *testing.T) {
	b, err := Decode(loadFixture(t))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := []string{
		"goanna_ec2_cpu_utilization",
		"goanna_ec2_network_in_bytes",
		"goanna_ec2_network_out_bytes",
		"goanna_ec2_disk_read_bytes",
		"goanna_ec2_disk_write_bytes",
		"goanna_ec2_disk_read_ops",
		"goanna_ec2_disk_write_ops",
	}
	got := make(map[string]bool, len(b.Series))
	for _, s := range b.Series {
		got[s.Name] = true
	}
	for _, name := range want {
		if !got[name] {
			t.Errorf("series %q not decoded", name)
		}
	}
}

// A field Goanna has never heard of must not cost the batch. The producer
// can ship a ninth series before this repo is rebuilt.
func TestDecodeIgnoresUnknownFields(t *testing.T) {
	var raw map[string]any
	if err := json.Unmarshal(loadFixture(t), &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	raw["future_field"] = "whatever"
	series := raw["series"].([]any)
	series[0].(map[string]any)["future_series_field"] = 1
	extended, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	b, err := Decode(extended)
	if err != nil {
		t.Fatalf("decode with unknown fields: %v", err)
	}
	if len(b.Series) != 7 {
		t.Errorf("series = %d, want 7", len(b.Series))
	}
	if b.Series[0].Value != 5.719990771824487 {
		t.Errorf("known field lost: value = %v", b.Series[0].Value)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name  string
		batch Batch
		want  error
	}{
		{"no timestamp", Batch{Series: []Series{{Name: "x"}}}, ErrNoTimestamp},
		{"no series", Batch{TS: 1787284904}, ErrNoSeries},
		{"ok", Batch{TS: 1787284904, Series: []Series{{Name: "x"}}}, nil},
		// The producer switching to milliseconds must fail loudly. Stored as
		// seconds it is a year-58000 sample that no query ever returns.
		{"milliseconds", Batch{TS: 1787284904000, Series: []Series{{Name: "x"}}}, ErrTimestampNotSeconds},
		{"before 2001", Batch{TS: 999_999_999, Series: []Series{{Name: "x"}}}, ErrTimestampNotSeconds},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.batch.Validate(); !errors.Is(got, tt.want) {
				t.Errorf("Validate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	if _, err := Decode([]byte("{not json")); err == nil {
		t.Fatal("want error for malformed payload")
	}
}

func TestInstanceIDFromSubject(t *testing.T) {
	tests := []struct {
		subject string
		want    string
		ok      bool
	}{
		{"metrics.ec2.i-0abc", "i-0abc", true},
		{"metrics.ec2.", "", false},
		{"metrics.ec2.i-0abc.extra", "", false},
		{"metrics.rds.db-1", "", false},
		{"", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.subject, func(t *testing.T) {
			got, ok := InstanceIDFromSubject(tt.subject)
			if got != tt.want || ok != tt.ok {
				t.Errorf("= (%q, %v), want (%q, %v)", got, ok, tt.want, tt.ok)
			}
		})
	}
}

// The TSDB indexes in milliseconds and the wire carries seconds. Getting
// this wrong files every sample in 1970, where nothing looks for it.
func TestTimestampMS(t *testing.T) {
	b, err := Decode(loadFixture(t))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got, want := b.TimestampMS(), b.TS*1000; got != want {
		t.Errorf("TimestampMS() = %d, want %d", got, want)
	}
	if b.TimestampMS() == b.TS {
		t.Error("TimestampMS returned the raw seconds value")
	}
}

func TestBatchTime(t *testing.T) {
	b, err := Decode(loadFixture(t))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got := b.Time().UTC().Format("2006-01-02T15:04:05Z"); got != "2026-08-21T04:01:44Z" {
		t.Errorf("Time() = %s", got)
	}
}
