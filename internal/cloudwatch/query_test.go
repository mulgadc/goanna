package cloudwatch

import (
	"net/url"
	"slices"
	"testing"
	"time"
)

func form(pairs ...string) params {
	values := url.Values{}
	for i := 0; i+1 < len(pairs); i += 2 {
		values.Set(pairs[i], pairs[i+1])
	}
	return params(values)
}

func TestMemberIndexesAcceptsBothListForms(t *testing.T) {
	// The SDKs emit ".member.N"; some older clients emit the flat ".N".
	member := form("Statistics.member.1", "Average", "Statistics.member.2", "Sum")
	flat := form("Statistics.1", "Average", "Statistics.2", "Sum")

	for name, p := range map[string]params{"member": member, "flat": flat} {
		t.Run(name, func(t *testing.T) {
			if got := p.memberIndexes("Statistics"); !slices.Equal(got, []int{1, 2}) {
				t.Errorf("indexes = %v, want [1 2]", got)
			}
			if got := p.strings("Statistics"); !slices.Equal(got, []string{"Average", "Sum"}) {
				t.Errorf("values = %v", got)
			}
		})
	}
}

// A sparse or unordered list must come back in numeric order, not in map or
// lexicographic order — "10" sorts before "2" as a string.
func TestMemberIndexesAreNumericallySorted(t *testing.T) {
	p := form(
		"Statistics.member.10", "Sum",
		"Statistics.member.2", "Average",
		"Statistics.member.1", "Minimum",
	)
	if got := p.memberIndexes("Statistics"); !slices.Equal(got, []int{1, 2, 10}) {
		t.Errorf("indexes = %v, want [1 2 10]", got)
	}
	if got := p.strings("Statistics"); !slices.Equal(got, []string{"Minimum", "Average", "Sum"}) {
		t.Errorf("values = %v", got)
	}
}

func TestMemberIndexesIgnoresNonIndexKeys(t *testing.T) {
	p := form("Statistics.member.x", "Sum", "Statistics.member.0", "Average", "StatisticsOther.member.1", "Min")
	if got := p.memberIndexes("Statistics"); len(got) != 0 {
		t.Errorf("indexes = %v, want none", got)
	}
}

func TestDimensions(t *testing.T) {
	p := form(
		"Dimensions.member.1.Name", "InstanceId",
		"Dimensions.member.1.Value", "i-abc",
		"Dimensions.member.2.Name", "Service",
		"Dimensions.member.2.Value", "api",
		// A member with no name is not a dimension.
		"Dimensions.member.3.Value", "orphan",
	)
	got := p.dimensions("Dimensions")
	if len(got) != 2 {
		t.Fatalf("dimensions = %v, want 2", got)
	}
	if got[0].Name != "InstanceId" || got[0].Value != "i-abc" {
		t.Errorf("first dimension = %v", got[0])
	}
	if got[1].Name != "Service" || got[1].Value != "api" {
		t.Errorf("second dimension = %v", got[1])
	}
}

func TestMemberPrefixFindsNestedLists(t *testing.T) {
	p := form("MetricDataQueries.member.1.Id", "m1")
	if got := p.memberPrefix("MetricDataQueries", 1); got != "MetricDataQueries.member.1" {
		t.Errorf("prefix = %s", got)
	}

	flat := form("MetricDataQueries.1.Id", "m1")
	if got := flat.memberPrefix("MetricDataQueries", 1); got != "MetricDataQueries.1" {
		t.Errorf("prefix = %s", got)
	}
}

func TestTimestamp(t *testing.T) {
	p := form(
		"Iso", "2026-08-21T01:02:03Z",
		"Offset", "2026-08-21T11:02:03+10:00",
		"Epoch", "1787284904",
		"Fractional", "1787284904.5",
		"Junk", "yesterday",
	)

	want := time.Date(2026, 8, 21, 1, 2, 3, 0, time.UTC)
	for _, key := range []string{"Iso", "Offset"} {
		got, ok, err := p.timestamp(key)
		if err != nil || !ok {
			t.Fatalf("%s: ok=%v err=%v", key, ok, err)
		}
		if !got.Equal(want) {
			t.Errorf("%s = %s, want %s", key, got, want)
		}
	}

	got, _, err := p.timestamp("Epoch")
	if err != nil || got.Unix() != 1787284904 {
		t.Errorf("Epoch = %s (%v)", got, err)
	}
	got, _, err = p.timestamp("Fractional")
	if err != nil || got.UnixMilli() != 1787284904500 {
		t.Errorf("Fractional = %s (%v)", got, err)
	}

	if _, ok, _ := p.timestamp("Missing"); ok {
		t.Error("an absent key reported as present")
	}
	_, _, err = p.timestamp("Junk")
	wantAPIError(t, err, "InvalidParameterValue")
}

func TestNumericAndBoolParams(t *testing.T) {
	p := form("Period", "300", "Value", "1.5", "Return", "false", "Bad", "x")

	if v, ok, err := p.int("Period"); err != nil || !ok || v != 300 {
		t.Errorf("Period = %v ok=%v err=%v", v, ok, err)
	}
	if v, ok, err := p.float("Value"); err != nil || !ok || v != 1.5 {
		t.Errorf("Value = %v ok=%v err=%v", v, ok, err)
	}
	if v, ok := p.bool("Return"); !ok || v {
		t.Errorf("Return = %v ok=%v", v, ok)
	}
	// An unparseable bool reports absent, so the caller falls back to its
	// default rather than treating garbage as false.
	if _, ok := p.bool("Bad"); ok {
		t.Error("a malformed bool reported as present")
	}

	_, _, err := p.int("Bad")
	wantAPIError(t, err, "InvalidParameterValue")
	_, _, err = p.float("Bad")
	wantAPIError(t, err, "InvalidParameterValue")
}

func TestWindow(t *testing.T) {
	p := form("StartTime", "2026-08-21T00:00:00Z", "EndTime", "2026-08-21T01:00:00Z")
	start, end, err := p.window("StartTime", "EndTime")
	if err != nil {
		t.Fatalf("window: %v", err)
	}
	if end.Sub(start) != time.Hour {
		t.Errorf("window = %s", end.Sub(start))
	}

	_, _, err = form("EndTime", "2026-08-21T01:00:00Z").window("StartTime", "EndTime")
	wantAPIError(t, err, "MissingParameter")
	_, _, err = form("StartTime", "2026-08-21T00:00:00Z").window("StartTime", "EndTime")
	wantAPIError(t, err, "MissingParameter")

	reversed := form("StartTime", "2026-08-21T01:00:00Z", "EndTime", "2026-08-21T00:00:00Z")
	_, _, err = reversed.window("StartTime", "EndTime")
	wantAPIError(t, err, "InvalidParameterCombination")
}
