package store

import (
	"errors"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

const (
	acctA = "111122223333"
	acctB = "999988887777"
)

// seedTwoAccounts stores one identically-named series per account, so a query
// that leaks across the boundary is visible as an extra result rather than as
// a different metric.
func seedTwoAccounts(t *testing.T, s *Store) {
	t.Helper()
	for _, tc := range []struct {
		account string
		value   float64
	}{{acctA, 1}, {acctB, 2}} {
		point := Point{
			Name:      "goanna_ec2_cpu_utilization",
			Labels:    map[string]string{"instance_id": "i-shared", "namespace": "AWS/EC2"},
			Timestamp: tsMS,
			Value:     tc.value,
		}
		if err := s.AppendTenant(t.Context(), tc.account, []Point{point}); err != nil {
			t.Fatalf("append for %s: %v", tc.account, err)
		}
	}
}

// selectValues runs a scoped query and returns the values it can see.
func selectValues(t *testing.T, q *TenantQuerier, matchers ...*labels.Matcher) []float64 {
	t.Helper()
	var out []float64
	set := q.Select(t.Context(), false, nil, matchers...)
	for set.Next() {
		it := set.At().Iterator(nil)
		for it.Next() != 0 {
			_, v := it.At()
			out = append(out, v)
		}
		if err := it.Err(); err != nil {
			t.Fatalf("iterate: %v", err)
		}
	}
	if err := set.Err(); err != nil {
		t.Fatalf("select: %v", err)
	}
	return out
}

func openTenantQuerier(t *testing.T, s *Store, account string) *TenantQuerier {
	t.Helper()
	q, err := s.TenantQuerier(account, tsMS-1000, tsMS+1000)
	if err != nil {
		t.Fatalf("tenant querier for %s: %v", account, err)
	}
	t.Cleanup(func() {
		if err := q.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return q
}

func TestTenantQuerierSeesOnlyItsOwnAccount(t *testing.T) {
	s := openTestStore(t)
	seedTwoAccounts(t, s)

	name := labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "goanna_ec2_cpu_utilization")
	got := selectValues(t, openTenantQuerier(t, s, acctA), name)

	if len(got) != 1 || got[0] != 1 {
		t.Errorf("account A saw %v, want only its own sample [1]", got)
	}
}

// The whole point of the type: no matcher a caller can influence widens the
// scope, because Prometheus ANDs matchers and the scope is one of them.
func TestTenantQuerierScopeCannotBeWidened(t *testing.T) {
	s := openTestStore(t)
	seedTwoAccounts(t, s)
	q := openTenantQuerier(t, s, acctA)

	tests := []struct {
		name    string
		matcher *labels.Matcher
	}{
		{
			"naming the other account",
			labels.MustNewMatcher(labels.MatchEqual, LabelAccountID, acctB),
		},
		{
			"matching every account",
			labels.MustNewMatcher(labels.MatchRegexp, LabelAccountID, ".*"),
		},
		{
			"excluding nothing",
			labels.MustNewMatcher(labels.MatchNotEqual, LabelAccountID, "no-such-account"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, v := range selectValues(t, q, tt.matcher) {
				if v == 2 {
					t.Fatalf("matcher %s reached account B's sample", tt.matcher)
				}
			}
		})
	}
}

// An empty account would scope to the series carrying no account label at all,
// which is every series a producer published without one.
func TestTenantQuerierRejectsEmptyAccount(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.TenantQuerier("", 0, 1); !errors.Is(err, ErrNoAccount) {
		t.Errorf("error = %v, want ErrNoAccount", err)
	}
}

func TestAppendTenantRejectsEmptyAccount(t *testing.T) {
	s := openTestStore(t)
	err := s.AppendTenant(t.Context(), "", []Point{{Name: "x", Timestamp: tsMS, Value: 1}})
	if !errors.Is(err, ErrNoAccount) {
		t.Errorf("error = %v, want ErrNoAccount", err)
	}
}

// A caller-supplied account_id must not decide where the sample lands.
func TestAppendTenantIgnoresCallerSuppliedAccount(t *testing.T) {
	s := openTestStore(t)
	point := Point{
		Name:      "goanna_custom",
		Labels:    map[string]string{LabelAccountID: acctB, "namespace": "Tenant/App"},
		Timestamp: tsMS,
		Value:     42,
	}
	if err := s.AppendTenant(t.Context(), acctA, []Point{point}); err != nil {
		t.Fatalf("append: %v", err)
	}

	name := labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "goanna_custom")
	if got := selectValues(t, openTenantQuerier(t, s, acctB), name); len(got) != 0 {
		t.Errorf("account B saw %v; the payload's account_id was honoured", got)
	}
	if got := selectValues(t, openTenantQuerier(t, s, acctA), name); len(got) != 1 || got[0] != 42 {
		t.Errorf("account A saw %v, want [42]", got)
	}
}

func TestAppendTenantSkipsUnnamedPoints(t *testing.T) {
	s := openTestStore(t)
	points := []Point{
		{Name: "", Timestamp: tsMS, Value: 1},
		{Name: "goanna_custom", Timestamp: tsMS, Value: 7},
	}
	if err := s.AppendTenant(t.Context(), acctA, points); err != nil {
		t.Fatalf("append: %v", err)
	}

	name := labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "goanna_custom")
	if got := selectValues(t, openTenantQuerier(t, s, acctA), name); len(got) != 1 {
		t.Errorf("samples = %v, want the one named point", got)
	}
}
