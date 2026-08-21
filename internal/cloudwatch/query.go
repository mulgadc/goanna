package cloudwatch

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// params is a decoded AWS query-protocol form. Lists are flattened into keys
// like "Dimensions.member.1.Name", so lookups are prefix walks rather than
// struct decoding.
type params url.Values

func (p params) get(key string) string {
	if v, ok := p[key]; ok && len(v) > 0 {
		return strings.TrimSpace(v[0])
	}
	return ""
}

// memberIndexes returns the 1-based list indexes present under prefix, sorted
// ascending. Both "prefix.member.N" (what the SDKs emit) and the flat
// "prefix.N" form are accepted, since some older clients send the latter.
func (p params) memberIndexes(prefix string) []int {
	seen := map[int]struct{}{}
	for _, form := range []string{prefix + ".member.", prefix + "."} {
		for key := range p {
			rest, ok := strings.CutPrefix(key, form)
			if !ok {
				continue
			}
			// The index runs to the next separator, or to the end for a list
			// of scalars such as Statistics.member.1.
			if dot := strings.IndexByte(rest, '.'); dot >= 0 {
				rest = rest[:dot]
			}
			n, err := strconv.Atoi(rest)
			if err != nil || n < 1 {
				continue
			}
			seen[n] = struct{}{}
		}
	}
	out := make([]int, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Ints(out)
	return out
}

// memberKey builds the key for one element of a list, preferring whichever
// form the request actually used.
func (p params) memberKey(prefix string, index int, suffix string) string {
	member := fmt.Sprintf("%s.member.%d", prefix, index)
	flat := fmt.Sprintf("%s.%d", prefix, index)
	if suffix != "" {
		member += "." + suffix
		flat += "." + suffix
	}
	if _, ok := p[member]; ok {
		return member
	}
	if _, ok := p[flat]; ok {
		return flat
	}
	return member
}

// memberPrefix is memberKey for a nested list, where the caller appends the
// rest of the path itself.
func (p params) memberPrefix(prefix string, index int) string {
	member := fmt.Sprintf("%s.member.%d", prefix, index)
	flat := fmt.Sprintf("%s.%d", prefix, index)
	for key := range p {
		if strings.HasPrefix(key, member+".") {
			return member
		}
		if strings.HasPrefix(key, flat+".") {
			return flat
		}
	}
	return member
}

// strings returns a scalar list such as Statistics.member.N.
func (p params) strings(prefix string) []string {
	idx := p.memberIndexes(prefix)
	out := make([]string, 0, len(idx))
	for _, n := range idx {
		if v := p.get(p.memberKey(prefix, n, "")); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// dimension is one CloudWatch dimension as supplied by a caller.
type dimension struct {
	Name  string
	Value string
}

// dimensions parses a Dimensions list under prefix.
func (p params) dimensions(prefix string) []dimension {
	idx := p.memberIndexes(prefix)
	out := make([]dimension, 0, len(idx))
	for _, n := range idx {
		name := p.get(p.memberKey(prefix, n, "Name"))
		if name == "" {
			continue
		}
		out = append(out, dimension{Name: name, Value: p.get(p.memberKey(prefix, n, "Value"))})
	}
	return out
}

// timestamp reads a CloudWatch time parameter. AWS accepts ISO 8601 and epoch
// seconds on the query protocol, and the SDKs send the former.
func (p params) timestamp(key string) (time.Time, bool, error) {
	raw := p.get(key)
	if raw == "" {
		return time.Time{}, false, nil
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC(), true, nil
	}
	// Epoch seconds, possibly fractional.
	if secs, err := strconv.ParseFloat(raw, 64); err == nil {
		nanos := int64(secs * float64(time.Second))
		return time.Unix(0, nanos).UTC(), true, nil
	}
	return time.Time{}, false, invalidParameter(key, fmt.Sprintf("%q is not a valid timestamp", raw))
}

func (p params) float(key string) (float64, bool, error) {
	raw := p.get(key)
	if raw == "" {
		return 0, false, nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false, invalidParameter(key, fmt.Sprintf("%q is not a number", raw))
	}
	return v, true, nil
}

func (p params) int(key string) (int, bool, error) {
	raw := p.get(key)
	if raw == "" {
		return 0, false, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false, invalidParameter(key, fmt.Sprintf("%q is not an integer", raw))
	}
	return v, true, nil
}

func (p params) bool(key string) (bool, bool) {
	raw := p.get(key)
	if raw == "" {
		return false, false
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, false
	}
	return v, true
}
