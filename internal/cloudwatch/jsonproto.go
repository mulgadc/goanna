package cloudwatch

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// CloudWatch is served over two wire protocols. The original query protocol
// posts a form and answers XML; current AWS SDKs post AWS JSON 1.0 with the
// action in X-Amz-Target and expect JSON back. Both are accepted, because
// which one a client uses depends only on its SDK version.
type wire int

const (
	wireQuery wire = iota
	wireJSON
)

const (
	targetHeader     = "X-Amz-Target"
	queryModeHeader  = "x-amzn-query-mode"
	queryErrorHeader = "x-amzn-query-error"
	jsonContentType  = "application/x-amz-json-1.0"
)

// maxJSONBody bounds a request body. PutMetricData carries at most 1000 data
// points, so a megabyte is generous.
const maxJSONBody = 1 << 20

// wireOf picks the protocol from the request. X-Amz-Target is the marker: the
// query protocol never sets it.
func wireOf(r *http.Request) wire {
	if r.Header.Get(targetHeader) != "" {
		return wireJSON
	}
	return wireQuery
}

// jsonAction reads the action from X-Amz-Target, which is
// "GraniteServiceVersion20100801.ListMetrics" — the service prefix is the
// wire-level name for CloudWatch and carries no meaning here.
func jsonAction(r *http.Request) string {
	target := r.Header.Get(targetHeader)
	if i := strings.LastIndex(target, "."); i >= 0 {
		return target[i+1:]
	}
	return target
}

// decodeJSONBody flattens a JSON request into the same key space the query
// protocol uses, so both protocols share one set of handlers and one set of
// validation rules.
func decodeJSONBody(r *http.Request) (params, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxJSONBody+1))
	if err != nil {
		return nil, senderError("MalformedQueryString",
			"The request body could not be read.", http.StatusBadRequest)
	}
	if len(body) > maxJSONBody {
		return nil, senderError("RequestEntityTooLarge",
			"The request payload is too large.", http.StatusRequestEntityTooLarge)
	}

	values := url.Values{}
	if len(strings.TrimSpace(string(body))) == 0 {
		return params(values), nil
	}

	var doc map[string]any
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return nil, senderError("MalformedQueryString",
			"The request body is not valid JSON.", http.StatusBadRequest)
	}
	for key, value := range doc {
		flattenJSON(values, key, value)
	}
	return params(values), nil
}

// flattenJSON writes one JSON value into query-protocol keys: an object
// becomes "prefix.Field" and a list becomes "prefix.member.N", both 1-based,
// which is exactly what the form parsers already read.
func flattenJSON(out url.Values, prefix string, value any) {
	switch v := value.(type) {
	case map[string]any:
		for key, inner := range v {
			flattenJSON(out, prefix+"."+key, inner)
		}
	case []any:
		for i, inner := range v {
			flattenJSON(out, fmt.Sprintf("%s.member.%d", prefix, i+1), inner)
		}
	case string:
		out.Set(prefix, v)
	case json.Number:
		out.Set(prefix, v.String())
	case bool:
		out.Set(prefix, strconv.FormatBool(v))
	case nil:
		// An explicit null is the same as an absent field.
	default:
		out.Set(prefix, fmt.Sprint(v))
	}
}

// writeJSON emits a JSON response body.
func writeJSON(w http.ResponseWriter, status int, doc any) error {
	w.Header().Set("Content-Type", jsonContentType)
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(doc)
}

// jsonError is the AWS JSON error shape. Clients in query mode read the code
// from the query-error header, so both are set.
type jsonError struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}
