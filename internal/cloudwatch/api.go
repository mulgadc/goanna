package cloudwatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/mulgadc/goanna/internal/store"
)

// listWindow is how far back ListMetrics looks for a series that has
// reported. CloudWatch uses two weeks.
const listWindow = 14 * 24 * time.Hour

// Identity is the caller a verified signature resolved to. AccountID is the
// tenant boundary; nothing downstream may take it from request content.
type Identity struct {
	AccountID   string
	AccessKeyID string
	UserName    string
}

type identityKey struct{}

// WithIdentity attaches a verified caller to ctx. Only the auth middleware
// should call this.
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

// IdentityFrom returns the verified caller, if the request was authenticated.
func IdentityFrom(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(identityKey{}).(Identity)
	return id, ok
}

// Options configures a Server.
type Options struct {
	Store  *store.Store
	Logger *slog.Logger
	// Now overrides the clock, for tests and for the default time windows.
	Now func() time.Time
}

// Server serves the CloudWatch metrics API over one TSDB.
type Server struct {
	store *store.Store
	log   *slog.Logger
	now   func() time.Time
}

var _ http.Handler = (*Server)(nil)

// New builds a Server. It requires a Store: every action either reads or
// writes one.
func New(opts Options) (*Server, error) {
	if opts.Store == nil {
		return nil, errors.New("cloudwatch: Store is required")
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Server{store: opts.Store, log: log, now: now}, nil
}

// ServeHTTP dispatches a CloudWatch request in either wire protocol. Every
// action funnels through here so the identity check cannot be skipped by one
// handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := uuid.NewString()
	proto := wireOf(r)

	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		writeError(w, s.log, requestID, proto, senderError("InvalidAction",
			"The request method is not supported.", http.StatusMethodNotAllowed))
		return
	}

	action, form, err := s.decode(r, proto)
	if err != nil {
		writeError(w, s.log, requestID, proto, err)
		return
	}

	// A request that reached here without an identity means the middleware was
	// not wired in. Refuse rather than serve one tenant's data unscoped.
	identity, ok := IdentityFrom(r.Context())
	if !ok || identity.AccountID == "" {
		writeError(w, s.log, requestID, proto, accessDenied("The request was not authenticated."))
		return
	}

	doc, err := s.dispatch(r.Context(), action, form, identity, requestID)
	if err != nil {
		writeError(w, s.log, requestID, proto, err)
		return
	}

	if proto == wireJSON {
		if err := writeJSON(w, http.StatusOK, toJSONResponse(doc)); err != nil {
			s.log.Warn("writing cloudwatch response", "action", action, "error", err)
		}
		return
	}

	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	if err := writeXML(w, doc); err != nil {
		s.log.Warn("writing cloudwatch response", "action", action, "error", err)
	}
}

// decode reads the action and its parameters out of whichever protocol the
// caller used. The body is read only once: ParseForm would consume a JSON body
// and leave nothing to decode.
func (s *Server) decode(r *http.Request, proto wire) (string, params, error) {
	if proto == wireJSON {
		form, err := decodeJSONBody(r)
		if err != nil {
			return "", nil, err
		}
		return jsonAction(r), form, nil
	}

	if err := r.ParseForm(); err != nil {
		return "", nil, senderError("MalformedQueryString",
			"The query string is malformed.", http.StatusBadRequest)
	}
	form := params(r.Form)
	return form.get("Action"), form, nil
}

func (s *Server) dispatch(ctx context.Context, action string, form params,
	identity Identity, requestID string) (any, error) {
	switch action {
	case "":
		return nil, missingParameter("Action")
	case "ListMetrics":
		return s.listMetrics(ctx, form, identity, requestID)
	case "GetMetricStatistics":
		return s.getMetricStatistics(ctx, form, identity, requestID)
	case "GetMetricData":
		return s.getMetricData(ctx, form, identity, requestID)
	case "PutMetricData":
		return s.putMetricData(ctx, form, identity, requestID)
	default:
		return nil, invalidAction(action)
	}
}

// querier opens a tenant-scoped querier. It is the only way the handlers reach
// the TSDB, so no handler can read outside its caller's account.
func (s *Server) querier(identity Identity, mint, maxt int64) (*store.TenantQuerier, error) {
	q, err := s.store.TenantQuerier(identity.AccountID, mint, maxt)
	if err != nil {
		if errors.Is(err, store.ErrNoAccount) {
			return nil, accessDenied("The request was not authenticated.")
		}
		return nil, err
	}
	return q, nil
}

// seriesLabels enumerates the label sets matching matchers without reading any
// samples.
func seriesLabels(ctx context.Context, q *store.TenantQuerier, mint, maxt int64,
	matchers []*labels.Matcher) ([]labels.Labels, error) {
	hints := &storage.SelectHints{Start: mint, End: maxt, Func: "series"}
	set := q.Select(ctx, false, hints, matchers...)

	var out []labels.Labels
	for set.Next() {
		out = append(out, set.At().Labels())
	}
	if err := set.Err(); err != nil {
		return nil, fmt.Errorf("enumerate series: %w", err)
	}
	return out, nil
}

// listMetrics reports the metrics that have reported inside the list window.
//
// The Dimensions parameter is a subset filter here, unlike GetMetricStatistics
// where the complete dimension set identifies the metric. That is CloudWatch's
// own asymmetry.
func (s *Server) listMetrics(ctx context.Context, form params, identity Identity,
	requestID string) (any, error) {
	namespace := form.get("Namespace")
	metricName := form.get("MetricName")
	dims := form.dimensions("Dimensions")

	now := s.now().UTC()
	mint := now.Add(-listWindow).UnixMilli()
	maxt := now.UnixMilli()

	q, err := s.querier(identity, mint, maxt)
	if err != nil {
		return nil, err
	}
	defer s.closeQuerier(q)

	found, err := s.collectMetrics(ctx, q, mint, maxt, namespace, metricName, dims)
	if err != nil {
		return nil, internalError(s.log, "ListMetrics", err)
	}

	resp := listMetricsResponse{Namespace: xmlNamespace}
	resp.Result.Metrics = found
	resp.Metadata.RequestID = requestID
	return resp, nil
}

// collectMetrics walks the locked series and, when the namespace filter allows
// it, the custom ones.
func (s *Server) collectMetrics(ctx context.Context, q *store.TenantQuerier, mint, maxt int64,
	namespace, metricName string, dims []dimension) ([]xmlMetric, error) {
	var out []xmlMetric

	if namespace == "" || !isReservedNamespace(namespace) {
		custom, err := s.collectCustomMetrics(ctx, q, mint, maxt, namespace, metricName, dims)
		if err != nil {
			return nil, err
		}
		out = append(out, custom...)
	}

	if namespace == "" || isReservedNamespace(namespace) {
		locked, err := s.collectLockedMetrics(ctx, q, mint, maxt, namespace, metricName, dims)
		if err != nil {
			return nil, err
		}
		out = append(out, locked...)
	}

	sortMetrics(out)
	return out, nil
}

func (s *Server) collectLockedMetrics(ctx context.Context, q *store.TenantQuerier, mint, maxt int64,
	namespace, metricName string, dims []dimension) ([]xmlMetric, error) {
	instanceFilter, ok := lockedDimensionFilter(dims)
	if !ok {
		// A dimension the collector never publishes matches nothing, so there
		// is no point querying for it.
		return nil, nil
	}

	var out []xmlMetric
	for _, m := range metricsInNamespace(namespace) {
		if metricName != "" && m.Name != metricName {
			continue
		}
		matchers := append([]*labels.Matcher{mustEqual(model.MetricNameLabel, m.Series)}, instanceFilter...)
		sets, err := seriesLabels(ctx, q, mint, maxt, matchers)
		if err != nil {
			return nil, err
		}
		for _, lset := range sets {
			out = append(out, xmlMetric{
				Namespace:  m.Namespace,
				MetricName: m.Name,
				Dimensions: toXMLDimensions(dimensionsOf(lset, false)),
			})
		}
	}
	return out, nil
}

func (s *Server) collectCustomMetrics(ctx context.Context, q *store.TenantQuerier, mint, maxt int64,
	namespace, metricName string, dims []dimension) ([]xmlMetric, error) {
	matchers := []*labels.Matcher{mustEqual(model.MetricNameLabel, customSeries)}
	if namespace != "" {
		matchers = append(matchers, mustEqual(labelNamespace, namespace))
	}
	if metricName != "" {
		matchers = append(matchers, mustEqual(labelMetricName, metricName))
	}
	for _, d := range dims {
		if !validDimensionName.MatchString(d.Name) {
			return nil, nil
		}
		matchers = append(matchers, mustEqual(dimensionPrefix+d.Name, d.Value))
	}

	sets, err := seriesLabels(ctx, q, mint, maxt, matchers)
	if err != nil {
		return nil, err
	}

	out := make([]xmlMetric, 0, len(sets))
	for _, lset := range sets {
		out = append(out, xmlMetric{
			Namespace:  lset.Get(labelNamespace),
			MetricName: lset.Get(labelMetricName),
			Dimensions: toXMLDimensions(dimensionsOf(lset, true)),
		})
	}
	return out, nil
}

// lockedDimensionFilter turns a ListMetrics dimension filter into matchers on
// the collector's labels. ok is false when the filter names a dimension the
// collector does not publish, which nothing can match.
func lockedDimensionFilter(dims []dimension) ([]*labels.Matcher, bool) {
	var out []*labels.Matcher
	for _, d := range dims {
		if d.Name != DimensionInstanceID {
			return nil, false
		}
		if d.Value != "" {
			out = append(out, mustEqual(labelInstanceID, d.Value))
		}
	}
	return out, true
}

func toXMLDimensions(dims []dimension) []xmlDimension {
	out := make([]xmlDimension, 0, len(dims))
	for _, d := range dims {
		out = append(out, xmlDimension(d))
	}
	return out
}

// sortMetrics gives the response a stable order, so a client diffing two calls
// sees only real changes.
func sortMetrics(metrics []xmlMetric) {
	sort.Slice(metrics, func(i, j int) bool {
		a, b := metrics[i], metrics[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		if a.MetricName != b.MetricName {
			return a.MetricName < b.MetricName
		}
		return dimensionKey(a.Dimensions) < dimensionKey(b.Dimensions)
	})
}

func dimensionKey(dims []xmlDimension) string {
	var key string
	for _, d := range dims {
		key += d.Name + "=" + d.Value + ";"
	}
	return key
}

func (s *Server) closeQuerier(q *store.TenantQuerier) {
	if err := q.Close(); err != nil {
		s.log.Warn("closing tenant querier", "error", err)
	}
}
