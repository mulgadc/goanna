// Package health serves the liveness, readiness and status endpoints.
package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"time"
)

// Check reports whether one dependency is usable. A nil return means ready.
type Check func() error

// Detail reports free-form diagnostics for the status endpoint. It is
// separate from Check because status is diagnostic and must never gate
// readiness.
type Detail func() any

// Options configures a Server.
type Options struct {
	Version string
	// Checks gate /readyz. Keep these to things that are genuinely broken
	// when they fail, not to things that are merely quiet.
	Checks map[string]Check
	// Details are reported by /statusz under their key.
	Details map[string]Detail
	Logger  *slog.Logger
}

// Server exposes the endpoints over an http.Handler.
type Server struct {
	opts    Options
	started time.Time
	log     *slog.Logger
}

// New builds a Server. The zero Options is valid and yields a Server whose
// readiness depends on nothing.
func New(opts Options) *Server {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	if opts.Version == "" {
		opts.Version = "dev"
	}
	return &Server{opts: opts, started: time.Now(), log: log}
}

// Handler returns the mux serving /healthz, /readyz and /statusz.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /statusz", s.statusz)
	return mux
}

// healthz answers liveness: the process is running and serving. It
// deliberately checks nothing else, so a dependency outage restarts nothing.
func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, _ *http.Request) {
	results := make(map[string]string, len(s.opts.Checks))
	var failed bool
	for name, check := range s.opts.Checks {
		if err := check(); err != nil {
			results[name] = err.Error()
			failed = true
			continue
		}
		results[name] = "ok"
	}

	status := http.StatusOK
	body := map[string]any{"status": "ready", "checks": results}
	if failed {
		status = http.StatusServiceUnavailable
		body["status"] = "not ready"
	}
	s.writeJSON(w, status, body)
}

func (s *Server) statusz(w http.ResponseWriter, _ *http.Request) {
	body := map[string]any{
		"version":    s.opts.Version,
		"started":    s.started.UTC(),
		"uptime_ms":  time.Since(s.started).Milliseconds(),
		"go_version": runtime.Version(),
	}
	for name, detail := range s.opts.Details {
		body[name] = detail()
	}
	s.writeJSON(w, http.StatusOK, body)
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		s.log.Warn("writing health response", "error", err)
	}
}

// Serve runs the endpoints on addr until ctx is done, then shuts down.
func (s *Server) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- fmt.Errorf("health: listen on %s: %w", addr, err)
			return
		}
		errc <- nil
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			return fmt.Errorf("health: shutdown: %w", err)
		}
		return nil
	}
}
