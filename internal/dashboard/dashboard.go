/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// This file (Task 14 + 15) holds the dashboard enablement gate, the HTTP
// handlers, the embedded HTML asset, and the unified operator HTTP server
// (DECISIONS B, C, D, E of the plan).
//
// Port / route map (DECISION C): the operator HTTP server runs on a configurable
// bind address (default :8080, flag --operator-http-bind-address) and is DISTINCT
// from the manager's metrics server (:8443) and health-probe server (:8081):
//
//	/metrics     -> Exporter.Handler(), ALWAYS on (US-3.1 / ADR-04)
//	/dashboard   -> embedded HTML, gated (US-6.1/6.3)
//	/api/events  -> JSON recent events, gated (US-6.2/6.3)
//	/cdc         -> HTTP CDC ingestion, ALWAYS on (resolves the former :8081
//	                HTTPConsumer-default collision with the probe port)
//	/healthz     -> 200 OK ping, ALWAYS on
//	/readyz      -> 200 OK ping, ALWAYS on
//
// The manager keeps its own :8081 /healthz and /readyz probes (manifests point
// liveness/readiness there, unchanged); the :8080 pings make the one
// operator-facing port self-describing. Exposing :8080 in the Deployment/Service
// is deferred to the manifest tasks (18/19).
package dashboard

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// indexHTML is the embedded dashboard page. Chart.js is pulled from a CDN
// <script> tag inside the page, so it is NOT a Go dependency.
//
//go:embed index.html
var indexHTML []byte

// Gate is the operator-wide dashboard enablement switch (DECISION B). The
// reconciler flips it on when any DatabaseSensor requests the dashboard; the
// /dashboard and /api/events handlers consult Enabled on every request and
// return 404 when off. It wraps an atomic.Bool and has no package globals, so it
// is safe for concurrent use.
type Gate struct {
	on atomic.Bool
}

// Enabled reports whether the dashboard is currently served.
func (g *Gate) Enabled() bool { return g.on.Load() }

// Enable turns the dashboard on.
func (g *Gate) Enable() { g.on.Store(true) }

// Disable turns the dashboard off.
func (g *Gate) Disable() { g.on.Store(false) }

// Set turns the dashboard on or off.
func (g *Gate) Set(on bool) { g.on.Store(on) }

// Server is the unified operator HTTP server. It owns an http.ServeMux wiring the
// always-on and gated routes and runs as a controller-runtime manager.Runnable
// so it starts and stops with the manager (DECISION E).
type Server struct {
	addr string
	mux  *http.ServeMux
	gate *Gate
	buf  *RingBuffer
}

// NewServer builds the operator HTTP server. metricsH is the always-on
// Exporter.Handler() mounted at /metrics; cdcH is the always-on CDC ingestion
// handler mounted at /cdc (may be nil to omit the route); buf supplies
// /api/events; gate guards /dashboard and /api/events. The returned Server is a
// manager.Runnable.
func NewServer(addr string, metricsH, cdcH http.Handler, buf *RingBuffer, gate *Gate) *Server {
	s := &Server{
		addr: addr,
		mux:  http.NewServeMux(),
		gate: gate,
		buf:  buf,
	}

	// Always-on: /metrics (US-3.1 / ADR-04).
	if metricsH != nil {
		s.mux.Handle("/metrics", metricsH)
	}
	// Always-on: CDC ingestion (replaces the standalone :8081 HTTP consumer).
	if cdcH != nil {
		s.mux.Handle("/cdc", cdcH)
	}
	// Gated: dashboard page and events API (US-6.1/6.2/6.3).
	s.mux.HandleFunc("/dashboard", s.dashboardHandler)
	s.mux.HandleFunc("/api/events", s.eventsHandler)
	// Always-on: trivial liveness/readiness pings for this port.
	s.mux.HandleFunc("/healthz", pingHandler)
	s.mux.HandleFunc("/readyz", pingHandler)

	return s
}

// Handler exposes the server's mux for in-process testing (httptest) without
// binding a socket.
func (s *Server) Handler() http.Handler { return s.mux }

// dashboardHandler serves the embedded HTML page when the dashboard is enabled,
// and returns 404 otherwise so a disabled dashboard is indistinguishable from an
// unmounted route (US-6.3).
func (s *Server) dashboardHandler(w http.ResponseWriter, r *http.Request) {
	if s.gate == nil || !s.gate.Enabled() {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}

// eventsHandler serves the recent CDC events as a JSON array (newest-first) when
// the dashboard is enabled, and returns 404 otherwise (US-6.2/6.3).
func (s *Server) eventsHandler(w http.ResponseWriter, r *http.Request) {
	if s.gate == nil || !s.gate.Enabled() {
		http.NotFound(w, r)
		return
	}
	var out []Event
	if s.buf != nil {
		out = s.buf.Recent()
	}
	if out == nil {
		out = []Event{}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		logf.FromContext(r.Context()).Error(err, "Failed to encode events response")
	}
}

// pingHandler answers 200 OK. Used for /healthz and /readyz on this port.
func pingHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// Start runs the operator HTTP server, implementing manager.Runnable. It serves
// until ctx is cancelled, then shuts down gracefully with a 5s timeout
// (mirroring the HTTPConsumer pattern). http.ErrServerClosed is treated as a
// clean stop.
func (s *Server) Start(ctx context.Context) error {
	log := logf.FromContext(ctx)

	srv := &http.Server{Addr: s.addr, Handler: s.mux}

	errCh := make(chan error, 1)
	go func() {
		log.Info("Starting operator HTTP server", "addr", s.addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("operator HTTP server: %w", err)
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutting down operator HTTP server: %w", err)
		}
		return nil
	case err := <-errCh:
		return err
	}
}

// compile-time assertion that Server satisfies manager.Runnable.
var _ manager.Runnable = (*Server)(nil)
