package httpapi

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// metricsRegistry collects per-route request counts and total
// duration for /metrics output.
type metricsRegistry struct {
	mu       sync.Mutex
	requests map[metricKey]*requestMetric
	start    time.Time
}

type metricKey struct {
	method string
	path   string
	status int
}

type requestMetric struct {
	count       uint64
	durationSum float64
}

func newMetricsRegistry() *metricsRegistry {
	return &metricsRegistry{
		requests: make(map[metricKey]*requestMetric),
		start:    time.Now(),
	}
}

func (m *metricsRegistry) observe(method, path string, status int, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := metricKey{method: method, path: path, status: status}
	rm := m.requests[key]
	if rm == nil {
		rm = &requestMetric{}
		m.requests[key] = rm
	}
	rm.count++
	rm.durationSum += d.Seconds()
}

// statusRecorder captures the HTTP status code so withMetrics can label
// per-status counters. Falls back to 200 when no header was written.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(p)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (h *httpServer) withMetrics(mux *http.ServeMux, next http.Handler) http.Handler {
	if h.metrics == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pattern := routePattern(mux, r)
		rec := &statusRecorder{ResponseWriter: w}
		start := time.Now()
		next.ServeHTTP(rec, r)
		status := rec.status
		if status == 0 {
			status = http.StatusOK
		}
		h.metrics.observe(normalizeMethod(r.Method), pattern, status, time.Since(start))
	})
}

// knownMethods are the HTTP verbs we're willing to use as a metric
// label. r.Method is whatever token the client put on the request line
// — on an unmatched route an arbitrary verb flows straight to observe,
// so without this clamp a client spraying distinct method strings could
// grow metricsRegistry.requests without bound.
var knownMethods = map[string]struct{}{
	http.MethodGet: {}, http.MethodHead: {}, http.MethodPost: {},
	http.MethodPut: {}, http.MethodPatch: {}, http.MethodDelete: {},
	http.MethodConnect: {}, http.MethodOptions: {}, http.MethodTrace: {},
}

func normalizeMethod(m string) string {
	if _, ok := knownMethods[m]; ok {
		return m
	}
	return "OTHER"
}

// routePattern returns the path pattern that handled r, or
// "unmatched" if no handler matched.
func routePattern(mux *http.ServeMux, r *http.Request) string {
	_, pattern := mux.Handler(r)
	if pattern == "" {
		return "unmatched"
	}
	if _, path, ok := strings.Cut(pattern, " "); ok {
		return path
	}
	return pattern
}

// handleMetrics is the Prometheus exposition endpoint.
func (h *httpServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	stats := h.currentStats()
	sessions := h.sessions.Count()
	ready := 1
	if h.draining.Load() {
		ready = 0
	}

	var b strings.Builder
	h.metrics.writePrometheus(&b)
	writeGauge(&b, "dflockd_ready", "1 when HTTP readiness is healthy, 0 while draining.", float64(ready))
	writeGauge(&b, "dflockd_uptime_seconds", "Seconds since the HTTP API process started.", time.Since(h.metrics.start).Seconds())
	writeGauge(&b, "dflockd_connections", "Active TCP plus HTTP virtual protocol connections.", float64(stats.Connections))
	writeGauge(&b, "dflockd_http_sessions", "Active HTTP bridge sessions.", float64(sessions))

	var heldLocks, lockWaiters, semHolders, semWaiters int
	for _, li := range stats.Locks {
		if li.OwnerConnID != 0 || li.LeaseExpiresInS > 0 {
			heldLocks++
		}
		lockWaiters += li.Waiters
	}
	for _, si := range stats.Semaphores {
		semHolders += si.Holders
		semWaiters += si.Waiters
	}
	writeGauge(&b, "dflockd_locks_held", "Currently held locks.", float64(heldLocks))
	writeGauge(&b, "dflockd_lock_waiters", "Current lock waiters.", float64(lockWaiters))
	writeGauge(&b, "dflockd_semaphore_holders", "Currently held semaphore slots.", float64(semHolders))
	writeGauge(&b, "dflockd_semaphore_waiters", "Current semaphore waiters.", float64(semWaiters))
	_, _ = w.Write([]byte(b.String()))
}

// writePrometheus dumps per-route counters under m.mu held briefly,
// then renders the snapshot to b without holding the lock.
func (m *metricsRegistry) writePrometheus(b *strings.Builder) {
	m.mu.Lock()
	keys := make([]metricKey, 0, len(m.requests))
	for k := range m.requests {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].path != keys[j].path {
			return keys[i].path < keys[j].path
		}
		if keys[i].method != keys[j].method {
			return keys[i].method < keys[j].method
		}
		return keys[i].status < keys[j].status
	})
	snap := make(map[metricKey]requestMetric, len(keys))
	for _, k := range keys {
		snap[k] = *m.requests[k]
	}
	m.mu.Unlock()

	b.WriteString("# HELP dflockd_http_requests_total Total HTTP requests.\n")
	b.WriteString("# TYPE dflockd_http_requests_total counter\n")
	for _, k := range keys {
		rm := snap[k]
		fmt.Fprintf(b, "dflockd_http_requests_total{method=%q,path=%q,status=%q} %d\n",
			k.method, k.path, strconv.Itoa(k.status), rm.count)
	}
	b.WriteString("# HELP dflockd_http_request_duration_seconds_sum Total HTTP request duration in seconds.\n")
	b.WriteString("# TYPE dflockd_http_request_duration_seconds_sum counter\n")
	for _, k := range keys {
		rm := snap[k]
		fmt.Fprintf(b, "dflockd_http_request_duration_seconds_sum{method=%q,path=%q,status=%q} %.9g\n",
			k.method, k.path, strconv.Itoa(k.status), rm.durationSum)
	}
	b.WriteString("# HELP dflockd_http_request_duration_seconds_count Count of HTTP request duration observations.\n")
	b.WriteString("# TYPE dflockd_http_request_duration_seconds_count counter\n")
	for _, k := range keys {
		rm := snap[k]
		fmt.Fprintf(b, "dflockd_http_request_duration_seconds_count{method=%q,path=%q,status=%q} %d\n",
			k.method, k.path, strconv.Itoa(k.status), rm.count)
	}
}

func writeGauge(b *strings.Builder, name, help string, value float64) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s gauge\n", name)
	fmt.Fprintf(b, "%s %.9g\n", name, value)
}
