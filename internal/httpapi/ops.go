package httpapi

import (
	"fmt"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func remoteIPFromAddr(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		return host
	}
	if host := strings.TrimSpace(addr); host != "" {
		return host
	}
	return "unknown"
}

// ---------------------------------------------------------------------------
// CORS
// ---------------------------------------------------------------------------

func (h *httpServer) withCORS(next http.Handler) http.Handler {
	allowed := h.cfg.HTTPCORSAllowedOrigins
	if len(allowed) == 0 {
		return next
	}
	allowAll := false
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, origin := range allowed {
		if origin == "*" {
			allowAll = true
			continue
		}
		allowedSet[origin] = struct{}{}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			if allowAll {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else if _, ok := allowedSet[origin]; ok {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Vary", "Origin")
			}
			if w.Header().Get("Access-Control-Allow-Origin") != "" {
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Dflockd-Session")
				w.Header().Set("Access-Control-Max-Age", "300")
			}
		}
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------------------
// Rate and connection limiting
// ---------------------------------------------------------------------------

type httpRateLimiter struct {
	rate  float64
	burst float64

	mu      sync.Mutex
	buckets map[string]*rateBucket
}

type rateBucket struct {
	tokens float64
	last   time.Time
}

func newHTTPRateLimiter(rate, burst int) *httpRateLimiter {
	if rate <= 0 {
		return nil
	}
	if burst <= 0 {
		burst = rate
	}
	return &httpRateLimiter{
		rate:    float64(rate),
		burst:   float64(burst),
		buckets: make(map[string]*rateBucket),
	}
}

func (l *httpRateLimiter) allow(ip string, now time.Time) bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.buckets[ip]
	if b == nil {
		l.buckets[ip] = &rateBucket{tokens: l.burst - 1, last: now}
		return true
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens = math.Min(l.burst, b.tokens+elapsed*l.rate)
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func (h *httpServer) withRateLimit(next http.Handler) http.Handler {
	if h.limiter == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Keep liveness/readiness probes reliable under load.
		if r.URL.Path == "/health" || r.URL.Path == "/ready" {
			next.ServeHTTP(w, r)
			return
		}
		ip := remoteIPFromAddr(r.RemoteAddr)
		if !h.limiter.allow(ip, time.Now()) {
			writeError(w, http.StatusTooManyRequests, "rate_limited", "")
			return
		}
		next.ServeHTTP(w, r)
	})
}

type httpConnLimiter struct {
	max int
	mu  sync.Mutex
	ips map[net.Conn]string
	n   map[string]int
}

func newHTTPConnLimiter(max int) *httpConnLimiter {
	return &httpConnLimiter{
		max: max,
		ips: make(map[net.Conn]string),
		n:   make(map[string]int),
	}
}

func (l *httpConnLimiter) ConnState(conn net.Conn, state http.ConnState) {
	if l == nil || l.max <= 0 {
		return
	}
	switch state {
	case http.StateNew:
		ip := remoteIPFromAddr(conn.RemoteAddr().String())
		l.mu.Lock()
		if l.n[ip] >= l.max {
			l.mu.Unlock()
			_ = conn.Close()
			return
		}
		l.n[ip]++
		l.ips[conn] = ip
		l.mu.Unlock()
	case http.StateClosed, http.StateHijacked:
		l.mu.Lock()
		if ip, ok := l.ips[conn]; ok {
			delete(l.ips, conn)
			l.n[ip]--
			if l.n[ip] <= 0 {
				delete(l.n, ip)
			}
		}
		l.mu.Unlock()
	}
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

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

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func (r *statusRecorder) Flush() {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func routePattern(mux *http.ServeMux, r *http.Request) string {
	_, pattern := mux.Handler(r)
	if pattern != "" {
		if _, path, ok := strings.Cut(pattern, " "); ok {
			return path
		}
		return pattern
	}
	return "unmatched"
}

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
		h.metrics.observe(r.Method, pattern, status, time.Since(start))
	})
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

func (h *httpServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	stats := h.currentStats()
	sessions := h.bridge.SessionCount()
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

	var heldLocks, lockWaiters, semHolders, semWaiters, signalListeners int
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
	for _, ci := range stats.SignalChannels {
		signalListeners += ci.Listeners
	}
	writeGauge(&b, "dflockd_locks_held", "Currently held locks.", float64(heldLocks))
	writeGauge(&b, "dflockd_lock_waiters", "Current lock waiters.", float64(lockWaiters))
	writeGauge(&b, "dflockd_semaphore_holders", "Currently held semaphore slots.", float64(semHolders))
	writeGauge(&b, "dflockd_semaphore_waiters", "Current semaphore waiters.", float64(semWaiters))
	writeGauge(&b, "dflockd_signal_listeners", "Current signal listener registrations.", float64(signalListeners))
	_, _ = w.Write([]byte(b.String()))
}

func (m *metricsRegistry) writePrometheus(b *strings.Builder) {
	m.mu.Lock()
	keys := make([]metricKey, 0, len(m.requests))
	for key := range m.requests {
		keys = append(keys, key)
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
	for _, key := range keys {
		snap[key] = *m.requests[key]
	}
	m.mu.Unlock()

	b.WriteString("# HELP dflockd_http_requests_total Total HTTP requests.\n")
	b.WriteString("# TYPE dflockd_http_requests_total counter\n")
	for _, key := range keys {
		rm := snap[key]
		fmt.Fprintf(b, "dflockd_http_requests_total{method=%q,path=%q,status=%q} %d\n",
			key.method, key.path, strconv.Itoa(key.status), rm.count)
	}
	b.WriteString("# HELP dflockd_http_request_duration_seconds_sum Total HTTP request duration in seconds.\n")
	b.WriteString("# TYPE dflockd_http_request_duration_seconds_sum counter\n")
	for _, key := range keys {
		rm := snap[key]
		fmt.Fprintf(b, "dflockd_http_request_duration_seconds_sum{method=%q,path=%q,status=%q} %.9g\n",
			key.method, key.path, strconv.Itoa(key.status), rm.durationSum)
	}
	b.WriteString("# HELP dflockd_http_request_duration_seconds_count Count of HTTP request duration observations.\n")
	b.WriteString("# TYPE dflockd_http_request_duration_seconds_count counter\n")
	for _, key := range keys {
		rm := snap[key]
		fmt.Fprintf(b, "dflockd_http_request_duration_seconds_count{method=%q,path=%q,status=%q} %d\n",
			key.method, key.path, strconv.Itoa(key.status), rm.count)
	}
}

func writeGauge(b *strings.Builder, name, help string, value float64) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s gauge\n", name)
	fmt.Fprintf(b, "%s %.9g\n", name, value)
}
