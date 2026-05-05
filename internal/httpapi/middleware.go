package httpapi

import (
	"crypto/subtle"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Auth middleware
// ---------------------------------------------------------------------------

// withAuth requires every protected endpoint to carry
// Authorization: Bearer <token> when AuthToken is configured.
// /health and /ready are intentionally exempt: they're load-balancer
// probes and shouldn't need credentials.
func (h *httpServer) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.cfg.AuthToken == "" {
			next.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/health" || r.URL.Path == "/ready" {
			next.ServeHTTP(w, r)
			return
		}
		got := extractBearerToken(r.Header.Get("Authorization"))
		if got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(h.cfg.AuthToken)) != 1 {
			writeError(w, http.StatusUnauthorized, "unauthorized", "")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// extractBearerToken parses an "Authorization: Bearer <token>" header.
// RFC 7235 makes the auth-scheme token case-insensitive; the credential
// itself stays case-sensitive (compared with subtle elsewhere).
func extractBearerToken(header string) string {
	const prefix = "Bearer "
	if len(header) < len(prefix) {
		return ""
	}
	if !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}

// ---------------------------------------------------------------------------
// CORS
// ---------------------------------------------------------------------------

// withCORS adds permissive CORS headers when the configured origin
// list is non-empty. "*" allows everything; specific origins are
// echoed back with Vary: Origin.
func (h *httpServer) withCORS(next http.Handler) http.Handler {
	if len(h.cfg.HTTPCORSAllowedOrigins) == 0 {
		return next
	}
	policy := newCORSPolicy(h.cfg.HTTPCORSAllowedOrigins)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveCORS(w, r, policy, next)
	})
}

// corsPolicy is the parsed origin allow-list.
type corsPolicy struct {
	allowAll bool
	allowed  map[string]struct{}
}

func newCORSPolicy(origins []string) *corsPolicy {
	p := &corsPolicy{allowed: make(map[string]struct{}, len(origins))}
	for _, o := range origins {
		if o == "*" {
			p.allowAll = true
			continue
		}
		p.allowed[o] = struct{}{}
	}
	return p
}

// serveCORS writes the CORS response headers (when applicable),
// short-circuits preflights, and otherwise delegates to next.
func serveCORS(w http.ResponseWriter, r *http.Request, p *corsPolicy, next http.Handler) {
	if origin := r.Header.Get("Origin"); origin != "" {
		writeCORSHeaders(w, origin, p)
	}
	if isCORSPreflight(r) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	next.ServeHTTP(w, r)
}

// writeCORSHeaders emits the per-request CORS headers. The full set
// is only written when an Allow-Origin was set.
func writeCORSHeaders(w http.ResponseWriter, origin string, p *corsPolicy) {
	setCORSAllowOrigin(w, origin, p)
	if w.Header().Get("Access-Control-Allow-Origin") == "" {
		return
	}
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Dflockd-Session")
	w.Header().Set("Access-Control-Max-Age", "300")
}

func setCORSAllowOrigin(w http.ResponseWriter, origin string, p *corsPolicy) {
	if p.allowAll {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		return
	}
	if _, ok := p.allowed[origin]; !ok {
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Vary", "Origin")
}

func isCORSPreflight(r *http.Request) bool {
	return r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != ""
}

// ---------------------------------------------------------------------------
// Per-IP rate limiting (token bucket)
// ---------------------------------------------------------------------------

// rateBucketIdleEviction caps how long an idle bucket lives in the
// map. After this much idle time the bucket is at full burst, so
// dropping it is equivalent to keeping it.
const rateBucketIdleEviction = 10 * time.Minute

// rateBucketSweepInterval is how often the sweeper scans for idle
// buckets to evict.
const rateBucketSweepInterval = 5 * time.Minute

type httpRateLimiter struct {
	rate  float64
	burst float64

	mu      sync.Mutex
	buckets map[string]*rateBucket

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

type rateBucket struct {
	tokens float64
	last   time.Time
}

// newHTTPRateLimiter returns nil when rate limiting is disabled, so
// callers can pass through to next without a wrapper.
func newHTTPRateLimiter(rate, burst int) *httpRateLimiter {
	if rate <= 0 {
		return nil
	}
	if burst <= 0 {
		burst = rate
	}
	l := &httpRateLimiter{
		rate:    float64(rate),
		burst:   float64(burst),
		buckets: make(map[string]*rateBucket),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go l.sweepLoop()
	return l
}

// allow consumes one token if available. nil-safe: a nil receiver
// always allows (rate limiting disabled).
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

func (l *httpRateLimiter) sweep(now time.Time) {
	cutoff := now.Add(-rateBucketIdleEviction)
	l.mu.Lock()
	defer l.mu.Unlock()
	for ip, b := range l.buckets {
		if b.last.Before(cutoff) {
			delete(l.buckets, ip)
		}
	}
}

func (l *httpRateLimiter) sweepLoop() {
	defer close(l.done)
	t := time.NewTicker(rateBucketSweepInterval)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case now := <-t.C:
			l.sweep(now)
		}
	}
}

// Stop terminates the sweeper goroutine. nil-safe and idempotent.
func (l *httpRateLimiter) Stop() {
	if l == nil {
		return
	}
	l.stopOnce.Do(func() { close(l.stop) })
	<-l.done
}

func (h *httpServer) withRateLimit(next http.Handler) http.Handler {
	if h.limiter == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Probes stay reachable under load.
		if r.URL.Path == "/health" || r.URL.Path == "/ready" {
			next.ServeHTTP(w, r)
			return
		}
		if !h.limiter.allow(remoteIPFromAddr(r.RemoteAddr), time.Now()) {
			writeError(w, http.StatusTooManyRequests, "rate_limited", "")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------------------
// Per-IP HTTP connection limit
// ---------------------------------------------------------------------------

// httpConnLimiter caps the number of HTTP transport-level connections
// per remote IP. Sessions are independent of this limit (they're a
// higher-level concept).
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

// ConnState is the http.Server.ConnState callback. Tracks new conns
// per-IP and enforces the limit at handshake time.
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
