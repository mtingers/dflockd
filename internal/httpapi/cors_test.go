package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// buildCORSTestServer constructs an httpServer with the given allowed
// origins. Uses the same harness as the other tests but exercises the
// full middleware chain (the mux Handler the *http.Server holds).
func buildCORSTestServer(t *testing.T, origins []string) http.Handler {
	t.Helper()
	cfg := defaultTestConfig()
	cfg.HTTPCORSAllowedOrigins = origins
	hs, _ := buildHTTPServer(context.Background(), testTCPServer(t, cfg, discardLogger()), cfg, discardLogger())
	t.Cleanup(func() { hs.limiter.Stop(); hs.sessions.Shutdown() })
	return hs.srv.Handler
}

// TestCORS_DisabledByDefault verifies that with no allowed origins
// configured, the CORS middleware is a no-op pass-through.
func TestCORS_DisabledByDefault(t *testing.T) {
	h := buildCORSTestServer(t, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("unexpected CORS header: %q", got)
	}
}

// TestCORS_AllowsListedOrigin verifies an allow-listed origin gets the
// Allow-Origin header (with Vary: Origin so caches stay correct).
func TestCORS_AllowsListedOrigin(t *testing.T) {
	h := buildCORSTestServer(t, []string{"https://app.example.com", "https://staging.example.com"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Origin", "https://app.example.com")
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Errorf("Allow-Origin = %q, want echo", got)
	}
	if got := rec.Header().Get("Vary"); got != "Origin" {
		t.Errorf("Vary = %q, want Origin", got)
	}
}

// TestCORS_RejectsUnlistedOrigin verifies an origin not on the allow
// list gets *no* Allow-Origin header. Browsers will refuse the response.
func TestCORS_RejectsUnlistedOrigin(t *testing.T) {
	h := buildCORSTestServer(t, []string{"https://app.example.com"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("unexpected Allow-Origin for unlisted origin: %q", got)
	}
}

// TestCORS_WildcardAllowsEverything verifies "*" sends Allow-Origin: *
// (wildcards don't pair with credentials).
func TestCORS_WildcardAllowsEverything(t *testing.T) {
	h := buildCORSTestServer(t, []string{"*"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Origin", "https://anywhere.example.com")
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Allow-Origin = %q, want *", got)
	}
}

// TestCORS_Preflight204 verifies OPTIONS preflight requests are
// short-circuited at 204 with the right Allow-Methods response.
func TestCORS_Preflight204(t *testing.T) {
	h := buildCORSTestServer(t, []string{"https://app.example.com"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/v1/sessions", nil)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("preflight status = %d, want 204", rec.Code)
	}
	wantMethods := "GET, POST, DELETE, OPTIONS"
	if got := rec.Header().Get("Access-Control-Allow-Methods"); got != wantMethods {
		t.Errorf("Allow-Methods = %q, want %q", got, wantMethods)
	}
}
