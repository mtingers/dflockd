package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHTTPAdmin_DisabledWhenNoTokenConfigured verifies the default-deny
// posture: with no AdminToken in cfg, the admin endpoints return
// 503 admin_disabled regardless of headers or body.
func TestHTTPAdmin_DisabledWhenNoTokenConfigured(t *testing.T) {
	fc := &httpFakeCluster{leader: true}
	hs := newClusterHTTPTest(t, fc) // cfg.AdminToken empty by default

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/voters",
		strings.NewReader(`{"node_id":"x","raft_addr":"1.2.3.4:7001","client_addr":"1.2.3.4:6388"}`))
	req.Header.Set(adminAuthHeader, "anything")
	hs.handleAdminAddVoter(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 admin_disabled", rec.Code)
	}
	var e errorBody
	_ = json.Unmarshal(rec.Body.Bytes(), &e)
	if e.Error != "admin_disabled" {
		t.Fatalf("error code = %q, want admin_disabled (body=%s)", e.Error, rec.Body)
	}
}

// TestHTTPAdmin_RejectsMissingHeader verifies that with admin token
// configured, a request lacking the X-Dflockd-Admin header returns 401.
func TestHTTPAdmin_RejectsMissingHeader(t *testing.T) {
	fc := &httpFakeCluster{leader: true}
	hs := newClusterHTTPTest(t, fc)
	hs.cfg.AdminToken = "supersecret"

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/voters",
		strings.NewReader(`{"node_id":"x","raft_addr":"1.2.3.4:7001","client_addr":"1.2.3.4:6388"}`))
	hs.handleAdminAddVoter(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

// TestHTTPAdmin_RejectsWrongToken verifies that the admin token is
// compared (in constant time) and a mismatch yields 401.
func TestHTTPAdmin_RejectsWrongToken(t *testing.T) {
	fc := &httpFakeCluster{leader: true}
	hs := newClusterHTTPTest(t, fc)
	hs.cfg.AdminToken = "right"

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/voters",
		strings.NewReader(`{"node_id":"x","raft_addr":"1.2.3.4:7001","client_addr":"1.2.3.4:6388"}`))
	req.Header.Set(adminAuthHeader, "wrong")
	hs.handleAdminAddVoter(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

// TestHTTPAdmin_AddVoterOK exercises the happy path on a leader with the
// correct admin token.
func TestHTTPAdmin_AddVoterOK(t *testing.T) {
	fc := &httpFakeCluster{leader: true}
	hs := newClusterHTTPTest(t, fc)
	hs.cfg.AdminToken = "supersecret"

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/voters",
		strings.NewReader(`{"node_id":"d","raft_addr":"1.2.3.4:7104","client_addr":"1.2.3.4:6388"}`))
	req.Header.Set(adminAuthHeader, "supersecret")
	hs.handleAdminAddVoter(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body)
	}
	var resp adminVoterResp
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != "ok" || resp.NodeID != "d" {
		t.Fatalf("resp = %+v", resp)
	}
}

// TestHTTPAdmin_AddVoterRejectsBadBody verifies request-shape validation.
func TestHTTPAdmin_AddVoterRejectsBadBody(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"empty fields", `{}`},
		{"missing client_addr", `{"node_id":"d","raft_addr":"1.2.3.4:7104"}`},
		{"non-numeric port", `{"node_id":"d","raft_addr":"1.2.3.4:notaport","client_addr":"1.2.3.4:6388"}`},
		{"no colon", `{"node_id":"d","raft_addr":"1.2.3.4","client_addr":"1.2.3.4:6388"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := &httpFakeCluster{leader: true}
			hs := newClusterHTTPTest(t, fc)
			hs.cfg.AdminToken = "secret"

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/admin/voters", strings.NewReader(tc.body))
			req.Header.Set(adminAuthHeader, "secret")
			hs.handleAdminAddVoter(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body)
			}
		})
	}
}

// TestHTTPAdmin_AddVoterFollowerRedirects verifies a follower returns
// 503 not_leader with X-Dflockd-Leader header.
func TestHTTPAdmin_AddVoterFollowerRedirects(t *testing.T) {
	fc := &httpFakeCluster{leader: false, leaderAddr: "10.0.0.7:6388"}
	hs := newClusterHTTPTest(t, fc)
	hs.cfg.AdminToken = "secret"

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/voters",
		strings.NewReader(`{"node_id":"d","raft_addr":"1.2.3.4:7104","client_addr":"1.2.3.4:6388"}`))
	req.Header.Set(adminAuthHeader, "secret")
	hs.handleAdminAddVoter(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 not_leader", rec.Code)
	}
	if got := rec.Header().Get("X-Dflockd-Leader"); got != "10.0.0.7:6388" {
		t.Fatalf("X-Dflockd-Leader = %q", got)
	}
}

// TestHTTPAdmin_RemoveVoterOK exercises the happy path on a leader.
func TestHTTPAdmin_RemoveVoterOK(t *testing.T) {
	fc := &httpFakeCluster{leader: true}
	hs := newClusterHTTPTest(t, fc)
	hs.cfg.AdminToken = "secret"

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/voters/d", nil)
	req.SetPathValue("id", "d")
	req.Header.Set(adminAuthHeader, "secret")
	hs.handleAdminRemoveVoter(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body)
	}
	var resp adminVoterResp
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Status != "ok" || resp.NodeID != "d" {
		t.Fatalf("resp = %+v", resp)
	}
}

// TestHTTPReadIndex_LeaderReturns200 verifies the GET /v1/readindex
// happy path on a leader.
func TestHTTPReadIndex_LeaderReturns200(t *testing.T) {
	fc := &httpFakeCluster{leader: true}
	hs := newClusterHTTPTest(t, fc)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/readindex", nil)
	hs.handleReadIndex(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body)
	}
}

// TestHTTPReadIndex_FollowerReturns503 verifies a follower redirects.
func TestHTTPReadIndex_FollowerReturns503(t *testing.T) {
	fc := &httpFakeCluster{leader: false, leaderAddr: "leader:6388"}
	hs := newClusterHTTPTest(t, fc)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/readindex", nil)
	hs.handleReadIndex(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("X-Dflockd-Leader"); got != "leader:6388" {
		t.Fatalf("X-Dflockd-Leader = %q", got)
	}
}

// TestHTTPReadIndex_SingleNodeReturns404 verifies the readindex endpoint
// 404s when the server isn't in cluster mode. Built using the same
// harness as the cluster tests but without calling SetCluster.
func TestHTTPReadIndex_SingleNodeReturns404(t *testing.T) {
	cfg := defaultTestConfig()
	log := discardLogger()
	hs, _ := buildHTTPServer(context.Background(), testTCPServer(t, cfg, log), cfg, log)
	t.Cleanup(func() { hs.limiter.Stop(); hs.sessions.Shutdown() })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/readindex", nil)
	hs.handleReadIndex(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", rec.Code, rec.Body)
	}
}
