package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// withEnv sets env vars for the duration of the test, restoring them
// (including unset state) afterwards.
func withEnv(t *testing.T, env map[string]string) {
	t.Helper()
	for k, v := range env {
		setEnvForTest(t, k, v)
	}
}

func setEnvForTest(t *testing.T, key, value string) {
	original, hadOriginal := os.LookupEnv(key)
	os.Setenv(key, value)
	t.Cleanup(func() { restoreEnv(key, original, hadOriginal) })
}

func restoreEnv(key, original string, hadOriginal bool) {
	if hadOriginal {
		os.Setenv(key, original)
		return
	}
	os.Unsetenv(key)
}

// clearEnv ensures every env var the loader looks at is unset.
func clearEnv(t *testing.T) {
	for _, k := range configEnvKeys {
		clearEnvKey(t, k)
	}
}

var configEnvKeys = []string{
	"DFLOCKD_HOST", "DFLOCKD_PORT", "DFLOCKD_DEFAULT_LEASE_TTL_S",
	"DFLOCKD_LEASE_SWEEP_INTERVAL_S", "DFLOCKD_GC_INTERVAL_S",
	"DFLOCKD_GC_MAX_IDLE_S", "DFLOCKD_MAX_LOCKS",
	"DFLOCKD_MAX_CONNECTIONS", "DFLOCKD_MAX_CONNECTIONS_PER_IP",
	"DFLOCKD_MAX_WAITERS", "DFLOCKD_READ_TIMEOUT_S",
	"DFLOCKD_WRITE_TIMEOUT_S", "DFLOCKD_SHUTDOWN_TIMEOUT_S",
	"DFLOCKD_AUTO_RELEASE_ON_DISCONNECT", "DFLOCKD_TLS_CERT", "DFLOCKD_TLS_KEY",
	"DFLOCKD_AUTH_TOKEN", "DFLOCKD_AUTH_TOKEN_FILE", "DFLOCKD_FENCE_STATE_FILE",
	"DFLOCKD_HTTP_PORT", "DFLOCKD_HTTP_HOST",
	"DFLOCKD_HTTP_SESSION_IDLE_S", "DFLOCKD_HTTP_MAX_SESSIONS",
	"DFLOCKD_HTTP_MAX_SESSIONS_PER_IP", "DFLOCKD_HTTP_MAX_CONNECTIONS_PER_IP",
	"DFLOCKD_HTTP_RATE_LIMIT_PER_IP", "DFLOCKD_HTTP_RATE_LIMIT_BURST",
	"DFLOCKD_HTTP_CORS_ALLOWED_ORIGINS", "DFLOCKD_DEBUG",
	"DFLOCKD_ORPHAN_TTL_S",
}

func clearEnvKey(t *testing.T, key string) {
	original, had := os.LookupEnv(key)
	os.Unsetenv(key)
	if had {
		t.Cleanup(func() { os.Setenv(key, original) })
	}
}

func TestLoad_Defaults(t *testing.T) {
	clearEnv(t)
	cfg, err := Load(nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Host != "127.0.0.1" {
		t.Errorf("Host = %q", cfg.Host)
	}
	if cfg.Port != 6388 {
		t.Errorf("Port = %d", cfg.Port)
	}
	if cfg.DefaultLeaseTTL != 33*time.Second {
		t.Errorf("DefaultLeaseTTL = %v", cfg.DefaultLeaseTTL)
	}
	if cfg.MaxLocks != 1024 {
		t.Errorf("MaxLocks = %d", cfg.MaxLocks)
	}
	if !cfg.AutoReleaseOnDisconnect {
		t.Error("AutoReleaseOnDisconnect should default true")
	}
}

func TestLoad_FlagWinsOverEnv(t *testing.T) {
	clearEnv(t)
	withEnv(t, map[string]string{"DFLOCKD_PORT": "9999"})
	cfg, err := Load([]string{"--port", "1234"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Port != 1234 {
		t.Errorf("Port = %d, want 1234 (flag wins)", cfg.Port)
	}
}

func TestLoad_EnvWhenFlagOmitted(t *testing.T) {
	clearEnv(t)
	withEnv(t, map[string]string{"DFLOCKD_PORT": "9999"})
	cfg, err := Load(nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Port != 9999 {
		t.Errorf("Port = %d, want 9999 (from env)", cfg.Port)
	}
}

func TestLoad_FenceStateFile(t *testing.T) {
	clearEnv(t)
	withEnv(t, map[string]string{"DFLOCKD_FENCE_STATE_FILE": "/tmp/from-env"})
	cfg, err := Load([]string{"--fence-state-file", "/tmp/from-flag"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.FenceStateFile != "/tmp/from-flag" {
		t.Errorf("FenceStateFile = %q, want flag value", cfg.FenceStateFile)
	}
}

func TestLoad_BadInt(t *testing.T) {
	clearEnv(t)
	withEnv(t, map[string]string{"DFLOCKD_PORT": "abc"})
	_, err := Load(nil)
	if err == nil {
		t.Fatal("expected error for non-integer DFLOCKD_PORT")
	}
}

func TestLoad_BoolValues(t *testing.T) {
	cases := []struct {
		val  string
		want bool
		ok   bool
	}{
		{"true", true, true},
		{"1", true, true},
		{"yes", true, true},
		{"false", false, true},
		{"0", false, true},
		{"no", false, true},
		{"maybe", false, false},
	}
	for _, c := range cases {
		clearEnv(t)
		withEnv(t, map[string]string{"DFLOCKD_AUTO_RELEASE_ON_DISCONNECT": c.val})
		cfg, err := Load(nil)
		if c.ok && err != nil {
			t.Errorf("%q: unexpected error %v", c.val, err)
			continue
		}
		if !c.ok && err == nil {
			t.Errorf("%q: expected error", c.val)
			continue
		}
		if c.ok && cfg.AutoReleaseOnDisconnect != c.want {
			t.Errorf("%q: got %v, want %v", c.val, cfg.AutoReleaseOnDisconnect, c.want)
		}
	}
}

func TestValidate_PortRange(t *testing.T) {
	cfg := &Config{
		MaxLocks:           1,
		DefaultLeaseTTL:    time.Second,
		LeaseSweepInterval: time.Second,
		GCInterval:         time.Second,
		ReadTimeout:        time.Second,
		Port:               65536,
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for port 65536")
	}
}

func TestValidate_TLSPaired(t *testing.T) {
	cfg := &Config{
		MaxLocks:           1,
		DefaultLeaseTTL:    time.Second,
		LeaseSweepInterval: time.Second,
		GCInterval:         time.Second,
		ReadTimeout:        time.Second,
		Port:               80,
		TLSCert:            "cert.pem",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error: cert without key")
	}
}

func TestValidate_HTTPPortConflict(t *testing.T) {
	cfg := &Config{
		MaxLocks:           1,
		DefaultLeaseTTL:    time.Second,
		LeaseSweepInterval: time.Second,
		GCInterval:         time.Second,
		ReadTimeout:        time.Second,
		Port:               8080,
		HTTPPort:           8080,
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error: HTTPPort == Port")
	}
}

func TestLoad_AuthTokenFile(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "tok")
	if err := os.WriteFile(path, []byte("hunter2\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load([]string{"--auth-token-file", path})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AuthToken != "hunter2" {
		t.Errorf("AuthToken = %q", cfg.AuthToken)
	}
}

func TestLoad_AuthTokenPrecedence(t *testing.T) {
	clearEnv(t)
	withEnv(t, map[string]string{"DFLOCKD_AUTH_TOKEN": "fromenv"})
	cfg, err := Load([]string{"--auth-token", "fromflag"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AuthToken != "fromflag" {
		t.Errorf("AuthToken = %q, want fromflag", cfg.AuthToken)
	}
}

func TestLoad_AuthTokenWithNewline(t *testing.T) {
	clearEnv(t)
	_, err := Load([]string{"--auth-token", "bad\ntoken"})
	if err == nil || !strings.Contains(err.Error(), "newline") {
		t.Fatalf("got %v, want newline error", err)
	}
}

func TestLoad_HTTPRateBurstDefaults(t *testing.T) {
	clearEnv(t)
	cfg, err := Load([]string{"--http-rate-limit-per-ip", "10"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HTTPRateLimitBurst != 10 {
		t.Errorf("HTTPRateLimitBurst = %d, want 10 (defaulted to rate)", cfg.HTTPRateLimitBurst)
	}
}

func TestLoad_CORS(t *testing.T) {
	clearEnv(t)
	cfg, err := Load([]string{"--http-cors-allowed-origins", "a, b ,c"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.HTTPCORSAllowedOrigins) != 3 {
		t.Fatalf("got %v", cfg.HTTPCORSAllowedOrigins)
	}
	for _, s := range cfg.HTTPCORSAllowedOrigins {
		if strings.ContainsAny(s, " \t") {
			t.Errorf("not trimmed: %q", s)
		}
	}
}

func TestUnboundedLimitWarnings(t *testing.T) {
	cases := []struct {
		name    string
		cfg     *Config
		wantAny bool
		want    []string
		wantNot []string // substrings that must NOT appear
	}{
		{
			name:    "loopback host stays quiet even with everything unbounded",
			cfg:     &Config{Host: "127.0.0.1"},
			wantAny: false,
		},
		{
			name:    "loopback host with HTTP on loopback stays quiet",
			cfg:     &Config{Host: "localhost", HTTPPort: 6389, HTTPHost: "::1"},
			wantAny: false,
		},
		{
			name:    "non-loopback host warns about TCP limits",
			cfg:     &Config{Host: "0.0.0.0"},
			wantAny: true,
			want:    []string{"--max-connections", "--max-connections-per-ip", "--max-waiters"},
			wantNot: []string{"http"},
		},
		{
			name:    "non-loopback host with HTTP warns about HTTP limits too",
			cfg:     &Config{Host: "10.0.0.1", HTTPPort: 6389},
			wantAny: true,
			want: []string{
				"--max-connections",
				"--max-connections-per-ip",
				"--max-waiters",
				"--http-max-sessions",
				"--http-max-sessions-per-ip",
				"--http-max-connections-per-ip",
				"--http-rate-limit-per-ip",
			},
		},
		{
			name: "fully-bounded non-loopback config stays quiet",
			cfg: &Config{
				Host: "0.0.0.0", MaxConnections: 100, MaxConnectionsPerIP: 10, MaxWaiters: 50,
				HTTPPort: 6389, HTTPMaxSessions: 100, HTTPMaxSessionsPerIP: 10,
				HTTPMaxConnectionsPerIP: 10, HTTPRateLimitPerIP: 100,
			},
			wantAny: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := c.cfg.UnboundedLimitWarnings()
			if c.wantAny && len(got) == 0 {
				t.Fatalf("got no warnings, want at least one")
			}
			if !c.wantAny && len(got) != 0 {
				t.Fatalf("got warnings %v, want none", got)
			}
			joined := strings.ToLower(strings.Join(got, " "))
			for _, want := range c.want {
				if !strings.Contains(joined, want) {
					t.Errorf("warnings %v missing %q", got, want)
				}
			}
			for _, sub := range c.wantNot {
				if strings.Contains(joined, sub) {
					t.Errorf("warnings %v unexpectedly mention %q", got, sub)
				}
			}
		})
	}
}

// orphanClusterArgs returns a minimal valid cluster flag set with any
// extra args appended — OrphanTTL > 0 is only valid in cluster mode.
func orphanClusterArgs(extra ...string) []string {
	return append([]string{
		"--raft-dir", "/tmp/raft", "--node-id", "n1",
		"--raft-addr", "127.0.0.1:7001",
		"--cluster-peers", "n1=127.0.0.1:7001@127.0.0.1:6388",
	}, extra...)
}

func TestLoad_OrphanTTL_Default(t *testing.T) {
	clearEnv(t)
	cfg, err := Load(nil)
	if err != nil {
		t.Fatalf("Load defaults: %v", err)
	}
	if cfg.OrphanTTL != 0 {
		t.Fatalf("default OrphanTTL = %v, want 0 (disabled)", cfg.OrphanTTL)
	}
}

func TestLoad_OrphanTTL_Flag(t *testing.T) {
	clearEnv(t)
	cfg, err := Load(orphanClusterArgs("--orphan-ttl", "45"))
	if err != nil {
		t.Fatalf("Load --orphan-ttl: %v", err)
	}
	if cfg.OrphanTTL != 45*time.Second {
		t.Fatalf("OrphanTTL = %v, want 45s", cfg.OrphanTTL)
	}
}

func TestLoad_OrphanTTL_EnvAndPrecedence(t *testing.T) {
	clearEnv(t)
	withEnv(t, map[string]string{"DFLOCKD_ORPHAN_TTL_S": "30"})
	cfg, err := Load(orphanClusterArgs())
	if err != nil {
		t.Fatalf("Load env: %v", err)
	}
	if cfg.OrphanTTL != 30*time.Second {
		t.Fatalf("OrphanTTL from env = %v, want 30s", cfg.OrphanTTL)
	}
	cfg, err = Load(orphanClusterArgs("--orphan-ttl", "10"))
	if err != nil {
		t.Fatalf("Load flag+env: %v", err)
	}
	if cfg.OrphanTTL != 10*time.Second {
		t.Fatalf("OrphanTTL = %v, want 10s (flag wins over env)", cfg.OrphanTTL)
	}
}

func TestLoad_OrphanTTL_RequiresCluster(t *testing.T) {
	clearEnv(t)
	_, err := Load([]string{"--orphan-ttl", "30"})
	if err == nil || !strings.Contains(err.Error(), "--orphan-ttl requires cluster mode") {
		t.Fatalf("want orphan-ttl-requires-cluster error, got %v", err)
	}
}

func TestLoad_OrphanTTL_Negative(t *testing.T) {
	clearEnv(t)
	withEnv(t, map[string]string{"DFLOCKD_ORPHAN_TTL_S": "-5"})
	_, err := Load(nil)
	if err == nil || !strings.Contains(err.Error(), "--orphan-ttl must be >= 0") {
		t.Fatalf("want orphan-ttl>=0 error, got %v", err)
	}
}
