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
	"DFLOCKD_AUTH_TOKEN", "DFLOCKD_AUTH_TOKEN_FILE",
	"DFLOCKD_HTTP_PORT", "DFLOCKD_HTTP_HOST",
	"DFLOCKD_HTTP_SESSION_IDLE_S", "DFLOCKD_HTTP_MAX_SESSIONS",
	"DFLOCKD_HTTP_MAX_SESSIONS_PER_IP", "DFLOCKD_HTTP_MAX_CONNECTIONS_PER_IP",
	"DFLOCKD_HTTP_RATE_LIMIT_PER_IP", "DFLOCKD_HTTP_RATE_LIMIT_BURST",
	"DFLOCKD_HTTP_CORS_ALLOWED_ORIGINS", "DFLOCKD_DEBUG",
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
