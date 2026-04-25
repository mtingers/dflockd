package config

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 6388 {
		t.Fatalf("expected port 6388, got %d", cfg.Port)
	}
	if cfg.MaxLocks != 1024 {
		t.Fatalf("expected max-locks 1024, got %d", cfg.MaxLocks)
	}
}

func TestLoad_BadFlags_ReturnsError(t *testing.T) {
	// Custom FlagSet should return an error, not os.Exit.
	_, err := Load([]string{"--nonexistent-flag"})
	if err == nil {
		t.Fatal("expected error for unknown flag")
	}
}

func TestLoad_ValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string // substring expected in error
	}{
		{"max-locks=0", []string{"--max-locks", "0"}, "max-locks"},
		{"default-lease-ttl=0", []string{"--default-lease-ttl", "0"}, "default-lease-ttl"},
		{"lease-sweep-interval=0", []string{"--lease-sweep-interval", "0"}, "lease-sweep-interval"},
		{"gc-interval=0", []string{"--gc-interval", "0"}, "gc-interval"},
		{"gc-max-idle=-1", []string{"--gc-max-idle", "-1"}, "gc-max-idle"},
		{"read-timeout=0", []string{"--read-timeout", "0"}, "read-timeout"},
		{"port negative", []string{"--port", "-1"}, "port"},
		{"port too high", []string{"--port", "99999"}, "port"},
		{"max-connections negative", []string{"--max-connections", "-1"}, "max-connections"},
		{"max-waiters negative", []string{"--max-waiters", "-1"}, "max-waiters"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(tc.args)
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q should contain %q", err.Error(), tc.want)
			}
		})
	}
}

// TestLoad_ValidatesPingIntervalAgainstIdleTimeout confirms that an
// HTTP SSE ping interval >= the session idle timeout is rejected at
// config load. The constraint exists so SSE streams refresh lastSeen
// before the bridge sweeper's 2x-idleTimeout cutoff fires.
func TestLoad_ValidatesPingIntervalAgainstIdleTimeout(t *testing.T) {
	// ping >= idle is rejected.
	_, err := Load([]string{
		"--http-port", "9999",
		"--http-sse-ping-interval", "30",
		"--http-session-idle-timeout", "20",
	})
	if err == nil {
		t.Fatal("expected validation error for ping >= idle")
	}
	if !strings.Contains(err.Error(), "http-sse-ping-interval") {
		t.Fatalf("error should mention http-sse-ping-interval, got %q", err.Error())
	}

	// ping < idle passes.
	_, err = Load([]string{
		"--http-port", "9999",
		"--http-sse-ping-interval", "10",
		"--http-session-idle-timeout", "20",
	})
	if err != nil {
		t.Fatalf("ping<idle should validate: %v", err)
	}

	// ping == idle is rejected (no margin against the sweeper).
	_, err = Load([]string{
		"--http-port", "9999",
		"--http-sse-ping-interval", "20",
		"--http-session-idle-timeout", "20",
	})
	if err == nil {
		t.Fatal("expected error for ping == idle")
	}
}

func TestLoad_ValidEdgeCases(t *testing.T) {
	// write-timeout=0 is valid (disables write timeout)
	_, err := Load([]string{"--write-timeout", "0"})
	if err != nil {
		t.Fatalf("write-timeout=0 should be valid: %v", err)
	}

	// shutdown-timeout=0 is valid (wait forever)
	_, err = Load([]string{"--shutdown-timeout", "0"})
	if err != nil {
		t.Fatalf("shutdown-timeout=0 should be valid: %v", err)
	}

	// max-connections=0 is valid (unlimited)
	_, err = Load([]string{"--max-connections", "0"})
	if err != nil {
		t.Fatalf("max-connections=0 should be valid: %v", err)
	}

	// max-waiters=0 is valid (unlimited)
	_, err = Load([]string{"--max-waiters", "0"})
	if err != nil {
		t.Fatalf("max-waiters=0 should be valid: %v", err)
	}

	// gc-max-idle=0 is valid (prune immediately)
	_, err = Load([]string{"--gc-max-idle", "0"})
	if err != nil {
		t.Fatalf("gc-max-idle=0 should be valid: %v", err)
	}
}

// TestAuthTokenPrecedence documents the precedence order for auth token
// resolution, matching the project-wide "CLI flag > env var > file" rule.
//
// The bug fixed here: previously DFLOCKD_AUTH_TOKEN silently overrode
// --auth-token, contradicting the documented precedence.
func TestAuthTokenPrecedence(t *testing.T) {
	t.Run("flag wins over env var", func(t *testing.T) {
		t.Setenv("DFLOCKD_AUTH_TOKEN", "from-env")
		cfg, err := Load([]string{"--auth-token", "from-flag"})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.AuthToken != "from-flag" {
			t.Fatalf("got %q, want from-flag (flag should win)", cfg.AuthToken)
		}
	})

	t.Run("env var used when flag absent", func(t *testing.T) {
		t.Setenv("DFLOCKD_AUTH_TOKEN", "from-env")
		cfg, err := Load([]string{})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.AuthToken != "from-env" {
			t.Fatalf("got %q, want from-env", cfg.AuthToken)
		}
	})

	t.Run("empty flag does not override env", func(t *testing.T) {
		// Edge case: `--auth-token=""` explicitly empty should NOT
		// silently fall through to env. Our implementation treats empty
		// flag as unset (so env wins), matching flag.Visit semantics.
		t.Setenv("DFLOCKD_AUTH_TOKEN", "from-env")
		cfg, err := Load([]string{"--auth-token", ""})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.AuthToken != "from-env" {
			t.Fatalf("got %q, want from-env (empty flag defers to env)", cfg.AuthToken)
		}
	})

	t.Run("flag token file wins over env var", func(t *testing.T) {
		t.Setenv("DFLOCKD_AUTH_TOKEN", "from-env")
		path := writeTempTokenFile(t, "from-file\n")
		cfg, err := Load([]string{"--auth-token-file", path})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.AuthToken != "from-file" {
			t.Fatalf("got %q, want from-file (explicit token file should win)", cfg.AuthToken)
		}
	})

	t.Run("direct token is trimmed like token files", func(t *testing.T) {
		cfg, err := Load([]string{"--auth-token", "  from-flag  "})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.AuthToken != "from-flag" {
			t.Fatalf("got %q, want from-flag", cfg.AuthToken)
		}
	})

	t.Run("whitespace only direct token is rejected", func(t *testing.T) {
		_, err := Load([]string{"--auth-token", "   "})
		if err == nil {
			t.Fatal("expected whitespace-only auth token to fail")
		}
		if !strings.Contains(err.Error(), "auth-token") {
			t.Fatalf("error should mention auth-token, got %q", err.Error())
		}
	})
}

func writeTempTokenFile(t *testing.T, token string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "token-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(token); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

// TestGCEnvVarAliases exercises the canonical + deprecated env var
// fallback. Both DFLOCKD_GC_INTERVAL_S and the legacy
// DFLOCKD_GC_LOOP_SLEEP should be accepted; the canonical takes
// priority when both are set. Same for GC_MAX_IDLE_S vs
// GC_MAX_UNUSED_TIME.
func TestGCEnvVarAliases(t *testing.T) {
	t.Run("canonical gc interval", func(t *testing.T) {
		t.Setenv("DFLOCKD_GC_INTERVAL_S", "7")
		cfg, err := Load([]string{})
		if err != nil {
			t.Fatal(err)
		}
		if got := cfg.GCInterval.Seconds(); got != 7 {
			t.Fatalf("GCInterval: got %v want 7s", got)
		}
	})
	t.Run("deprecated gc loop sleep still works", func(t *testing.T) {
		t.Setenv("DFLOCKD_GC_LOOP_SLEEP", "9")
		cfg, err := Load([]string{})
		if err != nil {
			t.Fatal(err)
		}
		if got := cfg.GCInterval.Seconds(); got != 9 {
			t.Fatalf("GCInterval: got %v want 9s", got)
		}
	})
	t.Run("canonical beats deprecated", func(t *testing.T) {
		t.Setenv("DFLOCKD_GC_INTERVAL_S", "5")
		t.Setenv("DFLOCKD_GC_LOOP_SLEEP", "99")
		cfg, err := Load([]string{})
		if err != nil {
			t.Fatal(err)
		}
		if got := cfg.GCInterval.Seconds(); got != 5 {
			t.Fatalf("GCInterval: got %v want 5s (canonical should win)", got)
		}
	})
	t.Run("canonical max idle", func(t *testing.T) {
		t.Setenv("DFLOCKD_GC_MAX_IDLE_S", "30")
		cfg, err := Load([]string{})
		if err != nil {
			t.Fatal(err)
		}
		if got := cfg.GCMaxIdleTime.Seconds(); got != 30 {
			t.Fatalf("GCMaxIdleTime: got %v want 30s", got)
		}
	})
	t.Run("deprecated max unused time", func(t *testing.T) {
		t.Setenv("DFLOCKD_GC_MAX_UNUSED_TIME", "45")
		cfg, err := Load([]string{})
		if err != nil {
			t.Fatal(err)
		}
		if got := cfg.GCMaxIdleTime.Seconds(); got != 45 {
			t.Fatalf("GCMaxIdleTime: got %v want 45s", got)
		}
	})
}

func TestSecondsCeil(t *testing.T) {
	// Not directly exported, but we test via config validation of
	// duration values. This exercises that the config correctly converts
	// integer seconds to time.Duration.
	cfg, err := Load([]string{"--default-lease-ttl", "7"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DefaultLeaseTTL.Seconds() != 7 {
		t.Fatalf("expected 7s, got %v", cfg.DefaultLeaseTTL)
	}
}

func TestLoadRejectsDurationOverflowBeforeMultiplication(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("duration overflow boundary does not fit in int on this platform")
	}
	tooLarge := strconv.FormatInt(maxDurationSeconds+1, 10)

	t.Run("flag", func(t *testing.T) {
		_, err := Load([]string{"--default-lease-ttl", tooLarge})
		if err == nil || !strings.Contains(err.Error(), "too large") {
			t.Fatalf("expected too-large error, got %v", err)
		}
	})

	t.Run("env", func(t *testing.T) {
		t.Setenv("DFLOCKD_READ_TIMEOUT_S", tooLarge)
		_, err := Load([]string{})
		if err == nil || !strings.Contains(err.Error(), "too large") {
			t.Fatalf("expected too-large error, got %v", err)
		}
	})

	t.Run("deprecated alias", func(t *testing.T) {
		t.Setenv("DFLOCKD_GC_LOOP_SLEEP", tooLarge)
		_, err := Load([]string{})
		if err == nil || !strings.Contains(err.Error(), "too large") {
			t.Fatalf("expected too-large error, got %v", err)
		}
	})
}
