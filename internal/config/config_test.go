package config

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
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
		{"gc-max-idle=0", []string{"--gc-max-idle", "0"}, "gc-max-idle"},
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

func TestLoad_AllFlagsParsed(t *testing.T) {
	args := []string{
		"--host", "0.0.0.0",
		"--port", "9999",
		"--default-lease-ttl", "60",
		"--lease-sweep-interval", "5",
		"--gc-interval", "10",
		"--gc-max-idle", "120",
		"--max-locks", "2048",
		"--max-keys", "500",
		"--max-list-length", "100",
		"--max-connections", "50",
		"--max-waiters", "25",
		"--read-timeout", "30",
		"--write-timeout", "10",
		"--shutdown-timeout", "60",
		"--auto-release-on-disconnect=false",
		"--debug",
	}
	cfg, err := Load(args)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Host != "0.0.0.0" {
		t.Errorf("host: got %q, want %q", cfg.Host, "0.0.0.0")
	}
	if cfg.Port != 9999 {
		t.Errorf("port: got %d, want 9999", cfg.Port)
	}
	if cfg.DefaultLeaseTTL.Seconds() != 60 {
		t.Errorf("default-lease-ttl: got %v, want 60s", cfg.DefaultLeaseTTL)
	}
	if cfg.LeaseSweepInterval.Seconds() != 5 {
		t.Errorf("lease-sweep-interval: got %v, want 5s", cfg.LeaseSweepInterval)
	}
	if cfg.GCInterval.Seconds() != 10 {
		t.Errorf("gc-interval: got %v, want 10s", cfg.GCInterval)
	}
	if cfg.GCMaxIdleTime.Seconds() != 120 {
		t.Errorf("gc-max-idle: got %v, want 120s", cfg.GCMaxIdleTime)
	}
	if cfg.MaxLocks != 2048 {
		t.Errorf("max-locks: got %d, want 2048", cfg.MaxLocks)
	}
	if cfg.MaxKeys != 500 {
		t.Errorf("max-keys: got %d, want 500", cfg.MaxKeys)
	}
	if cfg.MaxListLength != 100 {
		t.Errorf("max-list-length: got %d, want 100", cfg.MaxListLength)
	}
	if cfg.MaxConnections != 50 {
		t.Errorf("max-connections: got %d, want 50", cfg.MaxConnections)
	}
	if cfg.MaxWaiters != 25 {
		t.Errorf("max-waiters: got %d, want 25", cfg.MaxWaiters)
	}
	if cfg.ReadTimeout.Seconds() != 30 {
		t.Errorf("read-timeout: got %v, want 30s", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout.Seconds() != 10 {
		t.Errorf("write-timeout: got %v, want 10s", cfg.WriteTimeout)
	}
	if cfg.ShutdownTimeout.Seconds() != 60 {
		t.Errorf("shutdown-timeout: got %v, want 60s", cfg.ShutdownTimeout)
	}
	if cfg.AutoReleaseOnDisconnect {
		t.Error("auto-release-on-disconnect: got true, want false")
	}
	if !cfg.Debug {
		t.Error("debug: got false, want true")
	}
}

func TestLoad_EnvVarOverridesDefault(t *testing.T) {
	t.Setenv("DFLOCKD_PORT", "7777")
	t.Setenv("DFLOCKD_HOST", "0.0.0.0")
	t.Setenv("DFLOCKD_MAX_LOCKS", "4096")
	t.Setenv("DFLOCKD_MAX_KEYS", "1000")
	t.Setenv("DFLOCKD_DEBUG", "true")

	cfg, err := Load([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 7777 {
		t.Errorf("port: got %d, want 7777", cfg.Port)
	}
	if cfg.Host != "0.0.0.0" {
		t.Errorf("host: got %q, want %q", cfg.Host, "0.0.0.0")
	}
	if cfg.MaxLocks != 4096 {
		t.Errorf("max-locks: got %d, want 4096", cfg.MaxLocks)
	}
	if cfg.MaxKeys != 1000 {
		t.Errorf("max-keys: got %d, want 1000", cfg.MaxKeys)
	}
	if !cfg.Debug {
		t.Error("debug: got false, want true")
	}
}

func TestLoad_FlagOverridesEnvVar(t *testing.T) {
	t.Setenv("DFLOCKD_PORT", "7777")
	t.Setenv("DFLOCKD_MAX_LOCKS", "4096")

	cfg, err := Load([]string{"--port", "8888", "--max-locks", "512"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 8888 {
		t.Errorf("port: got %d, want 8888 (flag should override env)", cfg.Port)
	}
	if cfg.MaxLocks != 512 {
		t.Errorf("max-locks: got %d, want 512 (flag should override env)", cfg.MaxLocks)
	}
}

func TestLoad_EnvVarBoolFormats(t *testing.T) {
	tests := []struct {
		envVal string
		want   bool
	}{
		{"1", true},
		{"yes", true},
		{"true", true},
		{"TRUE", true},
		{"Yes", true},
		{"0", false},
		{"no", false},
		{"false", false},
		{"FALSE", false},
		{"No", false},
	}

	for _, tc := range tests {
		t.Run(tc.envVal, func(t *testing.T) {
			t.Setenv("DFLOCKD_DEBUG", tc.envVal)
			cfg, err := Load([]string{})
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Debug != tc.want {
				t.Errorf("DFLOCKD_DEBUG=%q: got %v, want %v", tc.envVal, cfg.Debug, tc.want)
			}
		})
	}
}

func TestLoad_EnvVarInvalidIntFallsBack(t *testing.T) {
	t.Setenv("DFLOCKD_PORT", "not_a_number")
	cfg, err := Load([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 6388 {
		t.Errorf("port: got %d, want 6388 (default)", cfg.Port)
	}
}

func TestLoad_EnvVarInvalidBoolFallsBack(t *testing.T) {
	t.Setenv("DFLOCKD_DEBUG", "maybe")
	cfg, err := Load([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Debug {
		t.Error("debug: got true, want false (default)")
	}
}

func TestLoad_AdditionalValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"max-keys negative", []string{"--max-keys", "-1"}, "max-keys"},
		{"max-list-length negative", []string{"--max-list-length", "-1"}, "max-list-length"},
		{"write-timeout negative", []string{"--write-timeout", "-1"}, "write-timeout"},
		{"shutdown-timeout negative", []string{"--shutdown-timeout", "-1"}, "shutdown-timeout"},
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

func TestLoad_PortBoundary(t *testing.T) {
	cfg, err := Load([]string{"--port", "0"})
	if err != nil {
		t.Fatalf("port 0 should be valid: %v", err)
	}
	if cfg.Port != 0 {
		t.Fatalf("port: got %d, want 0", cfg.Port)
	}

	cfg, err = Load([]string{"--port", "65535"})
	if err != nil {
		t.Fatalf("port 65535 should be valid: %v", err)
	}
	if cfg.Port != 65535 {
		t.Fatalf("port: got %d, want 65535", cfg.Port)
	}
}

func TestLoad_AuthTokenFlag(t *testing.T) {
	cfg, err := Load([]string{"--auth-token", "mysecret"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AuthToken != "mysecret" {
		t.Fatalf("auth-token: got %q, want %q", cfg.AuthToken, "mysecret")
	}
}

func TestLoad_AuthTokenEnvVar(t *testing.T) {
	t.Setenv("DFLOCKD_AUTH_TOKEN", "envsecret")
	cfg, err := Load([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AuthToken != "envsecret" {
		t.Fatalf("auth-token: got %q, want %q", cfg.AuthToken, "envsecret")
	}
}

func TestLoad_AuthTokenEnvOverridesFlag(t *testing.T) {
	t.Setenv("DFLOCKD_AUTH_TOKEN", "envwins")
	cfg, err := Load([]string{"--auth-token", "flagvalue"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AuthToken != "envwins" {
		t.Fatalf("auth-token: got %q, want %q (env should take priority)", cfg.AuthToken, "envwins")
	}
}

func TestLoad_AuthTokenFile(t *testing.T) {
	tmpFile := t.TempDir() + "/token.txt"
	if err := os.WriteFile(tmpFile, []byte("file-token\n  "), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load([]string{"--auth-token-file", tmpFile})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AuthToken != "file-token" {
		t.Fatalf("auth-token: got %q, want %q (whitespace should be trimmed)", cfg.AuthToken, "file-token")
	}
}

func TestLoad_AuthTokenFileMissing(t *testing.T) {
	_, err := Load([]string{"--auth-token-file", "/nonexistent/path/token.txt"})
	if err == nil {
		t.Fatal("expected error for missing auth token file")
	}
	if !strings.Contains(err.Error(), "auth token file") {
		t.Fatalf("error should mention auth token file: %v", err)
	}
}

func TestLoad_AuthTokenFileEnvVar(t *testing.T) {
	tmpFile := t.TempDir() + "/token.txt"
	if err := os.WriteFile(tmpFile, []byte("env-file-token"), 0600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("DFLOCKD_AUTH_TOKEN_FILE", tmpFile)
	cfg, err := Load([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AuthToken != "env-file-token" {
		t.Fatalf("auth-token: got %q, want %q", cfg.AuthToken, "env-file-token")
	}
}

func TestLoad_VersionFlag(t *testing.T) {
	cfg, err := Load([]string{"--version"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Version {
		t.Fatal("version flag should be true")
	}
}

func TestLoad_EnvDurationParsing(t *testing.T) {
	t.Setenv("DFLOCKD_DEFAULT_LEASE_TTL_S", "120")
	t.Setenv("DFLOCKD_READ_TIMEOUT_S", "45")

	cfg, err := Load([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DefaultLeaseTTL.Seconds() != 120 {
		t.Errorf("default-lease-ttl: got %v, want 120s", cfg.DefaultLeaseTTL)
	}
	if cfg.ReadTimeout.Seconds() != 45 {
		t.Errorf("read-timeout: got %v, want 45s", cfg.ReadTimeout)
	}
}

func TestLoad_AutoReleaseDefault(t *testing.T) {
	cfg, err := Load([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.AutoReleaseOnDisconnect {
		t.Error("auto-release-on-disconnect should default to true")
	}
}

func TestLoad_AutoReleaseEnvVar(t *testing.T) {
	t.Setenv("DFLOCKD_AUTO_RELEASE_ON_DISCONNECT", "false")
	cfg, err := Load([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AutoReleaseOnDisconnect {
		t.Error("auto-release-on-disconnect: got true, want false")
	}
}

// ---------------------------------------------------------------------------
// Regression: envOrDuration must not overflow for large second values
// ---------------------------------------------------------------------------

func TestEnvOrDuration_LargeValueClamped(t *testing.T) {
	// Use a value that fits in int but would overflow time.Duration (int64
	// nanoseconds) when multiplied by time.Second (1e9). maxSafeSeconds is
	// about 9.2e9 on 64-bit, so 9999999999 (just over) will be clamped.
	largeVal := fmt.Sprintf("%d", maxSafeSeconds+1)
	t.Setenv("DFLOCKD_TEST_LARGE_DUR", largeVal)
	d := envOrDuration("DFLOCKD_TEST_LARGE_DUR", 5)
	if d <= 0 {
		t.Fatalf("expected positive clamped duration, got %v", d)
	}
	// The result should be maxSafeSeconds * time.Second, not an overflow.
	expected := time.Duration(maxSafeSeconds) * time.Second
	if d != expected {
		t.Errorf("expected clamped duration %v, got %v", expected, d)
	}
}

func TestEnvOrDuration_NormalValue(t *testing.T) {
	t.Setenv("DFLOCKD_TEST_NORMAL", "42")
	d := envOrDuration("DFLOCKD_TEST_NORMAL", 5)
	if d != 42*time.Second {
		t.Errorf("expected 42s, got %v", d)
	}
}

func TestEnvOrDuration_FallbackDefault(t *testing.T) {
	// No env var set, should use the flag default.
	d := envOrDuration("DFLOCKD_NONEXISTENT_KEY_XYZZY", 10)
	if d != 10*time.Second {
		t.Errorf("expected 10s, got %v", d)
	}
}

func TestLoad_MaxSubscriptionsFlag(t *testing.T) {
	cfg, err := Load([]string{"--max-subscriptions", "100"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxSubscriptions != 100 {
		t.Errorf("expected MaxSubscriptions=100, got %d", cfg.MaxSubscriptions)
	}
}

func TestLoad_MaxSubscriptionsEnvVar(t *testing.T) {
	t.Setenv("DFLOCKD_MAX_SUBSCRIPTIONS", "50")
	cfg, err := Load([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxSubscriptions != 50 {
		t.Errorf("expected MaxSubscriptions=50, got %d", cfg.MaxSubscriptions)
	}
}

func TestLoad_MaxSubscriptionsNegativeInvalid(t *testing.T) {
	_, err := Load([]string{"--max-subscriptions", "-1"})
	if err == nil {
		t.Fatal("expected error for negative max-subscriptions")
	}
}

// ---------------------------------------------------------------------------
// Edge-case tests
// ---------------------------------------------------------------------------

func TestLoad_TLSCertNoKey(t *testing.T) {
	// Config doesn't cross-validate TLS cert/key; it just stores them
	// independently. Verify each can be set without the other.
	t.Run("cert_only", func(t *testing.T) {
		cfg, err := Load([]string{"--tls-cert", "/path/to/cert.pem"})
		if err != nil {
			t.Fatalf("tls-cert without tls-key should be accepted: %v", err)
		}
		if cfg.TLSCert != "/path/to/cert.pem" {
			t.Fatalf("tls-cert: got %q, want %q", cfg.TLSCert, "/path/to/cert.pem")
		}
		if cfg.TLSKey != "" {
			t.Fatalf("tls-key should be empty, got %q", cfg.TLSKey)
		}
	})

	t.Run("key_only", func(t *testing.T) {
		cfg, err := Load([]string{"--tls-key", "/path/to/key.pem"})
		if err != nil {
			t.Fatalf("tls-key without tls-cert should be accepted: %v", err)
		}
		if cfg.TLSKey != "/path/to/key.pem" {
			t.Fatalf("tls-key: got %q, want %q", cfg.TLSKey, "/path/to/key.pem")
		}
		if cfg.TLSCert != "" {
			t.Fatalf("tls-cert should be empty, got %q", cfg.TLSCert)
		}
	})

	t.Run("both_set", func(t *testing.T) {
		cfg, err := Load([]string{
			"--tls-cert", "/path/to/cert.pem",
			"--tls-key", "/path/to/key.pem",
		})
		if err != nil {
			t.Fatalf("tls-cert + tls-key should be accepted: %v", err)
		}
		if cfg.TLSCert != "/path/to/cert.pem" {
			t.Fatalf("tls-cert: got %q", cfg.TLSCert)
		}
		if cfg.TLSKey != "/path/to/key.pem" {
			t.Fatalf("tls-key: got %q", cfg.TLSKey)
		}
	})
}

func TestLoad_AllMaxValues(t *testing.T) {
	// Set all max-* flags to large (but safe) values and verify they parse.
	bigInt := "999999999"
	cfg, err := Load([]string{
		"--max-locks", bigInt,
		"--max-keys", bigInt,
		"--max-list-length", bigInt,
		"--max-connections", bigInt,
		"--max-waiters", bigInt,
		"--max-subscriptions", bigInt,
	})
	if err != nil {
		t.Fatalf("all max values should parse: %v", err)
	}
	if cfg.MaxLocks != 999999999 {
		t.Errorf("max-locks: got %d, want 999999999", cfg.MaxLocks)
	}
	if cfg.MaxKeys != 999999999 {
		t.Errorf("max-keys: got %d, want 999999999", cfg.MaxKeys)
	}
	if cfg.MaxListLength != 999999999 {
		t.Errorf("max-list-length: got %d, want 999999999", cfg.MaxListLength)
	}
	if cfg.MaxConnections != 999999999 {
		t.Errorf("max-connections: got %d, want 999999999", cfg.MaxConnections)
	}
	if cfg.MaxWaiters != 999999999 {
		t.Errorf("max-waiters: got %d, want 999999999", cfg.MaxWaiters)
	}
	if cfg.MaxSubscriptions != 999999999 {
		t.Errorf("max-subscriptions: got %d, want 999999999", cfg.MaxSubscriptions)
	}
}

func TestLoad_ZeroValues(t *testing.T) {
	// All zero-able values should be accepted (0 = unlimited for these).
	cfg, err := Load([]string{
		"--max-keys", "0",
		"--max-list-length", "0",
		"--max-connections", "0",
		"--max-waiters", "0",
		"--max-subscriptions", "0",
		"--write-timeout", "0",
		"--shutdown-timeout", "0",
	})
	if err != nil {
		t.Fatalf("zero values should be accepted: %v", err)
	}
	if cfg.MaxKeys != 0 {
		t.Errorf("max-keys: got %d, want 0", cfg.MaxKeys)
	}
	if cfg.MaxListLength != 0 {
		t.Errorf("max-list-length: got %d, want 0", cfg.MaxListLength)
	}
	if cfg.MaxConnections != 0 {
		t.Errorf("max-connections: got %d, want 0", cfg.MaxConnections)
	}
	if cfg.MaxWaiters != 0 {
		t.Errorf("max-waiters: got %d, want 0", cfg.MaxWaiters)
	}
	if cfg.MaxSubscriptions != 0 {
		t.Errorf("max-subscriptions: got %d, want 0", cfg.MaxSubscriptions)
	}
	if cfg.WriteTimeout != 0 {
		t.Errorf("write-timeout: got %v, want 0", cfg.WriteTimeout)
	}
	if cfg.ShutdownTimeout != 0 {
		t.Errorf("shutdown-timeout: got %v, want 0", cfg.ShutdownTimeout)
	}
}

func TestLoad_AuthTokenFile_EmptyFile(t *testing.T) {
	tmpFile := t.TempDir() + "/empty_token.txt"
	if err := os.WriteFile(tmpFile, []byte(""), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load([]string{"--auth-token-file", tmpFile})
	if err != nil {
		t.Fatalf("empty auth token file should be accepted: %v", err)
	}
	if cfg.AuthToken != "" {
		t.Fatalf("auth-token: got %q, want empty string", cfg.AuthToken)
	}
}

func TestLoad_AuthTokenFile_WithNewlines(t *testing.T) {
	tmpFile := t.TempDir() + "/token_newlines.txt"
	if err := os.WriteFile(tmpFile, []byte("my-secret-token\n\n\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load([]string{"--auth-token-file", tmpFile})
	if err != nil {
		t.Fatalf("auth token file with newlines should be accepted: %v", err)
	}
	if cfg.AuthToken != "my-secret-token" {
		t.Fatalf("auth-token: got %q, want %q (trailing newlines should be trimmed)",
			cfg.AuthToken, "my-secret-token")
	}
}

func TestLoad_EnvVarPrecedence_AllTypes(t *testing.T) {
	// Test that env vars override defaults for int, bool, string, duration types.

	t.Run("int", func(t *testing.T) {
		t.Setenv("DFLOCKD_MAX_LOCKS", "8192")
		cfg, err := Load([]string{})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MaxLocks != 8192 {
			t.Errorf("max-locks: got %d, want 8192 (env override)", cfg.MaxLocks)
		}
	})

	t.Run("bool", func(t *testing.T) {
		t.Setenv("DFLOCKD_AUTO_RELEASE_ON_DISCONNECT", "false")
		cfg, err := Load([]string{})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.AutoReleaseOnDisconnect {
			t.Error("auto-release: got true, want false (env override)")
		}
	})

	t.Run("string", func(t *testing.T) {
		t.Setenv("DFLOCKD_HOST", "192.168.1.100")
		cfg, err := Load([]string{})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Host != "192.168.1.100" {
			t.Errorf("host: got %q, want %q (env override)", cfg.Host, "192.168.1.100")
		}
	})

	t.Run("duration", func(t *testing.T) {
		t.Setenv("DFLOCKD_DEFAULT_LEASE_TTL_S", "999")
		cfg, err := Load([]string{})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.DefaultLeaseTTL != 999*time.Second {
			t.Errorf("default-lease-ttl: got %v, want %v (env override)",
				cfg.DefaultLeaseTTL, 999*time.Second)
		}
	})
}

func TestLoad_FlagOverridesEnvVar_AllTypes(t *testing.T) {
	// Explicit flags should override env vars for all types.

	t.Run("int", func(t *testing.T) {
		t.Setenv("DFLOCKD_MAX_LOCKS", "8192")
		cfg, err := Load([]string{"--max-locks", "256"})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MaxLocks != 256 {
			t.Errorf("max-locks: got %d, want 256 (flag should override env)", cfg.MaxLocks)
		}
	})

	t.Run("bool", func(t *testing.T) {
		t.Setenv("DFLOCKD_DEBUG", "true")
		cfg, err := Load([]string{"--debug=false"})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Debug {
			t.Error("debug: got true, want false (flag should override env)")
		}
	})

	t.Run("string", func(t *testing.T) {
		t.Setenv("DFLOCKD_HOST", "192.168.1.100")
		cfg, err := Load([]string{"--host", "10.0.0.1"})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Host != "10.0.0.1" {
			t.Errorf("host: got %q, want %q (flag should override env)", cfg.Host, "10.0.0.1")
		}
	})

	t.Run("duration", func(t *testing.T) {
		t.Setenv("DFLOCKD_DEFAULT_LEASE_TTL_S", "999")
		cfg, err := Load([]string{"--default-lease-ttl", "15"})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.DefaultLeaseTTL != 15*time.Second {
			t.Errorf("default-lease-ttl: got %v, want %v (flag should override env)",
				cfg.DefaultLeaseTTL, 15*time.Second)
		}
	})
}

func TestLoad_DebugFlag(t *testing.T) {
	// --debug should set Debug=true
	cfg, err := Load([]string{"--debug"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Debug {
		t.Error("debug: got false, want true")
	}

	// Without --debug, it should default to false
	cfg, err = Load([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Debug {
		t.Error("debug: got true, want false (default)")
	}
}

func TestLoad_ShutdownTimeoutZero(t *testing.T) {
	cfg, err := Load([]string{"--shutdown-timeout", "0"})
	if err != nil {
		t.Fatalf("shutdown-timeout=0 should be valid (means wait forever): %v", err)
	}
	if cfg.ShutdownTimeout != 0 {
		t.Fatalf("shutdown-timeout: got %v, want 0", cfg.ShutdownTimeout)
	}
}

func TestLoad_WriteTimeoutZero(t *testing.T) {
	cfg, err := Load([]string{"--write-timeout", "0"})
	if err != nil {
		t.Fatalf("write-timeout=0 should be valid (disables write timeout): %v", err)
	}
	if cfg.WriteTimeout != 0 {
		t.Fatalf("write-timeout: got %v, want 0", cfg.WriteTimeout)
	}
}
