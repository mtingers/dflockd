package config

import (
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
