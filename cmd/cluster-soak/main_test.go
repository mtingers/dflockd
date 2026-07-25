package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

// TestParseSoakFlagsDefaults verifies the CLI parses an empty arg
// list to the documented defaults.
func TestParseSoakFlagsDefaults(t *testing.T) {
	o, err := parseSoakFlags(nil)
	if err != nil {
		t.Fatalf("parseSoakFlags: %v", err)
	}
	if o.Nodes != 3 || o.Workers != 4 {
		t.Fatalf("defaults wrong: nodes=%d workers=%d", o.Nodes, o.Workers)
	}
	if o.Keys != 1 || o.HistoryLimit != maxHistoryLimit {
		t.Fatalf("history defaults wrong: keys=%d limit=%d", o.Keys, o.HistoryLimit)
	}
	if o.Duration <= 0 || o.KillInterval <= 0 {
		t.Fatalf("defaults wrong: duration=%s kill=%s", o.Duration, o.KillInterval)
	}
}

// TestParseSoakFlagsOverrides verifies each flag is honored.
func TestParseSoakFlagsOverrides(t *testing.T) {
	o, err := parseSoakFlags([]string{
		"-nodes=5", "-workers=2", "-duration=1s", "-kill-interval=500ms", "-seed=42",
	})
	if err != nil {
		t.Fatalf("parseSoakFlags: %v", err)
	}
	if o.Nodes != 5 || o.Workers != 2 || o.Seed != 42 {
		t.Fatalf("overrides wrong: %+v", o)
	}
	if o.Duration != time.Second || o.KillInterval != 500*time.Millisecond {
		t.Fatalf("durations wrong: %+v", o)
	}
}

// TestRunSoakCleanRunHasNoViolations: a short soak with kills
// disabled must complete with no invariant violations. This is the
// minimum "does it work end-to-end" check.
func TestRunSoakCleanRunHasNoViolations(t *testing.T) {
	if testing.Short() {
		t.Skip("soak: long-ish (1s) — skipped in short mode")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	report, err := runSoak(ctx, soakOpts{
		Nodes: 3, Workers: 2, Duration: 500 * time.Millisecond, KillInterval: 0, Seed: 1,
	}, &bytes.Buffer{}, log)
	if err != nil {
		t.Fatalf("runSoak: %v", err)
	}
	if len(report.Violations) > 0 {
		t.Fatalf("violations: %v", report.Violations)
	}
	if report.Successes == 0 {
		t.Fatalf("no successful writes — workload not running")
	}
}

// TestRunSoakWithKillsCompletes: a short soak with kills enabled must
// also complete cleanly. The fence-monotonic + no-token-reuse
// invariants are what we're checking — leadership-change should not
// violate either.
func TestRunSoakWithKillsCompletes(t *testing.T) {
	if testing.Short() {
		t.Skip("soak: long-ish (2s) — skipped in short mode")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	report, err := runSoak(ctx, soakOpts{
		Nodes: 3, Workers: 2, Duration: 1500 * time.Millisecond, KillInterval: 500 * time.Millisecond, Seed: 2,
	}, &bytes.Buffer{}, log)
	if err != nil {
		t.Fatalf("runSoak: %v", err)
	}
	if len(report.Violations) > 0 {
		t.Fatalf("violations under kills: %v", report.Violations)
	}
	if report.Killed == 0 {
		t.Fatalf("no leader kills occurred — kill loop broken")
	}
}
