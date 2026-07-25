package main

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestClusterClockFromEnv(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Setenv(unsafeTestClockOffsetEnv, "2h")
	now, err := clusterClockFromEnv(log)
	if err != nil {
		t.Fatalf("clusterClockFromEnv: %v", err)
	}
	offset := time.Until(now())
	if offset < 2*time.Hour-time.Second || offset > 2*time.Hour+time.Second {
		t.Fatalf("clock offset = %s, want about 2h", offset)
	}
}

func TestClusterClockFromEnvRejectsInvalidOrExtremeOffset(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, value := range []string{"later", "25h", "-25h"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(unsafeTestClockOffsetEnv, value)
			if _, err := clusterClockFromEnv(log); err == nil {
				t.Fatalf("offset %q accepted", value)
			}
		})
	}
}
