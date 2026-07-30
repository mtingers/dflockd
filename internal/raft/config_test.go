package raft

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultConfigValidatesWithID(t *testing.T) {
	c := DefaultConfig()
	c.ID = "n1"
	if err := c.Validate(); err != nil {
		t.Fatalf("DefaultConfig with ID should validate: %v", err)
	}
}

func TestConfigValidate(t *testing.T) {
	base := func() Config { c := DefaultConfig(); c.ID = "n1"; return c }
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantSub string // substring expected in the error; "" means no error
	}{
		{"ok", func(*Config) {}, ""},
		{"missing id", func(c *Config) { c.ID = "" }, "ID is required"},
		{"oversized id", func(c *Config) { c.ID = NodeID(strings.Repeat("x", maxRPCNodeIDBytes+1)) }, "ID length"},
		{"zero heartbeat", func(c *Config) { c.HeartbeatInterval = 0 }, "HeartbeatInterval must be > 0"},
		{"zero election min", func(c *Config) { c.ElectionTimeoutMin = 0 }, "ElectionTimeoutMin"},
		{"max below min", func(c *Config) { c.ElectionTimeoutMax = c.ElectionTimeoutMin - time.Millisecond }, "ElectionTimeoutMin"},
		{"heartbeat too big", func(c *Config) { c.HeartbeatInterval = c.ElectionTimeoutMin }, "too large vs ElectionTimeoutMin"},
		{"zero max append", func(c *Config) { c.MaxAppendEntries = 0 }, "MaxAppendEntries must be > 0"},
		{"zero max snapshot", func(c *Config) { c.MaxSnapshotBytes = 0 }, "MaxSnapshotBytes must be > 0"},
		{"oversized max snapshot", func(c *Config) { c.MaxSnapshotBytes = maxSnapshotDataBytes + 1 }, "wire-safe max"},
		{"zero apply depth", func(c *Config) { c.ApplyChanDepth = 0 }, "ApplyChanDepth must be > 0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := base()
			tc.mutate(&c)
			err := c.Validate()
			switch {
			case tc.wantSub == "" && err != nil:
				t.Fatalf("want nil error, got %v", err)
			case tc.wantSub != "" && err == nil:
				t.Fatalf("want error containing %q, got nil", tc.wantSub)
			case tc.wantSub != "" && !strings.Contains(err.Error(), tc.wantSub):
				t.Fatalf("want error containing %q, got %v", tc.wantSub, err)
			}
		})
	}
}

func TestConfigurationHelpers(t *testing.T) {
	c := Configuration{Voters: map[NodeID]string{"a": "h:1", "b": "h:2", "c": "h:3"}}
	if !c.Has("b") || c.Has("z") {
		t.Fatalf("Has wrong")
	}
	if c.Quorum() != 2 {
		t.Fatalf("Quorum(3) = %d, want 2", c.Quorum())
	}
	if got := len(c.IDs()); got != 3 {
		t.Fatalf("IDs len = %d, want 3", got)
	}
	cl := c.Clone()
	cl.Voters["d"] = "h:4"
	if c.Has("d") {
		t.Fatalf("Clone is not deep")
	}
	withMetadata := Configuration{
		Voters:      map[NodeID]string{"a": "h:1"},
		ClientAddrs: map[NodeID]string{"a": "c:1"},
	}
	metadataClone := withMetadata.Clone()
	metadataClone.ClientAddrs["a"] = "changed"
	if withMetadata.ClientAddrs["a"] != "c:1" {
		t.Fatal("Clone did not copy client metadata")
	}
	five := Configuration{Voters: map[NodeID]string{"a": "", "b": "", "c": "", "d": "", "e": ""}}
	if five.Quorum() != 3 {
		t.Fatalf("Quorum(5) = %d, want 3", five.Quorum())
	}
}

func TestEntryTypeString(t *testing.T) {
	cases := map[EntryType]string{EntryNoOp: "noop", EntryNormal: "normal", EntryConfig: "config", EntryType(99): "entrytype(99)"}
	for et, want := range cases {
		if got := et.String(); got != want {
			t.Fatalf("EntryType(%d).String() = %q, want %q", et, got, want)
		}
	}
}
