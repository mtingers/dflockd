package main

import (
	"strings"
	"testing"
)

func TestValidateBenchFlagsRejectsInvalidInputs(t *testing.T) {
	tests := []struct {
		name string
		args []any
		want string
	}{
		{name: "workers", args: []any{0, 1, 1, 1, 0, 0, "127.0.0.1:6388"}, want: "workers"},
		{name: "rounds", args: []any{1, 0, 1, 1, 0, 0, "127.0.0.1:6388"}, want: "rounds"},
		{name: "timeout", args: []any{1, 1, -1, 1, 0, 0, "127.0.0.1:6388"}, want: "timeout"},
		{name: "lease", args: []any{1, 1, 1, -1, 0, 0, "127.0.0.1:6388"}, want: "lease"},
		{name: "connections", args: []any{1, 1, 1, 1, -1, 0, "127.0.0.1:6388"}, want: "connections"},
		{name: "warmup", args: []any{1, 1, 1, 1, 0, -1, "127.0.0.1:6388"}, want: "warmup"},
		{name: "empty server", args: []any{1, 1, 1, 1, 0, 0, "127.0.0.1:6388,"}, want: "servers"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := validateBenchFlags(
				tc.args[0].(int),
				tc.args[1].(int),
				tc.args[2].(int),
				tc.args[3].(int),
				tc.args[4].(int),
				tc.args[5].(int),
				tc.args[6].(string),
			)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestValidateBenchFlagsDefaultsConnections(t *testing.T) {
	addrs, conns, err := validateBenchFlags(2, 3, 4, 5, 0, 1, " a:1 , b:2 ")
	if err != nil {
		t.Fatal(err)
	}
	if conns != 1 {
		t.Fatalf("connections: got %d want 1", conns)
	}
	if len(addrs) != 2 || addrs[0] != "a:1" || addrs[1] != "b:2" {
		t.Fatalf("addrs: %#v", addrs)
	}
}
