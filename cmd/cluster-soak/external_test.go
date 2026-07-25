package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseExternalMembers(t *testing.T) {
	members, err := parseExternalMembers("a=127.0.0.1:7001,b=[::1]:7002,c=host.example:7003")
	if err != nil {
		t.Fatalf("parseExternalMembers: %v", err)
	}
	if len(members) != 3 || members[1].ID != "b" || members[1].Addr != "[::1]:7002" {
		t.Fatalf("members = %+v", members)
	}
}

func TestParseExternalMembersRejectsInvalidTopology(t *testing.T) {
	tests := []string{
		"a=127.0.0.1:1,b=127.0.0.1:2",
		"a=127.0.0.1:1,b=127.0.0.1:2,a=127.0.0.1:3",
		"a=127.0.0.1:1,b=127.0.0.1:2,c=127.0.0.1:1",
		"a=127.0.0.1:0,b=127.0.0.1:2,c=127.0.0.1:3",
		"bad id=127.0.0.1:1,b=127.0.0.1:2,c=127.0.0.1:3",
	}
	for _, spec := range tests {
		t.Run(spec, func(t *testing.T) {
			if _, err := parseExternalMembers(spec); err == nil {
				t.Fatalf("parseExternalMembers(%q) succeeded", spec)
			}
		})
	}
}

func TestParseExternalFlagsWorkloadOnly(t *testing.T) {
	opts, err := parseSoakFlags([]string{
		"--targets=a=127.0.0.1:1,b=127.0.0.1:2,c=127.0.0.1:3",
		"--fault-interval=0",
		"--lease-ttl=1s",
		"--duration=1s",
	})
	if err != nil {
		t.Fatalf("parseSoakFlags: %v", err)
	}
	if opts.Targets == "" || opts.FaultInterval != 0 {
		t.Fatalf("external options = %+v", opts)
	}
}

func TestParseExternalFlagsRequiresFaultHook(t *testing.T) {
	_, err := parseSoakFlags([]string{
		"--targets=a=127.0.0.1:1,b=127.0.0.1:2,c=127.0.0.1:3",
	})
	if err == nil || !strings.Contains(err.Error(), "--fault-hook") {
		t.Fatalf("error = %v, want missing fault hook", err)
	}
}

func TestParseExternalFlagsRejectsExtremeClockSkew(t *testing.T) {
	_, err := parseSoakFlags([]string{
		"--targets=a=127.0.0.1:1,b=127.0.0.1:2,c=127.0.0.1:3",
		"--fault-interval=0",
		"--clock-skew=25h",
	})
	if err == nil {
		t.Fatal("25h clock skew accepted")
	}
}

func TestReadOptionalSecretRejectsMultipleLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("first\nsecond\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := readOptionalSecret(path); err == nil {
		t.Fatal("multi-line auth token accepted")
	}
}

func TestNewExternalHarnessRejectsMissingFaultHook(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := newExternalHarness(soakOpts{
		Targets:   "a=127.0.0.1:1,b=127.0.0.1:2,c=127.0.0.1:3",
		FaultHook: filepath.Join(t.TempDir(), "missing-hook"),
	}, log, nil)
	if err == nil {
		t.Fatal("missing fault hook accepted")
	}
}

func TestGrantLedgerInvariants(t *testing.T) {
	token10 := soakToken(10, 1)
	token11 := soakToken(11, 2)
	token09 := soakToken(9, 3)

	var ledger grantLedger
	ledger.tokens = map[string]grantRecord{}
	ledger.maxByKey = map[string]uint64{}
	ledger.activeByKey = map[string]string{}
	if got := ledger.recordGrant(1, "key", token10); got != "" {
		t.Fatalf("first grant: %s", got)
	}
	if got := ledger.recordGrant(1, "key", token10); got != "" {
		t.Fatalf("same-session reattach: %s", got)
	}
	if got := ledger.recordGrant(1, "key", token11); got != "" {
		t.Fatalf("next fence: %s", got)
	}
	if got := ledger.recordGrant(1, "key", token09); !strings.Contains(got, "fence regression") {
		t.Fatalf("regression = %q", got)
	}

	ledger.recordRelease(token10)
	if got := ledger.recordGrant(1, "key", token10); !strings.Contains(got, "token reused") {
		t.Fatalf("retired reuse = %q", got)
	}
	if got := ledger.recordGrant(2, "key", token11); !strings.Contains(got, "token reused") {
		t.Fatalf("cross-worker reuse = %q", got)
	}
}

func TestGrantLedgerRejectsHistoricalReattach(t *testing.T) {
	token10 := soakToken(10, 1)
	token11 := soakToken(11, 2)
	ledger := grantLedger{
		tokens: map[string]grantRecord{}, maxByKey: map[string]uint64{},
		activeByKey: map[string]string{},
	}
	if got := ledger.recordGrant(1, "key", token10); got != "" {
		t.Fatal(got)
	}
	if got := ledger.recordGrant(2, "key", token11); got != "" {
		t.Fatal(got)
	}
	if got := ledger.recordGrant(1, "key", token10); !strings.Contains(got, "token reused") {
		t.Fatalf("historical reattach = %q", got)
	}
}

func TestParseExternalHistoryFlags(t *testing.T) {
	opts, err := parseSoakFlags([]string{
		"--targets=a=127.0.0.1:1,b=127.0.0.1:2,c=127.0.0.1:3",
		"--workers=4", "--keys=2", "--history-limit=32",
		"--fault-interval=0", "--duration=1s",
	})
	if err != nil {
		t.Fatalf("parseSoakFlags: %v", err)
	}
	if opts.Keys != 2 || opts.HistoryLimit != 32 {
		t.Fatalf("history options = %+v", opts)
	}
}

func TestParseExternalHistoryFlagsRejectUnsafeBounds(t *testing.T) {
	tests := [][]string{
		{"--workers=2", "--keys=3"},
		{"--history-limit=33"},
		{"--lease-ttl=4s", "--clock-skew=2s", "--fault-interval=1s", "--fault-hook=hook"},
	}
	for _, args := range tests {
		allArgs := append([]string{
			"--targets=a=127.0.0.1:1,b=127.0.0.1:2,c=127.0.0.1:3",
			"--fault-interval=0", "--duration=1s",
		}, args...)
		if _, err := parseSoakFlags(allArgs); err == nil {
			t.Fatalf("parseSoakFlags(%v) succeeded", allArgs)
		}
	}
}

func soakToken(fence, salt uint64) string {
	return fmt.Sprintf("%016x%016x", fence, salt)
}

type recordedFault struct {
	action string
	node   string
	arg    string
}

type recordingFaultController struct {
	mu         sync.Mutex
	calls      []recordedFault
	failAction string
}

func (c *recordingFaultController) Run(_ context.Context, action, node, arg string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, recordedFault{action: action, node: node, arg: arg})
	if action == c.failAction {
		c.failAction = ""
		return fmt.Errorf("injected %s failure", action)
	}
	return nil
}

func TestExternalFaultCycle(t *testing.T) {
	hook := &recordingFaultController{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h, err := newExternalHarness(soakOpts{
		Targets:   "a=127.0.0.1:1,b=127.0.0.1:2,c=127.0.0.1:3",
		FaultHold: time.Millisecond,
		ClockSkew: 2 * time.Second,
	}, log, hook)
	if err != nil {
		t.Fatalf("newExternalHarness: %v", err)
	}
	h.probe = func(context.Context, []externalMember, string) (string, error) {
		return "a", nil
	}
	for step := 0; step < 4; step++ {
		if err := h.runFaultStep(context.Background(), step); err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
	}
	want := []recordedFault{
		{action: "partition", node: "a"},
		{action: "heal", node: "a"},
		{action: "skew", node: "b", arg: "2s"},
		{action: "restart", node: "a"},
		{action: "unskew", node: "b"},
	}
	if fmt.Sprint(hook.calls) != fmt.Sprint(want) {
		t.Fatalf("calls = %+v, want %+v", hook.calls, want)
	}
	report := h.report()
	if report.Partitions != 1 || report.Restarts != 1 || report.Skews != 1 {
		t.Fatalf("fault counters = %+v", report)
	}
}

func TestExternalFaultCleanup(t *testing.T) {
	hook := &recordingFaultController{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h, err := newExternalHarness(soakOpts{
		Targets: "a=127.0.0.1:1,b=127.0.0.1:2,c=127.0.0.1:3",
	}, log, hook)
	if err != nil {
		t.Fatalf("newExternalHarness: %v", err)
	}
	h.partitioned = "a"
	h.skewed = "b"
	h.cleanupFaults()
	want := []recordedFault{
		{action: "heal", node: "a"},
		{action: "unskew", node: "b"},
	}
	if fmt.Sprint(hook.calls) != fmt.Sprint(want) {
		t.Fatalf("calls = %+v, want %+v", hook.calls, want)
	}
}

func TestExternalFaultCleanupAfterAmbiguousHookFailure(t *testing.T) {
	hook := &recordingFaultController{failAction: "partition"}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h, err := newExternalHarness(soakOpts{
		Targets: "a=127.0.0.1:1,b=127.0.0.1:2,c=127.0.0.1:3",
	}, log, hook)
	if err != nil {
		t.Fatalf("newExternalHarness: %v", err)
	}
	h.probe = func(context.Context, []externalMember, string) (string, error) {
		return "a", nil
	}
	if err := h.partitionLeader(context.Background()); err == nil {
		t.Fatal("partitionLeader succeeded")
	}
	h.cleanupFaults()
	want := []recordedFault{
		{action: "partition", node: "a"},
		{action: "heal", node: "a"},
	}
	if fmt.Sprint(hook.calls) != fmt.Sprint(want) {
		t.Fatalf("calls = %+v, want %+v", hook.calls, want)
	}
}

func TestExternalFinalStateRequiresProgressAndLeader(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h, err := newExternalHarness(soakOpts{
		Targets: "a=127.0.0.1:1,b=127.0.0.1:2,c=127.0.0.1:3",
	}, log, nil)
	if err != nil {
		t.Fatalf("newExternalHarness: %v", err)
	}
	h.probe = func(context.Context, []externalMember, string) (string, error) {
		return "", fmt.Errorf("unavailable")
	}
	h.verifyFinalState()
	violations := h.report().Violations
	if len(violations) != 2 {
		t.Fatalf("violations = %v, want progress and leader failures", violations)
	}
}
