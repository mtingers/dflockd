package main

import (
	"fmt"
	"math"
	"testing"
	"time"
)

const testLease = 10 * time.Second

func TestCheckLockHistoryTransitions(t *testing.T) {
	tests := []struct {
		name string
		ops  []historyOperation
		skew time.Duration
		ok   bool
	}{
		{
			name: "release then next holder",
			ops: []historyOperation{
				historySuccess(0, historyAcquire, "a", 1, 2, 0, 1),
				historySuccess(0, historyRelease, "a", 3, 4, 2, 3),
				historySuccess(1, historyAcquire, "b", 5, 6, 4, 5),
			},
			ok: true,
		},
		{
			name: "overlapping release linearizes before acquire",
			ops: []historyOperation{
				historySuccess(0, historyAcquire, "a", 1, 2, 0, 1),
				historySuccess(0, historyRelease, "a", 3, 6, 2, 5),
				historySuccess(1, historyAcquire, "b", 4, 5, 3, 4),
			},
			ok: true,
		},
		{
			name: "waiter response crosses holder release",
			ops: []historyOperation{
				historySuccess(1, historyAcquire, "b", 1, 5, 0, 4),
				historySuccess(0, historyAcquire, "a", 2, 3, 1, 2),
				historySuccess(0, historyRelease, "a", 4, 6, 3, 5),
				historySuccess(1, historyRelease, "b", 7, 10, 6, 9),
				historySuccess(0, historyAcquire, "c", 8, 9, 7, 8),
			},
			ok: true,
		},
		{
			name: "overlapping grants without release",
			ops: []historyOperation{
				historySuccess(0, historyAcquire, "a", 1, 4, 0, 3),
				historySuccess(1, historyAcquire, "b", 2, 3, 1, 2),
			},
			ok: false,
		},
		{
			name: "uncertain release may commit",
			ops: []historyOperation{
				historySuccess(0, historyAcquire, "a", 1, 2, 0, 1),
				historyFailure(0, historyRelease, "a", 3, 6, 2, 5),
				historySuccess(1, historyAcquire, "b", 4, 5, 3, 4),
			},
			ok: true,
		},
		{
			name: "wrong uncertain release cannot clear",
			ops: []historyOperation{
				historySuccess(0, historyAcquire, "a", 1, 2, 0, 1),
				historyFailure(0, historyRelease, "wrong", 3, 6, 2, 5),
				historySuccess(1, historyAcquire, "b", 4, 5, 3, 4),
			},
			ok: false,
		},
		{
			name: "same worker reattaches token",
			ops: []historyOperation{
				historySuccess(0, historyAcquire, "a", 1, 2, 0, 1),
				historySuccess(0, historyAcquire, "a", 3, 4, 2, 3),
				historySuccess(0, historyRelease, "a", 5, 6, 4, 5),
			},
			ok: true,
		},
		{
			name: "different worker cannot reattach token",
			ops: []historyOperation{
				historySuccess(0, historyAcquire, "a", 1, 2, 0, 1),
				historySuccess(1, historyAcquire, "a", 3, 4, 2, 3),
			},
			ok: false,
		},
		{
			name: "retired token cannot return",
			ops: []historyOperation{
				historySuccess(0, historyAcquire, "a", 1, 2, 0, 1),
				historySuccess(0, historyRelease, "a", 3, 4, 2, 3),
				historySuccess(0, historyAcquire, "a", 5, 6, 4, 5),
			},
			ok: false,
		},
		{
			name: "lease can expire after skew margin",
			ops: []historyOperation{
				historySuccess(0, historyAcquire, "a", 1, 2, 0, int64(time.Second)),
				historySuccess(1, historyAcquire, "b", 3, 4, int64(7*time.Second), int64(8*time.Second)),
			},
			skew: 2 * time.Second,
			ok:   true,
		},
		{
			name: "lease cannot expire before skew margin",
			ops: []historyOperation{
				historySuccess(0, historyAcquire, "a", 1, 2, 0, int64(time.Second)),
				historySuccess(1, historyAcquire, "b", 3, 4, int64(5*time.Second), int64(5*time.Second)),
			},
			skew: 2 * time.Second,
			ok:   false,
		},
		{
			name: "failed acquire is omittable",
			ops: []historyOperation{
				historyFailure(0, historyAcquire, "", 1, 4, 0, 3),
				historySuccess(1, historyAcquire, "b", 2, 3, 1, 2),
			},
			ok: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkLockHistory(tt.ops, tt.skew)
			if (err == nil) != tt.ok {
				t.Fatalf("checkLockHistory() error = %v, want success %t", err, tt.ok)
			}
		})
	}
}

func TestCheckLockHistoryExhaustiveSequentialOrders(t *testing.T) {
	base := []historyOperation{
		historySuccess(0, historyAcquire, "a", 0, 0, 0, 0),
		historySuccess(0, historyRelease, "a", 0, 0, 0, 0),
		historySuccess(1, historyAcquire, "b", 0, 0, 0, 0),
		historySuccess(1, historyRelease, "b", 0, 0, 0, 0),
	}
	permutations := 0
	eachPermutation(len(base), func(order []int) {
		permutations++
		ops := append([]historyOperation(nil), base...)
		for position, index := range order {
			start := uint64(2*position + 1)
			ops[index].start, ops[index].end = start, start+1
			ops[index].startNano = int64(start)
			ops[index].endNano = int64(start + 1)
		}
		got := checkLockHistory(ops, 0) == nil
		want := sequentialHistoryValid(ops, order)
		if got != want {
			t.Fatalf("order %v: success = %t, want %t", order, got, want)
		}
	})
	if permutations != 24 {
		t.Fatalf("permutations = %d, want 24", permutations)
	}
}

func TestCheckLockHistoryAtMaximumBound(t *testing.T) {
	ops := make([]historyOperation, 0, maxCheckedHistoryOps)
	var event uint64 = 1
	for worker := 0; worker < maxCheckedHistoryOps/2; worker++ {
		token := fmt.Sprintf("token-%d", worker)
		ops = append(ops, historySuccess(worker, historyAcquire, token, event, event+1, int64(event), int64(event+1)))
		event += 2
		ops = append(ops, historySuccess(worker, historyRelease, token, event, event+1, int64(event), int64(event+1)))
		event += 2
	}
	if err := checkLockHistory(ops, 0); err != nil {
		t.Fatalf("maximum history: %v", err)
	}
	ops = append(ops, historyFailure(0, historyRelease, "extra", event, event+1, int64(event), int64(event+1)))
	if err := checkLockHistory(ops, 0); err == nil {
		t.Fatal("history beyond exact-check bound accepted")
	}
}

func TestHistoryExpirySaturates(t *testing.T) {
	if got := historyExpiry(math.MaxInt64-1, 2*time.Nanosecond); got != math.MaxInt64 {
		t.Fatalf("historyExpiry = %d, want saturation", got)
	}
}

func TestHistoryRecorderBoundsAndChecks(t *testing.T) {
	recorder := newHistoryRecorder(2)
	first := recorder.begin(0, historyAcquire, "key", "")
	recorder.finish(first, "a", testLease, true)
	second := recorder.begin(0, historyRelease, "key", "a")
	recorder.finish(second, "a", 0, true)
	if extra := recorder.begin(1, historyAcquire, "key", ""); extra != nil {
		t.Fatal("recorder exceeded per-key limit")
	}
	other := recorder.begin(1, historyAcquire, "other", "")
	recorder.finish(other, "b", testLease, true)
	if got := recorder.count(); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}
	if violations := recorder.violations(0); len(violations) != 0 {
		t.Fatalf("violations = %v", violations)
	}
}

func TestHistoryRecorderClosesBoundWithTrackedRelease(t *testing.T) {
	recorder := newHistoryRecorder(1)
	acquire := recorder.begin(0, historyAcquire, "key", "")
	recorder.finish(acquire, "a", testLease, true)
	release := recorder.begin(0, historyRelease, "key", "a")
	if release == nil {
		t.Fatal("release for recorded acquire omitted after initial bound")
	}
	recorder.finish(release, "a", 0, true)
	if extra := recorder.begin(0, historyAcquire, "key", ""); extra != nil {
		t.Fatal("new acquire admitted after initial bound")
	}
	if violations := recorder.violations(0); len(violations) != 0 {
		t.Fatalf("violations = %v", violations)
	}
}

func TestHistoryRecorderClosureStaysWithinCheckerBound(t *testing.T) {
	recorder := newHistoryRecorder(maxHistoryLimit)
	for worker := 0; worker < maxHistoryLimit; worker++ {
		op := recorder.begin(worker, historyAcquire, "key", "")
		recorder.finish(op, fmt.Sprintf("token-%d", worker), testLease, true)
	}
	for worker := 0; worker < maxHistoryLimit; worker++ {
		token := fmt.Sprintf("token-%d", worker)
		op := recorder.begin(worker, historyRelease, "key", token)
		if op == nil {
			t.Fatalf("tracked release %d omitted", worker)
		}
		recorder.finish(op, token, 0, false)
	}
	if got := recorder.count(); got != maxCheckedHistoryOps {
		t.Fatalf("count = %d, want %d", got, maxCheckedHistoryOps)
	}
	if extra := recorder.begin(0, historyRelease, "key", "token-0"); extra != nil {
		t.Fatal("second release attempt admitted after closure")
	}
}

func historySuccess(worker int, kind historyKind, token string, start, end uint64, startNano, endNano int64) historyOperation {
	return historyOperation{
		worker: worker, kind: kind, key: "key", token: token,
		start: start, end: end, startNano: startNano, endNano: endNano,
		lease: testLease, success: true,
	}
}

func historyFailure(worker int, kind historyKind, token string, start, end uint64, startNano, endNano int64) historyOperation {
	op := historySuccess(worker, kind, token, start, end, startNano, endNano)
	op.success = false
	return op
}

func eachPermutation(size int, visit func([]int)) {
	order := make([]int, size)
	used := make([]bool, size)
	var walk func(int)
	walk = func(position int) {
		if position == size {
			visit(append([]int(nil), order...))
			return
		}
		for value := 0; value < size; value++ {
			if used[value] {
				continue
			}
			used[value] = true
			order[position] = value
			walk(position + 1)
			used[value] = false
		}
	}
	walk(0)
}

func sequentialHistoryValid(ops []historyOperation, order []int) bool {
	token := ""
	for _, index := range order {
		op := ops[index]
		switch op.kind {
		case historyAcquire:
			if token != "" {
				return false
			}
			token = op.token
		case historyRelease:
			if token != op.token {
				return false
			}
			token = ""
		default:
			panic(fmt.Sprintf("unknown history kind %d", op.kind))
		}
	}
	return true
}
