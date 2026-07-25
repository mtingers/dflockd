package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	maxCheckedHistoryOps = 64
	maxHistoryLimit      = maxCheckedHistoryOps / 2
)

type historyKind uint8

const (
	historyAcquire historyKind = iota
	historyRelease
)

// historyOperation is one completed client call. A timed-out acquire has no
// observed token and may be omitted as an unknown operation. Failed releases
// are retained because a release can commit before its response is lost.
type historyOperation struct {
	worker    int
	kind      historyKind
	key       string
	token     string
	start     uint64
	end       uint64
	startNano int64
	endNano   int64
	lease     time.Duration
	success   bool
	tokenBit  uint64
}

type historyRecorder struct {
	mu       sync.Mutex
	epoch    time.Time
	seq      uint64
	limit    int
	byKey    map[string][]*historyOperation
	admitted map[string]int
	tracked  map[string]map[string]bool
	started  int
}

func newHistoryRecorder(limit int) *historyRecorder {
	return &historyRecorder{
		epoch:    time.Now(),
		limit:    limit,
		byKey:    map[string][]*historyOperation{},
		admitted: map[string]int{},
		tracked:  map[string]map[string]bool{},
	}
}

func (r *historyRecorder) begin(worker int, kind historyKind, key, token string) *historyOperation {
	if r == nil || r.limit == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.beginLocked(worker, kind, key, token)
}

func (r *historyRecorder) beginLocked(worker int, kind historyKind, key, token string) *historyOperation {
	if !r.admitLocked(kind, key, token) {
		return nil
	}
	r.seq++
	op := &historyOperation{
		worker: worker, kind: kind, key: key, token: token,
		start: r.seq, startNano: time.Since(r.epoch).Nanoseconds(),
	}
	r.appendLocked(key, op)
	return op
}

func (r *historyRecorder) admitLocked(kind historyKind, key, token string) bool {
	if r.admitted[key] < r.limit {
		r.admitted[key]++
		r.stopTrackingReleaseLocked(kind, key, token)
		return true
	}
	if kind != historyRelease || !r.tracked[key][token] {
		return false
	}
	delete(r.tracked[key], token)
	return true
}

func (r *historyRecorder) stopTrackingReleaseLocked(kind historyKind, key, token string) {
	if kind == historyRelease {
		delete(r.tracked[key], token)
	}
}

func (r *historyRecorder) appendLocked(key string, op *historyOperation) {
	r.byKey[key] = append(r.byKey[key], op)
	r.started++
}

func (r *historyRecorder) finish(op *historyOperation, token string, lease time.Duration, success bool) {
	if op == nil {
		return
	}
	r.mu.Lock()
	op.token = token
	op.lease = lease
	op.success = success
	op.endNano = time.Since(r.epoch).Nanoseconds()
	r.seq++
	op.end = r.seq
	if op.kind == historyAcquire && success {
		r.trackTokenLocked(op.key, token)
	}
	r.mu.Unlock()
}

func (r *historyRecorder) trackTokenLocked(key, token string) {
	if r.tracked[key] == nil {
		r.tracked[key] = map[string]bool{}
	}
	r.tracked[key][token] = true
}

func (r *historyRecorder) count() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.started
}

func (r *historyRecorder) violations(clockSkew time.Duration) []string {
	histories, incomplete := r.snapshot()
	var violations []string
	if incomplete > 0 {
		violations = append(violations, fmt.Sprintf("soak: history has %d incomplete operations", incomplete))
	}
	for _, key := range historyKeys(histories) {
		violations = append(violations, historyViolation(key, histories[key], clockSkew)...)
	}
	return violations
}

func historyKeys(histories map[string][]historyOperation) []string {
	keys := make([]string, 0, len(histories))
	for key := range histories {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func historyViolation(key string, ops []historyOperation, clockSkew time.Duration) []string {
	if err := checkLockHistory(ops, clockSkew); err != nil {
		return []string{fmt.Sprintf("soak: history %s: %v", key, err)}
	}
	return nil
}

func (r *historyRecorder) snapshot() (map[string][]historyOperation, int) {
	if r == nil {
		return map[string][]historyOperation{}, 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotLocked()
}

func (r *historyRecorder) snapshotLocked() (map[string][]historyOperation, int) {
	out := map[string][]historyOperation{}
	incomplete := 0
	for key, recorded := range r.byKey {
		var missing int
		out[key], missing = completedHistory(recorded)
		incomplete += missing
	}
	return out, incomplete
}

func completedHistory(recorded []*historyOperation) ([]historyOperation, int) {
	out := make([]historyOperation, 0, len(recorded))
	incomplete := 0
	for _, op := range recorded {
		if op.end == 0 {
			incomplete++
		} else {
			out = append(out, *op)
		}
	}
	return out, incomplete
}

type historyState struct {
	token   string
	worker  int
	expires int64
	seen    uint64
}

type historyMemo struct {
	done    uint64
	token   string
	worker  int
	expires int64
	seen    uint64
}

func checkLockHistory(recorded []historyOperation, clockSkew time.Duration) error {
	ops, err := prepareHistory(recorded, clockSkew)
	if err != nil {
		return err
	}
	if len(ops) > 0 && !historyLinearizable(ops, clockSkew) {
		return fmt.Errorf("%d completed operations are not linearizable: %s", len(ops), historySummary(ops))
	}
	return nil
}

func historySummary(ops []historyOperation) string {
	parts := make([]string, len(ops))
	for i, op := range ops {
		parts[i] = fmt.Sprintf("%s%d%s:%s@%d-%d", historyKindName(op.kind), op.worker,
			historyOutcomeName(op.success), shortHistoryToken(op.token), op.start, op.end)
	}
	return strings.Join(parts, " ")
}

func historyKindName(kind historyKind) string {
	if kind == historyAcquire {
		return "a"
	}
	return "r"
}

func historyOutcomeName(success bool) string {
	if success {
		return "+"
	}
	return "?"
}

func shortHistoryToken(token string) string {
	if len(token) > 16 {
		return token[:16]
	}
	return token
}

func historyLinearizable(ops []historyOperation, clockSkew time.Duration) bool {
	predecessors := historyPredecessors(ops)
	all := ^uint64(0)
	if len(ops) < maxCheckedHistoryOps {
		all = uint64(1)<<len(ops) - 1
	}
	return linearizeHistory(ops, predecessors, all, 0, historyState{}, clockSkew, map[historyMemo]bool{})
}

func prepareHistory(recorded []historyOperation, clockSkew time.Duration) ([]historyOperation, error) {
	if len(recorded) > maxCheckedHistoryOps {
		return nil, fmt.Errorf("operation count %d exceeds exact-check limit %d", len(recorded), maxCheckedHistoryOps)
	}
	return prepareBoundedHistory(recorded, clockSkew)
}

func prepareBoundedHistory(recorded []historyOperation, clockSkew time.Duration) ([]historyOperation, error) {
	ops, tokenBits := make([]historyOperation, 0, len(recorded)), map[string]uint64{}
	for i, op := range recorded {
		if err := validateHistoryOperation(i, op, clockSkew); err != nil {
			return nil, err
		}
		if prepared, keep := prepareHistoryOperation(op, tokenBits); keep {
			ops = append(ops, prepared)
		}
	}
	return ops, nil
}

func prepareHistoryOperation(op historyOperation, tokenBits map[string]uint64) (historyOperation, bool) {
	if op.kind == historyAcquire && !op.success {
		return historyOperation{}, false
	}
	if op.kind == historyAcquire {
		op.tokenBit = historyTokenBit(tokenBits, op.token)
	}
	return op, true
}

func validateHistoryOperation(index int, op historyOperation, clockSkew time.Duration) error {
	if op.start == 0 || op.end <= op.start || op.endNano < op.startNano {
		return fmt.Errorf("operation %d has invalid interval", index)
	}
	if op.kind == historyRelease && op.token == "" {
		return fmt.Errorf("operation %d released without a token", index)
	}
	if op.kind != historyAcquire || !op.success {
		return nil
	}
	return validateSuccessfulAcquire(index, op, clockSkew)
}

func validateSuccessfulAcquire(index int, op historyOperation, clockSkew time.Duration) error {
	if op.token == "" {
		return fmt.Errorf("operation %d acquired without a token", index)
	}
	if op.lease <= 2*clockSkew {
		return fmt.Errorf("operation %d lease %s does not exceed twice clock skew %s", index, op.lease, clockSkew)
	}
	return nil
}

func historyTokenBit(tokenBits map[string]uint64, token string) uint64 {
	if bit := tokenBits[token]; bit != 0 {
		return bit
	}
	bit := uint64(1) << len(tokenBits)
	tokenBits[token] = bit
	return bit
}

func historyPredecessors(ops []historyOperation) []uint64 {
	out := make([]uint64, len(ops))
	for i := range ops {
		for j := range ops {
			if ops[j].end < ops[i].start {
				out[i] |= uint64(1) << j
			}
		}
	}
	return out
}

func linearizeHistory(ops []historyOperation, predecessors []uint64, all, done uint64, state historyState, clockSkew time.Duration, failed map[historyMemo]bool) bool {
	if done == all {
		return true
	}
	return linearizeUnfinished(ops, predecessors, all, done, state, clockSkew, failed)
}

func linearizeUnfinished(ops []historyOperation, predecessors []uint64, all, done uint64, state historyState, clockSkew time.Duration, failed map[historyMemo]bool) bool {
	memo := historyMemoFor(done, state)
	if failed[memo] {
		return false
	}
	if tryHistoryOperations(ops, predecessors, all, done, state, clockSkew, failed) {
		return true
	}
	failed[memo] = true
	return false
}

func historyMemoFor(done uint64, state historyState) historyMemo {
	return historyMemo{
		done: done, token: state.token, worker: state.worker,
		expires: state.expires, seen: state.seen,
	}
}

func tryHistoryOperations(ops []historyOperation, predecessors []uint64, all, done uint64, state historyState, clockSkew time.Duration, failed map[historyMemo]bool) bool {
	for i, op := range ops {
		bit := uint64(1) << i
		if done&bit != 0 || predecessors[i]&^done != 0 {
			continue
		}
		if tryHistoryTransitions(ops, predecessors, all, done|bit, state, op, clockSkew, failed) {
			return true
		}
	}
	return false
}

func tryHistoryTransitions(ops []historyOperation, predecessors []uint64, all, done uint64, state historyState, op historyOperation, clockSkew time.Duration, failed map[historyMemo]bool) bool {
	for _, next := range historyTransitions(state, op, clockSkew) {
		if linearizeHistory(ops, predecessors, all, done, next, clockSkew, failed) {
			return true
		}
	}
	return false
}

func historyTransitions(state historyState, op historyOperation, clockSkew time.Duration) []historyState {
	if !op.success {
		return uncertainHistoryTransitions(state, op)
	}
	if op.kind == historyRelease {
		return releaseHistoryTransitions(state, op)
	}
	return historyAcquireTransitions(state, op, clockSkew)
}

func uncertainHistoryTransitions(state historyState, op historyOperation) []historyState {
	if op.kind == historyRelease && state.token == op.token {
		return []historyState{state, {seen: state.seen}}
	}
	return []historyState{state}
}

func releaseHistoryTransitions(state historyState, op historyOperation) []historyState {
	if state.token != op.token {
		return nil
	}
	return []historyState{{seen: state.seen}}
}

func historyAcquireTransitions(state historyState, op historyOperation, clockSkew time.Duration) []historyState {
	reattach := state.token == op.token && state.worker == op.worker
	if !reattach && state.seen&op.tokenBit != 0 {
		return nil
	}
	linearizedAt, ok := historyAcquireTime(state, op, reattach)
	if !ok {
		return nil
	}
	minimumValidity := op.lease - 2*clockSkew
	return []historyState{newHistoryHolder(state, op, linearizedAt, minimumValidity)}
}

func newHistoryHolder(state historyState, op historyOperation, linearizedAt int64, minimumValidity time.Duration) historyState {
	return historyState{
		token: op.token, worker: op.worker,
		expires: historyExpiry(linearizedAt, minimumValidity),
		seen:    state.seen | op.tokenBit,
	}
}

func historyExpiry(linearizedAt int64, validity time.Duration) int64 {
	nanos := validity.Nanoseconds()
	if linearizedAt > math.MaxInt64-nanos {
		return math.MaxInt64
	}
	return linearizedAt + nanos
}

func historyAcquireTime(state historyState, op historyOperation, reattach bool) (int64, bool) {
	if state.token == "" || reattach {
		return op.startNano, true
	}
	if state.expires > op.endNano {
		return 0, false
	}
	return max(op.startNano, state.expires), true
}
