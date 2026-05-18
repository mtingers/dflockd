package lock

import (
	"crypto/rand"
	"strings"
	"sync"
	"time"
)

// LockPrefix and SemPrefix namespace the user-visible key so the same
// name can be used as a lock and a semaphore without conflict. Callers
// (server, httpapi) prepend the prefix; StripKeyPrefix reverses it for
// stats/display.
const (
	LockPrefix = "lock:"
	SemPrefix  = "sem:"
)

// StripKeyPrefix removes the internal namespace prefix for external display.
func StripKeyPrefix(key string) string {
	if after, ok := strings.CutPrefix(key, LockPrefix); ok {
		return after
	}
	if after, ok := strings.CutPrefix(key, SemPrefix); ok {
		return after
	}
	return key
}

// holder is one currently-held slot in a ResourceState.
//
// Ref is the cluster-mode requester identifier. Single-node callers leave
// it empty; the existing connID-based routing remains the source of
// truth for them. Both fields coexist so that cluster failover (where
// connID is meaningless across nodes) can route by Ref while the
// single-node fast paths continue to key off connID.
//
// abandonedAtNanos != 0 means the original connection went away while the
// holder had a stable ref. The slot is preserved (not removed via
// CleanupConn) and the FSM's evict sweep retires it after OrphanTTL.
// A reconnect with matching ref before that deadline re-adopts the
// holder (resets abandonedAtNanos, rebinds connID).
type holder struct {
	connID           uint64
	leaseExpires     time.Time
	ref              string
	abandonedAtNanos int64
}

// waiter is a queued grant request waiting for capacity.
//
// Ref / Salt are populated only by the FSM apply path so a leadership
// change can preserve which client a queued slot belongs to and so
// promotion can mint a token deterministically; single-node callers
// continue to use ch + connID for routing.
//
// abandonedAtNanos behaves identically to the same field on holder: a
// stable-ref waiter whose connection died is parked here until the
// caller reconnects (and re-adopts) or the OrphanTTL fires.
type waiter struct {
	ch               chan string
	connID           uint64
	leaseTTL         time.Duration
	ref              string
	salt             [8]byte
	abandonedAtNanos int64
}

// connKey identifies one connection's two-phase state for one key.
type connKey struct {
	ConnID uint64
	Key    string
}

// enqueuedState is the per-(conn,key) two-phase state. Exactly one of
// {waiter, token} is set: a non-empty token means the fast-path acquire
// during Enqueue succeeded; a non-nil waiter means the request is queued
// pending Wait.
type enqueuedState struct {
	waiter   *waiter
	token    string
	leaseTTL time.Duration
}

// ResourceState is the shared state for one key. A lock is just a
// semaphore with Limit==1.
type ResourceState struct {
	Limit        int
	Holders      map[string]*holder // token → holder
	Waiters      []*waiter
	WaiterHead   int // index of first active waiter (rest are nil-tombstones)
	LastActivity time.Time
}

// waiterCount returns the number of active waiters, excluding head tombstones.
func (rs *ResourceState) waiterCount() int {
	return len(rs.Waiters) - rs.WaiterHead
}

// compactWaiters reclaims dead head-tombstone slots when more than half
// the slice is dead. Must be called with the shard mutex held.
func (rs *ResourceState) compactWaiters() {
	if rs.WaiterHead > len(rs.Waiters)/2 {
		n := copy(rs.Waiters, rs.Waiters[rs.WaiterHead:])
		for i := n; i < len(rs.Waiters); i++ {
			rs.Waiters[i] = nil
		}
		rs.Waiters = rs.Waiters[:n]
		rs.WaiterHead = 0
	}
}

// removeWaiter removes target from rs's waiter queue. Must be called
// with the shard mutex held.
func (rs *ResourceState) removeWaiter(target *waiter) {
	for i := rs.WaiterHead; i < len(rs.Waiters); i++ {
		if rs.Waiters[i] == target {
			copy(rs.Waiters[i:], rs.Waiters[i+1:])
			rs.Waiters[len(rs.Waiters)-1] = nil
			rs.Waiters = rs.Waiters[:len(rs.Waiters)-1]
			return
		}
	}
}

// removeWaitersByConn removes every waiter belonging to connID and
// closes its channel exactly once (using the shared `closed` set so that
// a waiter on multiple shards isn't double-closed). Must be called with
// the shard mutex held.
func (rs *ResourceState) removeWaitersByConn(connID uint64, closed map[chan string]struct{}) {
	n := rs.WaiterHead
	for i := rs.WaiterHead; i < len(rs.Waiters); i++ {
		w := rs.Waiters[i]
		if w.connID == connID {
			if _, already := closed[w.ch]; !already {
				close(w.ch)
				closed[w.ch] = struct{}{}
			}
		} else {
			rs.Waiters[n] = w
			n++
		}
	}
	for i := n; i < len(rs.Waiters); i++ {
		rs.Waiters[i] = nil
	}
	rs.Waiters = rs.Waiters[:n]
}

// shard is one stripe of the lock-manager state. Each shard owns its
// own mutex; cross-shard operations (CleanupConnection) iterate shards
// one at a time without ever holding two mutexes.
type shard struct {
	mu               sync.Mutex
	resources        map[string]*ResourceState
	connOwned        map[uint64]map[string]map[string]struct{} // connID → key → set of tokens
	connEnqueued     map[connKey]*enqueuedState
	connEnqueuedByID map[uint64]map[string]struct{} // connID → set of keys with two-phase state
}

func (sh *shard) init() {
	sh.resources = make(map[string]*ResourceState)
	sh.connOwned = make(map[uint64]map[string]map[string]struct{})
	sh.connEnqueued = make(map[connKey]*enqueuedState)
	sh.connEnqueuedByID = make(map[uint64]map[string]struct{})
}

// connID == 0 is reserved as a sentinel meaning "skip per-connection
// bookkeeping." The accept loop allocates IDs starting at 1, so this
// only matters for transport-agnostic test callers.

func (sh *shard) addOwned(connID uint64, key, token string) {
	if connID == 0 {
		return
	}
	m := sh.connOwned[connID]
	if m == nil {
		m = make(map[string]map[string]struct{})
		sh.connOwned[connID] = m
	}
	tokens := m[key]
	if tokens == nil {
		tokens = make(map[string]struct{})
		m[key] = tokens
	}
	tokens[token] = struct{}{}
}

func (sh *shard) removeOwned(connID uint64, key, token string) {
	if connID == 0 {
		return
	}
	m := sh.connOwned[connID]
	if m == nil {
		return
	}
	tokens := m[key]
	if tokens == nil {
		return
	}
	delete(tokens, token)
	if len(tokens) == 0 {
		delete(m, key)
	}
	if len(m) == 0 {
		delete(sh.connOwned, connID)
	}
}

func (sh *shard) setEnqueued(ck connKey, es *enqueuedState) {
	sh.connEnqueued[ck] = es
	if ck.ConnID == 0 {
		return
	}
	keys := sh.connEnqueuedByID[ck.ConnID]
	if keys == nil {
		keys = make(map[string]struct{})
		sh.connEnqueuedByID[ck.ConnID] = keys
	}
	keys[ck.Key] = struct{}{}
}

func (sh *shard) removeEnqueued(ck connKey) {
	delete(sh.connEnqueued, ck)
	if ck.ConnID == 0 {
		return
	}
	keys := sh.connEnqueuedByID[ck.ConnID]
	if keys == nil {
		return
	}
	delete(keys, ck.Key)
	if len(keys) == 0 {
		delete(sh.connEnqueuedByID, ck.ConnID)
	}
}

// enqueuedKeys returns every key for which connID has an enqueued state
// in this shard. Used during disconnect cleanup to avoid scanning the
// whole connEnqueued map on every connection close.
func (sh *shard) enqueuedKeys(connID uint64) []connKey {
	if connID == 0 {
		return nil
	}
	keys := sh.connEnqueuedByID[connID]
	if len(keys) == 0 {
		return nil
	}
	out := make([]connKey, 0, len(keys))
	for k := range keys {
		out = append(out, connKey{ConnID: connID, Key: k})
	}
	return out
}

// randBuf amortises crypto/rand syscalls by buffering 4096 bytes and
// dispensing arbitrary slice-sized chunks. fill is the only entry
// point; callers ask for exactly the bytes they need.
type randBuf struct {
	mu  sync.Mutex
	buf [4096]byte
	pos int // starts at len(buf) to force initial fill
}

func newRandBuf() randBuf {
	return randBuf{pos: 4096}
}

// fill copies len(dst) random bytes into dst. Requests larger than the
// reservoir bypass it entirely.
func (rb *randBuf) fill(dst []byte) {
	if len(dst) > len(rb.buf) {
		readRand(dst)
		return
	}
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.dispense(dst)
}

// dispense copies len(dst) bytes from the reservoir, refilling first
// if needed. Caller holds rb.mu.
func (rb *randBuf) dispense(dst []byte) {
	rb.refillIfNeeded(len(dst))
	copy(dst, rb.buf[rb.pos:rb.pos+len(dst)])
	rb.pos += len(dst)
}

// refillIfNeeded reads from crypto/rand into the reservoir when it
// can't satisfy n more bytes. Caller holds rb.mu.
func (rb *randBuf) refillIfNeeded(n int) {
	if rb.pos+n <= len(rb.buf) {
		return
	}
	readRand(rb.buf[:])
	rb.pos = 0
}

// readRand fills dst from crypto/rand or panics. crypto/rand should
// never fail in practice; treating it as fatal keeps callers tidy.
func readRand(dst []byte) {
	if _, err := rand.Read(dst); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
}
