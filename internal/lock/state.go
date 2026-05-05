package lock

import (
	"crypto/rand"
	"encoding/hex"
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
type holder struct {
	connID       uint64
	leaseExpires time.Time
}

// waiter is a queued grant request waiting for capacity.
type waiter struct {
	ch       chan string
	connID   uint64
	leaseTTL time.Duration
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

// tokenBuf amortises crypto/rand syscalls by buffering 4096 bytes
// (256 16-byte tokens) and dispensing 16 bytes per request.
type tokenBuf struct {
	mu  sync.Mutex
	buf [4096]byte
	pos int // starts at len(buf) to force initial fill
}

func newTokenBuf() tokenBuf {
	return tokenBuf{pos: 4096}
}

func (tb *tokenBuf) next() string {
	tb.mu.Lock()
	if tb.pos+16 > len(tb.buf) {
		if _, err := rand.Read(tb.buf[:]); err != nil {
			panic("crypto/rand failed: " + err.Error())
		}
		tb.pos = 0
	}
	tok := hex.EncodeToString(tb.buf[tb.pos : tb.pos+16])
	tb.pos += 16
	tb.mu.Unlock()
	return tok
}
