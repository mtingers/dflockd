package lock

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"time"
)

// Snapshot is a self-contained dump of every replicable LockManager
// field: the FSM fence counter, per-resource holders / waiters /
// enqueued state. Channels are not part of the snapshot (they cannot be
// transferred between processes). Restore reconstructs the per-conn
// indices from the holder / waiter / enqueued data.

const (
	snapshotMagic = "dfllksn1"
	snapshotVer   = byte(1)
	// snapshotMaxStr16 is the largest string the u16-length-prefixed
	// encoding can represent. Keys are bounded far below this by the
	// protocol (a key gets a "lock:"/"sem:" prefix, so up to ~261 bytes);
	// tokens are 32 bytes; refs are short. The cap exists only so a
	// pathological value can't silently truncate via uint16(len(s)).
	snapshotMaxStr16 = 1<<16 - 1
)

var sbe = binary.BigEndian

// Snapshot serializes a point-in-time FSM view. The shards are locked
// one at a time (locks are held only during each shard's emit), so the
// snapshot is per-shard consistent (which is what the algorithm
// requires, since locks are per-key).
func (lm *LockManager) Snapshot(w io.Writer) error {
	if _, err := w.Write([]byte(snapshotMagic)); err != nil {
		return fmt.Errorf("snapshot magic: %w", err)
	}
	if _, err := w.Write([]byte{snapshotVer}); err != nil {
		return fmt.Errorf("snapshot ver: %w", err)
	}
	if err := writeU64(w, lm.fsmFenceCounter); err != nil {
		return err
	}
	return lm.snapshotShards(w)
}

// snapshotShards emits a flat resource list (in deterministic key order
// across all shards) followed by a flat enqueued-index list. Restore
// places resources back into the correct shard via shardFor().
func (lm *LockManager) snapshotShards(w io.Writer) error {
	resources, enqueued := lm.collectSnapshotData()
	if err := writeU32(w, uint32(len(resources))); err != nil {
		return err
	}
	for _, r := range resources {
		if err := writeResource(w, r.key, r.st); err != nil {
			return err
		}
	}
	if err := writeU32(w, uint32(len(enqueued))); err != nil {
		return err
	}
	for _, e := range enqueued {
		if err := writeOneEnqueued(w, e.ck, e.es); err != nil {
			return err
		}
	}
	return nil
}

type resourceEntry struct {
	key string
	st  *ResourceState
}

type enqueuedEntry struct {
	ck connKey
	es *enqueuedState
}

// collectSnapshotData locks each shard in turn, gathers a snapshot of
// resources and enqueued entries, and returns them in deterministic
// order (resources by key; enqueued by (connID, key)).
func (lm *LockManager) collectSnapshotData() ([]resourceEntry, []enqueuedEntry) {
	var resources []resourceEntry
	var enqueued []enqueuedEntry
	for i := range lm.shards {
		resources, enqueued = lm.collectShard(&lm.shards[i], resources, enqueued)
	}
	sort.Slice(resources, func(i, j int) bool { return resources[i].key < resources[j].key })
	sort.Slice(enqueued, func(i, j int) bool {
		if enqueued[i].ck.ConnID != enqueued[j].ck.ConnID {
			return enqueued[i].ck.ConnID < enqueued[j].ck.ConnID
		}
		return enqueued[i].ck.Key < enqueued[j].ck.Key
	})
	return resources, enqueued
}

func (lm *LockManager) collectShard(sh *shard, res []resourceEntry, enq []enqueuedEntry) ([]resourceEntry, []enqueuedEntry) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for k, st := range sh.resources {
		res = append(res, resourceEntry{key: k, st: st})
	}
	for ck, es := range sh.connEnqueued {
		enq = append(enq, enqueuedEntry{ck: ck, es: es})
	}
	return res, enq
}

func writeResource(w io.Writer, key string, st *ResourceState) error {
	if err := writeString16(w, key); err != nil {
		return err
	}
	if err := writeU32(w, uint32(st.Limit)); err != nil {
		return err
	}
	if err := writeI64(w, unixNanosOf(st.LastActivity)); err != nil {
		return err
	}
	if err := writeHolders(w, st.Holders); err != nil {
		return err
	}
	return writeWaiters(w, st.Waiters[st.WaiterHead:])
}

func writeHolders(w io.Writer, holders map[string]*holder) error {
	if err := writeU32(w, uint32(len(holders))); err != nil {
		return err
	}
	tokens := make([]string, 0, len(holders))
	for t := range holders {
		tokens = append(tokens, t)
	}
	sort.Strings(tokens)
	for _, token := range tokens {
		if err := writeOneHolder(w, token, holders[token]); err != nil {
			return err
		}
	}
	return nil
}

func writeOneHolder(w io.Writer, token string, h *holder) error {
	if err := writeString16(w, token); err != nil {
		return err
	}
	if err := writeString16(w, h.ref); err != nil {
		return err
	}
	if err := writeU64(w, h.connID); err != nil {
		return err
	}
	return writeI64(w, unixNanosOf(h.leaseExpires))
}

func writeWaiters(w io.Writer, waiters []*waiter) error {
	if err := writeU32(w, uint32(len(waiters))); err != nil {
		return err
	}
	for _, wt := range waiters {
		if err := writeWaiter(w, wt); err != nil {
			return err
		}
	}
	return nil
}

func writeWaiter(w io.Writer, wt *waiter) error {
	if err := writeString16(w, wt.ref); err != nil {
		return err
	}
	if _, err := w.Write(wt.salt[:]); err != nil {
		return err
	}
	if err := writeU64(w, wt.connID); err != nil {
		return err
	}
	return writeI64(w, int64(wt.leaseTTL))
}

func writeOneEnqueued(w io.Writer, ck connKey, es *enqueuedState) error {
	if err := writeU64(w, ck.ConnID); err != nil {
		return err
	}
	if err := writeString16(w, ck.Key); err != nil {
		return err
	}
	if err := writeString16(w, es.token); err != nil {
		return err
	}
	return writeI64(w, int64(es.leaseTTL))
}

// ---------------------------------------------------------------------------
// Restore
// ---------------------------------------------------------------------------

// Restore replaces the entire FSM with the contents of r. Existing state
// is discarded. Restore MUST be called with no in-flight Apply calls;
// the cluster layer arranges that via its apply pipeline.
func (lm *LockManager) Restore(r io.Reader) error {
	if err := readSnapshotHeader(r); err != nil {
		return err
	}
	fc, err := readU64(r)
	if err != nil {
		return fmt.Errorf("snapshot fence counter: %w", err)
	}
	lm.clearAllShards()
	lm.fsmFenceCounter = fc
	return lm.restoreShards(r)
}

func readSnapshotHeader(r io.Reader) error {
	magic := make([]byte, len(snapshotMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return fmt.Errorf("snapshot magic: %w", err)
	}
	if string(magic) != snapshotMagic {
		return fmt.Errorf("snapshot: bad magic %q", magic)
	}
	ver := make([]byte, 1)
	if _, err := io.ReadFull(r, ver); err != nil {
		return fmt.Errorf("snapshot version: %w", err)
	}
	if ver[0] != snapshotVer {
		return fmt.Errorf("snapshot: unsupported version %d", ver[0])
	}
	return nil
}

// clearAllShards wipes per-shard state ahead of Restore. resourceTotal
// is reset; Restore will re-bump it as it reads.
func (lm *LockManager) clearAllShards() {
	for i := range lm.shards {
		sh := &lm.shards[i]
		sh.mu.Lock()
		sh.resources = map[string]*ResourceState{}
		sh.connOwned = map[uint64]map[string]map[string]struct{}{}
		sh.connEnqueued = map[connKey]*enqueuedState{}
		sh.connEnqueuedByID = map[uint64]map[string]struct{}{}
		sh.mu.Unlock()
	}
	lm.resourceTotal.Store(0)
}

func (lm *LockManager) restoreShards(r io.Reader) error {
	nRes, err := readU32(r)
	if err != nil {
		return fmt.Errorf("snapshot total resources: %w", err)
	}
	for i := uint32(0); i < nRes; i++ {
		if err := lm.restoreOneResource(r); err != nil {
			return err
		}
	}
	return lm.restoreEnqueuedIndex(r)
}

func (lm *LockManager) restoreOneResource(r io.Reader) error {
	key, err := readString16(r)
	if err != nil {
		return err
	}
	limit, err := readU32(r)
	if err != nil {
		return err
	}
	lastActivity, err := readI64(r)
	if err != nil {
		return err
	}
	st := &ResourceState{Limit: int(limit), Holders: map[string]*holder{}, LastActivity: timeFromNanos(lastActivity)}
	return lm.restoreResourceBody(r, key, st)
}

func (lm *LockManager) restoreResourceBody(r io.Reader, key string, st *ResourceState) error {
	if err := readHolders(r, st); err != nil {
		return err
	}
	if err := readWaiters(r, st); err != nil {
		return err
	}
	sh := lm.shardFor(key)
	sh.mu.Lock()
	sh.resources[key] = st
	rebuildOwnedIndex(sh, key, st)
	sh.mu.Unlock()
	lm.resourceTotal.Add(1)
	return nil
}

func readHolders(r io.Reader, st *ResourceState) error {
	n, err := readU32(r)
	if err != nil {
		return err
	}
	for i := uint32(0); i < n; i++ {
		if err := readOneHolder(r, st); err != nil {
			return err
		}
	}
	return nil
}

func readOneHolder(r io.Reader, st *ResourceState) error {
	token, err := readString16(r)
	if err != nil {
		return err
	}
	ref, err := readString16(r)
	if err != nil {
		return err
	}
	connID, err := readU64(r)
	if err != nil {
		return err
	}
	leaseExpiresNanos, err := readI64(r)
	if err != nil {
		return err
	}
	st.Holders[token] = &holder{connID: connID, leaseExpires: timeFromNanos(leaseExpiresNanos), ref: ref}
	return nil
}

func readWaiters(r io.Reader, st *ResourceState) error {
	n, err := readU32(r)
	if err != nil {
		return err
	}
	for i := uint32(0); i < n; i++ {
		w, err := readOneWaiter(r)
		if err != nil {
			return err
		}
		st.Waiters = append(st.Waiters, w)
	}
	return nil
}

func readOneWaiter(r io.Reader) (*waiter, error) {
	ref, err := readString16(r)
	if err != nil {
		return nil, err
	}
	var salt [8]byte
	if _, err := io.ReadFull(r, salt[:]); err != nil {
		return nil, fmt.Errorf("waiter salt: %w", err)
	}
	connID, err := readU64(r)
	if err != nil {
		return nil, err
	}
	leaseTTLNanos, err := readI64(r)
	if err != nil {
		return nil, err
	}
	return &waiter{ref: ref, connID: connID, leaseTTL: time.Duration(leaseTTLNanos), salt: salt}, nil
}

func rebuildOwnedIndex(sh *shard, key string, st *ResourceState) {
	for token, h := range st.Holders {
		sh.addOwned(h.connID, key, token)
	}
}

func (lm *LockManager) restoreEnqueuedIndex(r io.Reader) error {
	n, err := readU32(r)
	if err != nil {
		return err
	}
	for i := uint32(0); i < n; i++ {
		if err := lm.restoreOneEnqueued(r); err != nil {
			return err
		}
	}
	return nil
}

func (lm *LockManager) restoreOneEnqueued(r io.Reader) error {
	connID, err := readU64(r)
	if err != nil {
		return err
	}
	key, err := readString16(r)
	if err != nil {
		return err
	}
	token, err := readString16(r)
	if err != nil {
		return err
	}
	leaseTTLNanos, err := readI64(r)
	if err != nil {
		return err
	}
	return lm.installEnqueuedEntry(connID, key, token, time.Duration(leaseTTLNanos))
}

func (lm *LockManager) installEnqueuedEntry(connID uint64, key, token string, leaseTTL time.Duration) error {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st := sh.resources[key]
	es := &enqueuedState{token: token, leaseTTL: leaseTTL}
	if token == "" {
		es.waiter = findWaiterFor(st, connID)
	}
	sh.setEnqueued(connKey{ConnID: connID, Key: key}, es)
	return nil
}

func findWaiterFor(st *ResourceState, connID uint64) *waiter {
	if st == nil {
		return nil
	}
	for i := st.WaiterHead; i < len(st.Waiters); i++ {
		if st.Waiters[i].connID == connID {
			return st.Waiters[i]
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// codec primitives (snapshot-local; raft has its own equivalents)
// ---------------------------------------------------------------------------

func writeU32(w io.Writer, v uint32) error {
	var b [4]byte
	sbe.PutUint32(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeU64(w io.Writer, v uint64) error {
	var b [8]byte
	sbe.PutUint64(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeI64(w io.Writer, v int64) error { return writeU64(w, uint64(v)) }

func writeString16(w io.Writer, s string) error {
	if len(s) > snapshotMaxStr16 {
		return fmt.Errorf("snapshot: string too long (%d > %d)", len(s), snapshotMaxStr16)
	}
	var b [2]byte
	sbe.PutUint16(b[:], uint16(len(s)))
	if _, err := w.Write(b[:]); err != nil {
		return err
	}
	_, err := w.Write([]byte(s))
	return err
}

func readU32(r io.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return sbe.Uint32(b[:]), nil
}

func readU64(r io.Reader) (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return sbe.Uint64(b[:]), nil
}

func readI64(r io.Reader) (int64, error) {
	v, err := readU64(r)
	return int64(v), err
}

func readString16(r io.Reader) (string, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return "", err
	}
	n := int(sbe.Uint16(lenBuf[:]))
	if n == 0 {
		return "", nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func unixNanosOf(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func timeFromNanos(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}
