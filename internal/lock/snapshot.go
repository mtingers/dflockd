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
	// snapshotVer is the writer's version. Version 2 added abandonedAtNanos
	// to holders and waiters. Version 3 adds the waiter salt to each queued
	// two-phase index entry so Restore can identify its exact waiter.
	//
	// Readers must gate each optional field on the version that INTRODUCED it
	// (snapshotVer2, snapshotVer3, …), never on snapshotVer — bumping the
	// writer would otherwise silently stop reading every field of the prior
	// format and desynchronise the byte stream.
	snapshotVer  = snapshotVer3
	snapshotVer1 = byte(1)
	snapshotVer2 = byte(2)
	snapshotVer3 = byte(3)
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
		if err := writeOneEnqueued(w, e.ck, e.es, e.waiterSalt); err != nil {
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
	ck         connKey
	es         *enqueuedState
	waiterSalt [8]byte
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
		res = append(res, resourceEntry{key: k, st: cloneResourceForSnapshot(st)})
	}
	for ck, es := range sh.connEnqueued {
		copy := *es
		entry := enqueuedEntry{ck: ck, es: &copy}
		if es.waiter != nil {
			entry.waiterSalt = es.waiter.salt
		}
		copy.waiter = nil // the snapshot index persists only token + lease TTL
		enq = append(enq, entry)
	}
	return res, enq
}

func cloneResourceForSnapshot(st *ResourceState) *ResourceState {
	copy := &ResourceState{
		Limit:        st.Limit,
		Holders:      make(map[string]*holder, len(st.Holders)),
		LastActivity: st.LastActivity,
	}
	for token, live := range st.Holders {
		holderCopy := *live
		copy.Holders[token] = &holderCopy
	}
	copy.Waiters = make([]*waiter, 0, st.waiterCount())
	for i := st.WaiterHead; i < len(st.Waiters); i++ {
		if st.Waiters[i] == nil {
			continue
		}
		waiterCopy := *st.Waiters[i]
		waiterCopy.ch = nil
		copy.Waiters = append(copy.Waiters, &waiterCopy)
	}
	return copy
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
	if err := writeI64(w, unixNanosOf(h.leaseExpires)); err != nil {
		return err
	}
	return writeI64(w, h.abandonedAtNanos) // snapshotVer 2+
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
	if err := writeI64(w, int64(wt.leaseTTL)); err != nil {
		return err
	}
	return writeI64(w, wt.abandonedAtNanos) // snapshotVer 2+
}

func writeOneEnqueued(w io.Writer, ck connKey, es *enqueuedState, waiterSalt [8]byte) error {
	if err := writeU64(w, ck.ConnID); err != nil {
		return err
	}
	if err := writeString16(w, ck.Key); err != nil {
		return err
	}
	if err := writeString16(w, es.token); err != nil {
		return err
	}
	if err := writeI64(w, int64(es.leaseTTL)); err != nil {
		return err
	}
	_, err := w.Write(waiterSalt[:])
	return err
}

// ---------------------------------------------------------------------------
// Restore
// ---------------------------------------------------------------------------

// Restore replaces the entire FSM with the contents of r. Existing state
// is discarded. Restore MUST be called with no in-flight Apply calls;
// the cluster layer arranges that via its apply pipeline.
func (lm *LockManager) Restore(r io.Reader) error {
	ver, err := readSnapshotHeader(r)
	if err != nil {
		return err
	}
	fc, err := readU64(r)
	if err != nil {
		return fmt.Errorf("snapshot fence counter: %w", err)
	}
	restored := newRestoreTarget(fc)
	if err := restored.restoreShards(r, ver); err != nil {
		return err
	}
	trailing, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("snapshot trailing data: %w", err)
	}
	if len(trailing) != 0 {
		return fmt.Errorf("snapshot: %d trailing bytes", len(trailing))
	}
	lm.installRestoredState(restored)
	return nil
}

func readSnapshotHeader(r io.Reader) (byte, error) {
	magic := make([]byte, len(snapshotMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return 0, fmt.Errorf("snapshot magic: %w", err)
	}
	if string(magic) != snapshotMagic {
		return 0, fmt.Errorf("snapshot: bad magic %q", magic)
	}
	ver := make([]byte, 1)
	if _, err := io.ReadFull(r, ver); err != nil {
		return 0, fmt.Errorf("snapshot version: %w", err)
	}
	if ver[0] != snapshotVer && ver[0] != snapshotVer2 && ver[0] != snapshotVer1 {
		return 0, fmt.Errorf("snapshot: unsupported version %d", ver[0])
	}
	return ver[0], nil
}

func newRestoreTarget(fenceCounter uint64) *LockManager {
	restored := &LockManager{fsmFenceCounter: fenceCounter}
	for i := range restored.shards {
		restored.shards[i].init()
	}
	return restored
}

// installRestoredState publishes only a fully decoded snapshot. The Restore
// contract excludes concurrent Apply calls, but shard locks also keep
// diagnostic readers from observing partially replaced maps.
func (lm *LockManager) installRestoredState(restored *LockManager) {
	for i := range lm.shards {
		dst := &lm.shards[i]
		src := &restored.shards[i]
		dst.mu.Lock()
		dst.resources = src.resources
		dst.connOwned = src.connOwned
		dst.connEnqueued = src.connEnqueued
		dst.connEnqueuedByID = src.connEnqueuedByID
		dst.mu.Unlock()
	}
	lm.resourceTotal.Store(restored.resourceTotal.Load())
	lm.fsmFenceCounter = restored.fsmFenceCounter
}

func (lm *LockManager) restoreShards(r io.Reader, ver byte) error {
	nRes, err := readU32(r)
	if err != nil {
		return fmt.Errorf("snapshot total resources: %w", err)
	}
	for i := uint32(0); i < nRes; i++ {
		if err := lm.restoreOneResource(r, ver); err != nil {
			return err
		}
	}
	return lm.restoreEnqueuedIndex(r, ver)
}

func (lm *LockManager) restoreOneResource(r io.Reader, ver byte) error {
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
	st := lm.newResourceState(int(limit), timeFromNanos(lastActivity))
	return lm.restoreResourceBody(r, key, st, ver)
}

func (lm *LockManager) restoreResourceBody(r io.Reader, key string, st *ResourceState, ver byte) error {
	if err := readHolders(r, st, ver); err != nil {
		return err
	}
	if err := readWaiters(r, st, ver); err != nil {
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

func readHolders(r io.Reader, st *ResourceState, ver byte) error {
	n, err := readU32(r)
	if err != nil {
		return err
	}
	for i := uint32(0); i < n; i++ {
		if err := readOneHolder(r, st, ver); err != nil {
			return err
		}
	}
	return nil
}

func readOneHolder(r io.Reader, st *ResourceState, ver byte) error {
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
	var abandoned int64
	if ver >= snapshotVer2 {
		if abandoned, err = readI64(r); err != nil {
			return err
		}
	}
	st.addHolder(token, &holder{connID: connID, leaseExpires: timeFromNanos(leaseExpiresNanos), ref: ref, abandonedAtNanos: abandoned})
	return nil
}

func readWaiters(r io.Reader, st *ResourceState, ver byte) error {
	n, err := readU32(r)
	if err != nil {
		return err
	}
	for i := uint32(0); i < n; i++ {
		w, err := readOneWaiter(r, ver)
		if err != nil {
			return err
		}
		st.appendWaiter(w)
	}
	return nil
}

func readOneWaiter(r io.Reader, ver byte) (*waiter, error) {
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
	var abandoned int64
	if ver >= snapshotVer2 {
		if abandoned, err = readI64(r); err != nil {
			return nil, err
		}
	}
	return &waiter{ref: ref, connID: connID, leaseTTL: time.Duration(leaseTTLNanos), salt: salt, abandonedAtNanos: abandoned}, nil
}

func rebuildOwnedIndex(sh *shard, key string, st *ResourceState) {
	for token, h := range st.Holders {
		sh.addOwned(h.connID, key, token)
	}
}

func (lm *LockManager) restoreEnqueuedIndex(r io.Reader, ver byte) error {
	n, err := readU32(r)
	if err != nil {
		return err
	}
	for i := uint32(0); i < n; i++ {
		if err := lm.restoreOneEnqueued(r, ver); err != nil {
			return err
		}
	}
	return nil
}

func (lm *LockManager) restoreOneEnqueued(r io.Reader, ver byte) error {
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
	var salt [8]byte
	haveSalt := false
	if ver >= snapshotVer3 {
		if _, err := io.ReadFull(r, salt[:]); err != nil {
			return fmt.Errorf("snapshot enqueued waiter salt: %w", err)
		}
		haveSalt = true
	}
	return lm.installEnqueuedEntry(connID, key, token, time.Duration(leaseTTLNanos), salt, haveSalt)
}

func (lm *LockManager) installEnqueuedEntry(connID uint64, key, token string, leaseTTL time.Duration, salt [8]byte, haveSalt bool) error {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st := sh.resources[key]
	es := &enqueuedState{token: token, leaseTTL: leaseTTL}
	if token == "" {
		es.waiter = findWaiterFor(st, connID, salt, haveSalt)
	}
	sh.setEnqueued(connKey{ConnID: connID, Key: key}, es)
	return nil
}

// findWaiterFor re-links a restored enqueued-index entry to its waiter.
// snapshotVer3+ carries the waiter's salt, which identifies the exact waiter
// when one connection has several queued on the same key; older snapshots can
// only fall back to the first waiter on that connection.
func findWaiterFor(st *ResourceState, connID uint64, salt [8]byte, haveSalt bool) *waiter {
	if st == nil {
		return nil
	}
	for i := st.WaiterHead; i < len(st.Waiters); i++ {
		w := st.Waiters[i]
		if w.connID != connID {
			continue
		}
		if haveSalt && w.salt != salt {
			continue
		}
		return w
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
