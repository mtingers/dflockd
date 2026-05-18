package lock

// This file contains tiny accessor helpers used only from tests in
// other files in this package. They expose internal state in a
// minimal way so a test can express "has the FSM moved into the
// orphan-state I expect?" without us having to export the state types
// themselves.
//
// Tests in this package that don't need cross-file access can keep
// reaching into unexported state directly; these helpers exist so
// future tests in subpackages (and the orphan test file) stay
// dependency-free.

// HasOrphanedWaiterForTest reports whether the resource at key has at
// least one waiter with the given ref that is currently marked
// abandoned (i.e., abandonedAtNanos != 0).
func (lm *LockManager) HasOrphanedWaiterForTest(key, ref string) bool {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st := sh.resources[key]
	if st == nil {
		return false
	}
	for i := st.WaiterHead; i < len(st.Waiters); i++ {
		w := st.Waiters[i]
		if w != nil && w.ref == ref && w.abandonedAtNanos != 0 {
			return true
		}
	}
	return false
}

// HasActiveWaiterForTest reports whether the resource has a non-
// abandoned waiter with the given (ref, connID). Used to verify
// re-adopt updated connID and cleared abandonedAtNanos.
func (lm *LockManager) HasActiveWaiterForTest(key, ref string, connID uint64) bool {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st := sh.resources[key]
	if st == nil {
		return false
	}
	for i := st.WaiterHead; i < len(st.Waiters); i++ {
		w := st.Waiters[i]
		if w != nil && w.ref == ref && w.connID == connID && w.abandonedAtNanos == 0 {
			return true
		}
	}
	return false
}

// CountWaitersForTest returns the number of active (non-tombstone)
// waiters on key.
func (lm *LockManager) CountWaitersForTest(key string) int {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st := sh.resources[key]
	if st == nil {
		return 0
	}
	return st.waiterCount()
}

// WaiterSaltForTest returns the salt of the waiter with the given ref
// on key, or the zero salt if no matching waiter exists. Used to
// distinguish "re-adopted the original waiter" (original salt kept)
// from "created a fresh one" (caller's new salt).
func (lm *LockManager) WaiterSaltForTest(key, ref string) [8]byte {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st := sh.resources[key]
	if st == nil {
		return [8]byte{}
	}
	for i := st.WaiterHead; i < len(st.Waiters); i++ {
		if w := st.Waiters[i]; w != nil && w.ref == ref {
			return w.salt
		}
	}
	return [8]byte{}
}
