package lock

import "time"

// LockInfo describes one held or queued lock for the stats endpoint.
type LockInfo struct {
	Key             string  `json:"key"`
	OwnerConnID     uint64  `json:"owner_conn_id"`
	LeaseExpiresInS float64 `json:"lease_expires_in_s"`
	Waiters         int     `json:"waiters"`
}

// SemInfo describes one held or queued semaphore for the stats endpoint.
type SemInfo struct {
	Key     string `json:"key"`
	Limit   int    `json:"limit"`
	Holders int    `json:"holders"`
	Waiters int    `json:"waiters"`
}

// IdleInfo describes a resource that has neither holders nor waiters
// and is awaiting GC.
type IdleInfo struct {
	Key   string  `json:"key"`
	IdleS float64 `json:"idle_s"`
}

// Stats is the snapshot returned by LockManager.Stats. The shape is
// stable across the wire and HTTP.
type Stats struct {
	Connections    int64      `json:"connections"`
	Locks          []LockInfo `json:"locks"`
	Semaphores     []SemInfo  `json:"semaphores"`
	IdleLocks      []IdleInfo `json:"idle_locks"`
	IdleSemaphores []IdleInfo `json:"idle_semaphores"`
}

// Stats returns a point-in-time snapshot of all known resources.
func (lm *LockManager) Stats(connections int64) *Stats {
	now := time.Now()
	s := &Stats{
		Connections:    connections,
		Locks:          []LockInfo{},
		Semaphores:     []SemInfo{},
		IdleLocks:      []IdleInfo{},
		IdleSemaphores: []IdleInfo{},
	}
	for i := range lm.shards {
		sh := &lm.shards[i]
		sh.mu.Lock()
		for key, st := range sh.resources {
			s.appendResource(key, st, now)
		}
		sh.mu.Unlock()
	}
	return s
}

func (s *Stats) appendResource(key string, st *ResourceState, now time.Time) {
	if st.Limit == 1 {
		s.appendLockOrIdle(key, st, now)
		return
	}
	s.appendSemOrIdle(key, st, now)
}

// appendLockOrIdle classifies a Limit==1 resource into Locks/IdleLocks.
func (s *Stats) appendLockOrIdle(key string, st *ResourceState, now time.Time) {
	nw := st.waiterCount()
	switch {
	case len(st.Holders) > 0:
		s.Locks = append(s.Locks, lockInfoFromHolders(key, st, now, nw))
	case nw > 0:
		s.Locks = append(s.Locks, LockInfo{Key: key, Waiters: nw})
	default:
		s.IdleLocks = append(s.IdleLocks, idleInfo(key, st, now))
	}
}

// appendSemOrIdle is appendLockOrIdle for Limit>1 resources.
func (s *Stats) appendSemOrIdle(key string, st *ResourceState, now time.Time) {
	nw := st.waiterCount()
	switch {
	case len(st.Holders) > 0:
		s.Semaphores = append(s.Semaphores, SemInfo{Key: key, Limit: st.Limit, Holders: len(st.Holders), Waiters: nw})
	case nw > 0:
		s.Semaphores = append(s.Semaphores, SemInfo{Key: key, Limit: st.Limit, Waiters: nw})
	default:
		s.IdleSemaphores = append(s.IdleSemaphores, idleInfo(key, st, now))
	}
}

// lockInfoFromHolders builds the LockInfo for a held lock. Locks are
// Limit==1 so there's exactly one holder; the for-range visits it once.
func lockInfoFromHolders(key string, st *ResourceState, now time.Time, waiters int) LockInfo {
	var owner uint64
	var expires float64
	for _, h := range st.Holders {
		owner = h.connID
		expires = secondsUntil(h.leaseExpires, now)
	}
	return LockInfo{Key: key, OwnerConnID: owner, LeaseExpiresInS: expires, Waiters: waiters}
}

func secondsUntil(deadline, now time.Time) float64 {
	s := deadline.Sub(now).Seconds()
	if s < 0 {
		return 0
	}
	return s
}

func idleInfo(key string, st *ResourceState, now time.Time) IdleInfo {
	return IdleInfo{Key: key, IdleS: now.Sub(st.LastActivity).Seconds()}
}
