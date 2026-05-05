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
	nw := st.waiterCount()
	if st.Limit == 1 {
		switch {
		case len(st.Holders) > 0:
			var ownerConn uint64
			var expires float64
			for _, h := range st.Holders {
				ownerConn = h.connID
				expires = h.leaseExpires.Sub(now).Seconds()
				if expires < 0 {
					expires = 0
				}
			}
			s.Locks = append(s.Locks, LockInfo{
				Key:             key,
				OwnerConnID:     ownerConn,
				LeaseExpiresInS: expires,
				Waiters:         nw,
			})
		case nw > 0:
			s.Locks = append(s.Locks, LockInfo{Key: key, Waiters: nw})
		default:
			s.IdleLocks = append(s.IdleLocks, IdleInfo{
				Key:   key,
				IdleS: now.Sub(st.LastActivity).Seconds(),
			})
		}
		return
	}
	switch {
	case len(st.Holders) > 0:
		s.Semaphores = append(s.Semaphores, SemInfo{
			Key:     key,
			Limit:   st.Limit,
			Holders: len(st.Holders),
			Waiters: nw,
		})
	case nw > 0:
		s.Semaphores = append(s.Semaphores, SemInfo{
			Key:     key,
			Limit:   st.Limit,
			Waiters: nw,
		})
	default:
		s.IdleSemaphores = append(s.IdleSemaphores, IdleInfo{
			Key:   key,
			IdleS: now.Sub(st.LastActivity).Seconds(),
		})
	}
}
