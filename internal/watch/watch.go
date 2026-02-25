package watch

import (
	"fmt"
	"strings"
	"sync"
)

// Watcher represents a connection watching for key changes.
type Watcher struct {
	ConnID     uint64
	Pattern    string     // exact key or wildcard pattern (*, >)
	WriteCh    chan []byte // reuses connection's push channel
	CancelConn func()     // called when WriteCh is full (slow consumer)
}

type watcherEntry struct {
	pattern string
	isWild  bool
}

// WatchInfo holds stats about a watch registration.
type WatchInfo struct {
	Pattern  string `json:"pattern"`
	Watchers int    `json:"watchers"`
}

// Manager manages watch subscriptions and delivery.
type Manager struct {
	mu           sync.RWMutex
	exact        map[string]map[uint64]*Watcher // key → connID → watcher
	wildcards    []*Watcher
	connWatchers map[uint64][]watcherEntry // reverse index for cleanup
}

// NewManager creates a new watch manager.
func NewManager() *Manager {
	return &Manager{
		exact:        make(map[string]map[uint64]*Watcher),
		connWatchers: make(map[uint64][]watcherEntry),
	}
}

// isWildPattern returns true if the pattern contains wildcard tokens.
func isWildPattern(pattern string) bool {
	return strings.Contains(pattern, "*") || strings.Contains(pattern, ">")
}

// validatePattern checks that ">" only appears as the final token and
// that "*" only appears as a whole token (not as a substring like "c*").
func validatePattern(pattern string) error {
	tokens := strings.Split(pattern, ".")
	for i, t := range tokens {
		if t == ">" && i != len(tokens)-1 {
			return fmt.Errorf("'>' must be the last token in pattern")
		}
		if strings.Contains(t, "*") && t != "*" {
			return fmt.Errorf("'*' must be an entire dot-separated token, got %q", t)
		}
		if strings.Contains(t, ">") && t != ">" {
			return fmt.Errorf("'>' must be an entire dot-separated token, got %q", t)
		}
	}
	return nil
}

// matchPattern checks if a literal key matches a pattern.
func matchPattern(pattern, key string) bool {
	patTokens := strings.Split(pattern, ".")
	keyTokens := strings.Split(key, ".")
	for i, pt := range patTokens {
		if pt == ">" {
			return i < len(keyTokens) // ">" matches 1+ remaining tokens
		}
		if i >= len(keyTokens) {
			return false
		}
		if pt != "*" && pt != keyTokens[i] {
			return false
		}
	}
	return len(patTokens) == len(keyTokens)
}

// Watch registers a watcher. Duplicate (connID, pattern) is ignored.
func (m *Manager) Watch(w *Watcher) error {
	if err := validatePattern(w.Pattern); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check for duplicate
	for _, e := range m.connWatchers[w.ConnID] {
		if e.pattern == w.Pattern {
			return nil
		}
	}

	wild := isWildPattern(w.Pattern)
	if wild {
		m.wildcards = append(m.wildcards, w)
	} else {
		subs, ok := m.exact[w.Pattern]
		if !ok {
			subs = make(map[uint64]*Watcher)
			m.exact[w.Pattern] = subs
		}
		subs[w.ConnID] = w
	}

	m.connWatchers[w.ConnID] = append(m.connWatchers[w.ConnID], watcherEntry{
		pattern: w.Pattern,
		isWild:  wild,
	})
	return nil
}

// Unwatch removes a watcher for a specific pattern and connID.
func (m *Manager) Unwatch(pattern string, connID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	wild := isWildPattern(pattern)
	if wild {
		for i, w := range m.wildcards {
			if w.ConnID == connID && w.Pattern == pattern {
				m.wildcards = append(m.wildcards[:i], m.wildcards[i+1:]...)
				break
			}
		}
	} else {
		if subs, ok := m.exact[pattern]; ok {
			delete(subs, connID)
			if len(subs) == 0 {
				delete(m.exact, pattern)
			}
		}
	}

	entries := m.connWatchers[connID]
	for i, e := range entries {
		if e.pattern == pattern {
			entries = append(entries[:i], entries[i+1:]...)
			break
		}
	}
	if len(entries) == 0 {
		delete(m.connWatchers, connID)
	} else {
		m.connWatchers[connID] = entries
	}
}

// UnwatchAll removes all watchers for a given connection.
func (m *Manager) UnwatchAll(connID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entries := m.connWatchers[connID]
	for _, e := range entries {
		if e.isWild {
			for i, w := range m.wildcards {
				if w.ConnID == connID && w.Pattern == e.pattern {
					m.wildcards = append(m.wildcards[:i], m.wildcards[i+1:]...)
					break
				}
			}
		} else {
			if subs, ok := m.exact[e.pattern]; ok {
				delete(subs, connID)
				if len(subs) == 0 {
					delete(m.exact, e.pattern)
				}
			}
		}
	}
	delete(m.connWatchers, connID)
}

// Notify sends a watch event to all watchers matching the key.
// Slow consumers (full WriteCh) are disconnected via CancelConn.
func (m *Manager) Notify(eventType, key string) {
	msg := []byte(fmt.Sprintf("watch %s %s\n", eventType, key))

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Deduplicate: a connection watching both exact and wildcard
	// should only receive one notification.
	delivered := make(map[uint64]struct{})

	// Exact match
	if subs, ok := m.exact[key]; ok {
		for connID, w := range subs {
			select {
			case w.WriteCh <- msg:
			default:
				if w.CancelConn != nil {
					w.CancelConn()
				}
			}
			delivered[connID] = struct{}{}
		}
	}

	// Wildcard match
	for _, w := range m.wildcards {
		if _, already := delivered[w.ConnID]; already {
			continue
		}
		if matchPattern(w.Pattern, key) {
			select {
			case w.WriteCh <- msg:
			default:
				if w.CancelConn != nil {
					w.CancelConn()
				}
			}
			delivered[w.ConnID] = struct{}{}
		}
	}
}

// Stats returns information about active watches.
func (m *Manager) Stats() []WatchInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := []WatchInfo{}

	// Exact watchers per key
	for key, subs := range m.exact {
		result = append(result, WatchInfo{
			Pattern:  key,
			Watchers: len(subs),
		})
	}

	// Wildcard watchers per pattern
	wildCounts := make(map[string]int)
	for _, w := range m.wildcards {
		wildCounts[w.Pattern]++
	}
	for pat, count := range wildCounts {
		result = append(result, WatchInfo{
			Pattern:  pat,
			Watchers: count,
		})
	}

	return result
}
