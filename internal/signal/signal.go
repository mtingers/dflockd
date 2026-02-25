package signal

import (
	"fmt"
	"strings"
	"sync"
)

// Listener represents a connection listening for signals on a pattern.
type Listener struct {
	ConnID  uint64
	Pattern string
	WriteCh chan []byte
}

type listenerEntry struct {
	pattern string
	isWild  bool
}

// ChannelInfo holds stats about a signal channel/pattern.
type ChannelInfo struct {
	Pattern   string `json:"pattern"`
	Listeners int    `json:"listeners"`
}

// Manager manages signal subscriptions and delivery.
type Manager struct {
	mu            sync.RWMutex
	exact         map[string]map[uint64]*Listener  // channel → connID → listener
	wildcards     []*Listener                       // checked on every signal
	connListeners map[uint64][]*listenerEntry       // connID → list of registrations
}

// NewManager creates a new signal manager.
func NewManager() *Manager {
	return &Manager{
		exact:         make(map[string]map[uint64]*Listener),
		connListeners: make(map[uint64][]*listenerEntry),
	}
}

// isWildPattern returns true if the pattern contains wildcard tokens.
func isWildPattern(pattern string) bool {
	return strings.Contains(pattern, "*") || strings.Contains(pattern, ">")
}

// MatchPattern checks if a literal channel name matches a pattern.
func MatchPattern(pattern, channel string) bool {
	patTokens := strings.Split(pattern, ".")
	chanTokens := strings.Split(channel, ".")
	for i, pt := range patTokens {
		if pt == ">" {
			return i < len(chanTokens) // ">" matches 1+ remaining tokens
		}
		if i >= len(chanTokens) {
			return false
		}
		if pt != "*" && pt != chanTokens[i] {
			return false
		}
	}
	return len(patTokens) == len(chanTokens)
}

// Listen registers a listener for a pattern.
// Duplicate subscriptions (same connID + pattern) are ignored.
func (m *Manager) Listen(pattern string, listener *Listener) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check for duplicate subscription
	for _, e := range m.connListeners[listener.ConnID] {
		if e.pattern == pattern {
			return // already subscribed
		}
	}

	wild := isWildPattern(pattern)
	if wild {
		m.wildcards = append(m.wildcards, listener)
	} else {
		subs, ok := m.exact[pattern]
		if !ok {
			subs = make(map[uint64]*Listener)
			m.exact[pattern] = subs
		}
		subs[listener.ConnID] = listener
	}

	m.connListeners[listener.ConnID] = append(m.connListeners[listener.ConnID], &listenerEntry{
		pattern: pattern,
		isWild:  wild,
	})
}

// Unlisten removes a listener for a specific pattern and connID.
func (m *Manager) Unlisten(pattern string, connID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	wild := isWildPattern(pattern)
	if wild {
		for i, l := range m.wildcards {
			if l.ConnID == connID && l.Pattern == pattern {
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

	// Update reverse index
	entries := m.connListeners[connID]
	for i, e := range entries {
		if e.pattern == pattern {
			entries = append(entries[:i], entries[i+1:]...)
			break
		}
	}
	if len(entries) == 0 {
		delete(m.connListeners, connID)
	} else {
		m.connListeners[connID] = entries
	}
}

// Signal sends a payload to all listeners matching the literal channel.
// Returns the number of unique connections that received the signal.
func (m *Manager) Signal(channel, payload string) int {
	msg := []byte(fmt.Sprintf("sig %s %s\n", channel, payload))

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Deduplicate: a connection listening on both exact and wildcard
	// should only receive the signal once.
	delivered := make(map[uint64]struct{})

	// Exact match
	if subs, ok := m.exact[channel]; ok {
		for connID, l := range subs {
			select {
			case l.WriteCh <- msg:
			default:
				// Buffer full — fire-and-forget drop
			}
			delivered[connID] = struct{}{}
		}
	}

	// Wildcard match
	for _, l := range m.wildcards {
		if _, already := delivered[l.ConnID]; already {
			continue
		}
		if MatchPattern(l.Pattern, channel) {
			select {
			case l.WriteCh <- msg:
			default:
			}
			delivered[l.ConnID] = struct{}{}
		}
	}

	return len(delivered)
}

// UnlistenAll removes all listeners for a given connection.
func (m *Manager) UnlistenAll(connID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entries := m.connListeners[connID]
	for _, e := range entries {
		if e.isWild {
			for i, l := range m.wildcards {
				if l.ConnID == connID && l.Pattern == e.pattern {
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
	delete(m.connListeners, connID)
}

// Stats returns information about active signal channels.
func (m *Manager) Stats() []ChannelInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := []ChannelInfo{}

	// Count exact listeners per channel
	for ch, subs := range m.exact {
		result = append(result, ChannelInfo{
			Pattern:   ch,
			Listeners: len(subs),
		})
	}

	// Count wildcard listeners per pattern
	wildCounts := make(map[string]int)
	for _, l := range m.wildcards {
		wildCounts[l.Pattern]++
	}
	for pat, count := range wildCounts {
		result = append(result, ChannelInfo{
			Pattern:   pat,
			Listeners: count,
		})
	}

	return result
}
