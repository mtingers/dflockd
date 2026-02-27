package signal

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// Listener represents a connection listening for signals on a pattern.
type Listener struct {
	ConnID     uint64
	Pattern    string
	Group      string     // empty = non-grouped (individual delivery)
	WriteCh    chan []byte
	CancelConn func()     // called when WriteCh is full (slow consumer)
}

type listenerEntry struct {
	pattern string
	group   string // empty = non-grouped
	isWild  bool
}

// queueGroup tracks members of a named group for a specific pattern.
type queueGroup struct {
	members []*Listener
	counter atomic.Uint64 // round-robin index, safe under RLock
}

type wildGroupEntry struct {
	pattern string
	group   string
	qg      *queueGroup
}

// ChannelInfo holds stats about a signal channel/pattern.
type ChannelInfo struct {
	Pattern   string `json:"pattern"`
	Group     string `json:"group,omitempty"`
	Listeners int    `json:"listeners"`
}

// Manager manages signal subscriptions and delivery.
type Manager struct {
	mu            sync.RWMutex
	exact         map[string]map[uint64]*Listener  // channel → connID → listener (non-grouped)
	wildcards     []*Listener                       // non-grouped wildcards
	connListeners map[uint64][]*listenerEntry       // connID → list of registrations
	exactGroups   map[string]map[string]*queueGroup // channel → groupName → queueGroup
	wildGroups    []*wildGroupEntry                  // wildcard group entries
}

// NewManager creates a new signal manager.
func NewManager() *Manager {
	return &Manager{
		exact:         make(map[string]map[uint64]*Listener),
		connListeners: make(map[uint64][]*listenerEntry),
		exactGroups:   make(map[string]map[string]*queueGroup),
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

// Listen registers a listener for the pattern specified in listener.Pattern.
// Duplicate subscriptions (same connID + pattern + group) are ignored.
// Returns an error if the pattern is invalid (e.g. ">" not as last token).
func (m *Manager) Listen(listener *Listener) error {
	pattern := listener.Pattern
	if err := validatePattern(pattern); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check for duplicate subscription (same connID + pattern + group)
	for _, e := range m.connListeners[listener.ConnID] {
		if e.pattern == pattern && e.group == listener.Group {
			return nil // already subscribed
		}
	}

	wild := isWildPattern(pattern)
	group := listener.Group

	if group == "" {
		// Non-grouped path (existing behavior)
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
	} else {
		// Grouped path
		if wild {
			// Find existing wildGroupEntry or create new one
			var found *queueGroup
			for _, wge := range m.wildGroups {
				if wge.pattern == pattern && wge.group == group {
					found = wge.qg
					break
				}
			}
			if found == nil {
				found = &queueGroup{}
				m.wildGroups = append(m.wildGroups, &wildGroupEntry{
					pattern: pattern,
					group:   group,
					qg:      found,
				})
			}
			found.members = append(found.members, listener)
		} else {
			groups, ok := m.exactGroups[pattern]
			if !ok {
				groups = make(map[string]*queueGroup)
				m.exactGroups[pattern] = groups
			}
			qg, ok := groups[group]
			if !ok {
				qg = &queueGroup{}
				groups[group] = qg
			}
			qg.members = append(qg.members, listener)
		}
	}

	m.connListeners[listener.ConnID] = append(m.connListeners[listener.ConnID], &listenerEntry{
		pattern: pattern,
		group:   group,
		isWild:  wild,
	})
	return nil
}

// Unlisten removes a listener for a specific pattern, connID, and group.
func (m *Manager) Unlisten(pattern string, connID uint64, group string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	wild := isWildPattern(pattern)

	if group == "" {
		// Non-grouped removal
		if wild {
			for i, l := range m.wildcards {
				if l.ConnID == connID && l.Pattern == pattern {
					copy(m.wildcards[i:], m.wildcards[i+1:])
					m.wildcards[len(m.wildcards)-1] = nil
					m.wildcards = m.wildcards[:len(m.wildcards)-1]
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
	} else {
		// Grouped removal
		if wild {
			for wi, wge := range m.wildGroups {
				if wge.pattern == pattern && wge.group == group {
					for mi, mem := range wge.qg.members {
						if mem.ConnID == connID {
							copy(wge.qg.members[mi:], wge.qg.members[mi+1:])
							wge.qg.members[len(wge.qg.members)-1] = nil
							wge.qg.members = wge.qg.members[:len(wge.qg.members)-1]
							break
						}
					}
					if len(wge.qg.members) == 0 {
						copy(m.wildGroups[wi:], m.wildGroups[wi+1:])
						m.wildGroups[len(m.wildGroups)-1] = nil
						m.wildGroups = m.wildGroups[:len(m.wildGroups)-1]
					}
					break
				}
			}
		} else {
			if groups, ok := m.exactGroups[pattern]; ok {
				if qg, ok := groups[group]; ok {
					for mi, mem := range qg.members {
						if mem.ConnID == connID {
							copy(qg.members[mi:], qg.members[mi+1:])
							qg.members[len(qg.members)-1] = nil
							qg.members = qg.members[:len(qg.members)-1]
							break
						}
					}
					if len(qg.members) == 0 {
						delete(groups, group)
						if len(groups) == 0 {
							delete(m.exactGroups, pattern)
						}
					}
				}
			}
		}
	}

	// Update reverse index
	entries := m.connListeners[connID]
	for i, e := range entries {
		if e.pattern == pattern && e.group == group {
			copy(entries[i:], entries[i+1:])
			entries[len(entries)-1] = nil
			entries = entries[:len(entries)-1]
			break
		}
	}
	if len(entries) == 0 {
		delete(m.connListeners, connID)
	} else {
		m.connListeners[connID] = entries
	}
}

// deliverToGroup delivers a message to one member of a queue group via round-robin.
// Returns true if a delivery was made.
// Falls back to the next member if the selected member's buffer is full.
func deliverToGroup(qg *queueGroup, msg []byte) bool {
	n := len(qg.members)
	if n == 0 {
		return false
	}
	start := qg.counter.Add(1) - 1
	primary := int(start % uint64(n))
	for i := 0; i < n; i++ {
		idx := int((start + uint64(i)) % uint64(n))
		mem := qg.members[idx]
		select {
		case mem.WriteCh <- msg:
			return true
		default:
			// Buffer full — try next member
		}
	}
	// All members full — disconnect the primary to prevent permanent black hole.
	if mem := qg.members[primary]; mem.CancelConn != nil {
		mem.CancelConn()
	}
	return false
}

// Signal sends a payload to all listeners matching the literal channel.
// Returns the number of successful deliveries (including independent group
// deliveries, so a connection in both a non-grouped and grouped subscription
// counts as 2).
func (m *Manager) Signal(channel, payload string) int {
	// msg is shared read-only across all recipients. Each recipient's push
	// writer only passes it to conn.Write (read-only). Do not mutate msg
	// after creation.
	msg := []byte(fmt.Sprintf("sig %s %s\n", channel, payload))

	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0

	// Deduplicate non-grouped: a connection listening on both exact and wildcard
	// should only receive the signal once via non-grouped paths.
	// Groups are independent and do not participate in dedup.
	delivered := make(map[uint64]struct{})

	// 1. Exact non-grouped
	if subs, ok := m.exact[channel]; ok {
		for connID, l := range subs {
			select {
			case l.WriteCh <- msg:
				count++
				delivered[connID] = struct{}{}
			default:
				if l.CancelConn != nil {
					l.CancelConn()
				}
				delivered[connID] = struct{}{}
			}
		}
	}

	// 2. Exact grouped — each group independently delivers to one member
	if groups, ok := m.exactGroups[channel]; ok {
		for _, qg := range groups {
			if deliverToGroup(qg, msg) {
				count++
			}
		}
	}

	// 3. Wildcard non-grouped
	for _, l := range m.wildcards {
		if _, already := delivered[l.ConnID]; already {
			continue
		}
		if MatchPattern(l.Pattern, channel) {
			select {
			case l.WriteCh <- msg:
				count++
				delivered[l.ConnID] = struct{}{}
			default:
				if l.CancelConn != nil {
					l.CancelConn()
				}
				delivered[l.ConnID] = struct{}{}
			}
		}
	}

	// 4. Wildcard grouped — each group independently delivers to one member
	for _, wge := range m.wildGroups {
		if MatchPattern(wge.pattern, channel) {
			if deliverToGroup(wge.qg, msg) {
				count++
			}
		}
	}

	return count
}

// UnlistenAll removes all listeners for a given connection.
func (m *Manager) UnlistenAll(connID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entries := m.connListeners[connID]
	for _, e := range entries {
		if e.group == "" {
			// Non-grouped
			if e.isWild {
				for i, l := range m.wildcards {
					if l.ConnID == connID && l.Pattern == e.pattern {
						copy(m.wildcards[i:], m.wildcards[i+1:])
						m.wildcards[len(m.wildcards)-1] = nil
						m.wildcards = m.wildcards[:len(m.wildcards)-1]
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
		} else {
			// Grouped
			if e.isWild {
				// Iterate backwards so that removing an empty entry does
				// not cause the loop to skip the next element.
				for wi := len(m.wildGroups) - 1; wi >= 0; wi-- {
					wge := m.wildGroups[wi]
					if wge.pattern == e.pattern && wge.group == e.group {
						for mi, mem := range wge.qg.members {
							if mem.ConnID == connID {
								copy(wge.qg.members[mi:], wge.qg.members[mi+1:])
								wge.qg.members[len(wge.qg.members)-1] = nil
								wge.qg.members = wge.qg.members[:len(wge.qg.members)-1]
								break
							}
						}
						if len(wge.qg.members) == 0 {
							copy(m.wildGroups[wi:], m.wildGroups[wi+1:])
							m.wildGroups[len(m.wildGroups)-1] = nil
							m.wildGroups = m.wildGroups[:len(m.wildGroups)-1]
						}
						break
					}
				}
			} else {
				if groups, ok := m.exactGroups[e.pattern]; ok {
					if qg, ok := groups[e.group]; ok {
						for mi, mem := range qg.members {
							if mem.ConnID == connID {
								copy(qg.members[mi:], qg.members[mi+1:])
								qg.members[len(qg.members)-1] = nil
								qg.members = qg.members[:len(qg.members)-1]
								break
							}
						}
						if len(qg.members) == 0 {
							delete(groups, e.group)
							if len(groups) == 0 {
								delete(m.exactGroups, e.pattern)
							}
						}
					}
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

	// Non-grouped exact listeners per channel
	for ch, subs := range m.exact {
		result = append(result, ChannelInfo{
			Pattern:   ch,
			Listeners: len(subs),
		})
	}

	// Non-grouped wildcard listeners per pattern
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

	// Grouped exact listeners
	for ch, groups := range m.exactGroups {
		for groupName, qg := range groups {
			result = append(result, ChannelInfo{
				Pattern:   ch,
				Group:     groupName,
				Listeners: len(qg.members),
			})
		}
	}

	// Grouped wildcard listeners
	for _, wge := range m.wildGroups {
		result = append(result, ChannelInfo{
			Pattern:   wge.pattern,
			Group:     wge.group,
			Listeners: len(wge.qg.members),
		})
	}

	return result
}
