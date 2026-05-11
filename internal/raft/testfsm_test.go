package raft

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

// recordingFSM is a test FSM: every Apply records the entry's data so a
// test can later assert what each node saw, in order. It also tracks the
// last applied index, so a test can compare convergence across nodes.
// Snapshot/Restore are implemented (binary-encoded) so the snapshot path
// is exercisable end-to-end.
//
// recordingFSM is concurrent-safe — but per the FSM contract Apply only
// runs on the apply goroutine, so the mutex is mostly defensive in case
// a test reads state from a different goroutine while applies are still
// in flight.
type recordingFSM struct {
	mu      sync.Mutex
	applied [][]byte
}

func newRecordingFSM() *recordingFSM { return &recordingFSM{} }

func (f *recordingFSM) Apply(e Entry) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := append([]byte(nil), e.Data...)
	f.applied = append(f.applied, cp)
	return Index(len(f.applied)) // result = number of Normal entries applied so far
}

func (f *recordingFSM) Snapshot() (FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([][]byte, len(f.applied))
	for i, b := range f.applied {
		cp[i] = append([]byte(nil), b...)
	}
	return &recordingFSMSnapshot{entries: cp}, nil
}

func (f *recordingFSM) Restore(r io.Reader) error {
	entries, err := decodeRecordedSnapshot(r)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = entries
	return nil
}

// applied returns a copy of the recorded data (one slice per Apply).
func (f *recordingFSM) appliedCopy() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]byte, len(f.applied))
	for i, b := range f.applied {
		out[i] = append([]byte(nil), b...)
	}
	return out
}

// count returns the number of Apply calls so far.
func (f *recordingFSM) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.applied)
}

type recordingFSMSnapshot struct{ entries [][]byte }

func (s *recordingFSMSnapshot) Persist(w io.Writer) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(s.entries)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	for _, e := range s.entries {
		binary.BigEndian.PutUint32(hdr[:], uint32(len(e)))
		if _, err := w.Write(hdr[:]); err != nil {
			return err
		}
		if _, err := w.Write(e); err != nil {
			return err
		}
	}
	return nil
}

func (s *recordingFSMSnapshot) Release() {}

func decodeRecordedSnapshot(r io.Reader) ([][]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(hdr[:]))
	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		entry, err := readRecordedEntry(r, hdr[:])
		if err != nil {
			return nil, err
		}
		out = append(out, entry)
	}
	return out, nil
}

func readRecordedEntry(r io.Reader, hdr []byte) ([]byte, error) {
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(hdr))
	if n < 0 {
		return nil, errors.New("recordingFSM: negative entry length")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
