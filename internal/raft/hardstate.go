package raft

import (
	"bytes"
	"fmt"
	"os"
)

// HardState is persisted in a fixed-size two-slot journal: each Save
// writes the slot not written last (chosen by sequence-number parity)
// and fsyncs it, so a crash mid-write leaves the previous slot intact.
// On load, the slot with the highest sequence number that has a valid
// magic and CRC wins. (Same shape as internal/lock's fence journal.)

const (
	hardStateSlotBytes = 512
	hardStateFileBytes = hardStateSlotBytes * 2
	hardStateCRCOff    = hardStateSlotBytes - 8
	hardStateFilePerm  = 0o600
)

var hardStateMagic = [8]byte{'d', 'f', 'l', 'r', 'f', 'h', 's', '1'}

type hardStateFile struct {
	path    string
	f       *os.File
	nextSeq uint64
}

// openHardStateFile opens (creating if absent) the journal at path and
// returns it along with the recovered HardState (the zero value if the
// journal is empty or unreadable).
func openHardStateFile(path string) (*hardStateFile, HardState, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, hardStateFilePerm)
	if err != nil {
		return nil, HardState{}, fmt.Errorf("open hardstate %s: %w", path, err)
	}
	if err := ensureSize(f, hardStateFileBytes); err != nil {
		f.Close()
		return nil, HardState{}, err
	}
	return loadHardStateSlots(f, path)
}

func loadHardStateSlots(f *os.File, path string) (*hardStateFile, HardState, error) {
	buf := make([]byte, hardStateFileBytes)
	if _, err := f.ReadAt(buf, 0); err != nil {
		f.Close()
		return nil, HardState{}, fmt.Errorf("read hardstate %s: %w", path, err)
	}
	hs, seq := bestHardStateSlot(buf)
	return &hardStateFile{path: path, f: f, nextSeq: seq + 1}, hs, nil
}

// bestHardStateSlot returns the HardState from the higher-sequence valid
// slot and that sequence number (0 if neither slot is valid).
func bestHardStateSlot(buf []byte) (HardState, uint64) {
	var best HardState
	var bestSeq uint64
	for i := 0; i < 2; i++ {
		slot := buf[i*hardStateSlotBytes : (i+1)*hardStateSlotBytes]
		if hs, seq, ok := decodeHardStateSlot(slot); ok && seq > bestSeq {
			best, bestSeq = hs, seq
		}
	}
	return best, bestSeq
}

func decodeHardStateSlot(slot []byte) (HardState, uint64, bool) {
	if !bytes.Equal(slot[0:8], hardStateMagic[:]) {
		return HardState{}, 0, false
	}
	if crc(slot[:hardStateCRCOff]) != be.Uint64(slot[hardStateCRCOff:]) {
		return HardState{}, 0, false
	}
	return parseHardStateBody(slot)
}

func parseHardStateBody(slot []byte) (HardState, uint64, bool) {
	seq := be.Uint64(slot[8:16])
	hs := HardState{CurrentTerm: Term(be.Uint64(slot[16:24])), CommitIndex: Index(be.Uint64(slot[24:32]))}
	votedLen := int(be.Uint16(slot[32:34]))
	if 34+votedLen > hardStateCRCOff {
		return HardState{}, 0, false
	}
	hs.VotedFor = NodeID(slot[34 : 34+votedLen])
	return hs, seq, true
}

// save writes hs to the next slot and fsyncs.
func (h *hardStateFile) save(hs HardState) error {
	seq := h.nextSeq
	slot, err := encodeHardStateSlot(seq, hs)
	if err != nil {
		return err
	}
	if _, err := h.f.WriteAt(slot, int64(seq%2)*hardStateSlotBytes); err != nil {
		return fmt.Errorf("write hardstate %s: %w", h.path, err)
	}
	if err := fsyncFile(h.f); err != nil {
		return err
	}
	h.nextSeq = seq + 1
	return nil
}

func encodeHardStateSlot(seq uint64, hs HardState) ([]byte, error) {
	if len(hs.VotedFor) > hardStateCRCOff-34 {
		return nil, fmt.Errorf("raft: votedFor too long (%d bytes)", len(hs.VotedFor))
	}
	slot := make([]byte, hardStateSlotBytes)
	copy(slot[0:8], hardStateMagic[:])
	be.PutUint64(slot[8:16], seq)
	be.PutUint64(slot[16:24], uint64(hs.CurrentTerm))
	be.PutUint64(slot[24:32], uint64(hs.CommitIndex))
	be.PutUint16(slot[32:34], uint16(len(hs.VotedFor)))
	copy(slot[34:], hs.VotedFor)
	be.PutUint64(slot[hardStateCRCOff:], crc(slot[:hardStateCRCOff]))
	return slot, nil
}

func (h *hardStateFile) close() error {
	if h.f == nil {
		return nil
	}
	err := h.f.Close()
	h.f = nil
	return err
}

// ensureSize grows f to at least size bytes (a freshly created journal is
// zero-length; this pads it so the two slots have stable offsets).
func ensureSize(f *os.File, size int64) error {
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", f.Name(), err)
	}
	if fi.Size() >= size {
		return nil
	}
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("grow %s to %d: %w", f.Name(), size, err)
	}
	return fsyncFile(f)
}
