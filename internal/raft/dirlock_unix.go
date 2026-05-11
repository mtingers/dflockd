//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package raft

import (
	"os"
	"syscall"
)

// fileLocksSupported reports whether exclusive advisory file locking is
// available on this platform. FileStorage refuses to operate without it
// (two dflockd processes sharing one --raft-dir would corrupt the log).
const fileLocksSupported = true

func lockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func unlockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
