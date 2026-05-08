//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package lock

import (
	"os"
	"syscall"
)

const fenceFileLocksSupported = true

func lockFenceFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func unlockFenceFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
