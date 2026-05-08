//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package lock

import "os"

const fenceFileLocksSupported = false

func lockFenceFile(_ *os.File) error {
	return nil
}

func unlockFenceFile(_ *os.File) error {
	return nil
}
