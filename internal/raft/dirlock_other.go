//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package raft

import "os"

const fileLocksSupported = false

func lockFile(_ *os.File) error   { return nil }
func unlockFile(_ *os.File) error { return nil }
