//go:build !windows && !plan9

package bbolt

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// mmapLockFile memory-maps the lock file with MAP_SHARED for cross-process visibility.
func mmapLockFile(f interface{ Fd() uintptr }, sz int) ([]byte, error) {
	b, err := unix.Mmap(int(f.Fd()), 0, sz, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap lock file: %w", err)
	}
	return b, nil
}

// munmapLockFile unmaps the lock file.
func munmapLockFile(data []byte) error {
	return unix.Munmap(data)
}
