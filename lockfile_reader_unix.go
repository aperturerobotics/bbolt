//go:build !windows && !plan9

package bbolt

import "syscall"

// processAlive checks whether the given process ID is still alive.
// It sends signal 0 which does not actually kill the process but
// checks for its existence. EPERM means the process exists but we
// lack permission to signal it -- still alive.
func processAlive(pid uint32) bool {
	err := syscall.Kill(int(pid), 0)
	return err == nil || err == syscall.EPERM
}
