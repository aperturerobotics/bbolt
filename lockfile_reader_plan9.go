//go:build plan9

package bbolt

// processAlive on Plan 9 conservatively returns false. This causes
// ClearStaleReaders to reclaim all slots held by other PIDs. Since the
// lock file is not shared across processes on Plan 9 (no mmap), this is
// safe and prevents slot leaks within a single process.
func processAlive(pid uint32) bool {
	return false
}
