//go:build plan9

package bbolt

// mmapLockFile allocates a regular byte slice on Plan 9 instead of using
// mmap. Cross-process shared memory is not available, so the lock file
// only provides single-process coordination.
func mmapLockFile(f interface{ Fd() uintptr }, sz int) ([]byte, error) {
	return make([]byte, sz), nil
}

// munmapLockFile is a no-op on Plan 9 since the data is a regular
// heap-allocated byte slice.
func munmapLockFile(data []byte) error {
	return nil
}
