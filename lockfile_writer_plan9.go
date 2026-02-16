//go:build plan9

package bbolt

// AcquireWriterLock is a no-op on Plan 9. Cross-process file locking
// (fcntl) is not available; the lock file provides single-process
// coordination only.
func (lf *LockFile) AcquireWriterLock() error {
	return nil
}

// TryAcquireWriterLock always succeeds on Plan 9 since there is no
// cross-process contention.
func (lf *LockFile) TryAcquireWriterLock() (bool, error) {
	return true, nil
}

// ReleaseWriterLock is a no-op on Plan 9.
func (lf *LockFile) ReleaseWriterLock() error {
	return nil
}
