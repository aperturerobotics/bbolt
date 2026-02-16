//go:build windows

package bbolt

import (
	"golang.org/x/sys/windows"
)

// AcquireWriterLock acquires the cross-process writer lock.
// Blocks until the lock is available. Uses LockFileEx with
// LOCKFILE_EXCLUSIVE_LOCK on the writer lock region of the lock file.
func (lf *LockFile) AcquireWriterLock() error {
	err := windows.LockFileEx(
		windows.Handle(lf.fd.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK,
		0,
		writerLockRegionSize,
		0,
		// Zero-initialized Overlapped is correct for blocking byte-range locks;
		// hEvent is not needed when not using overlapped I/O.
		&windows.Overlapped{Offset: writerLockRegionOffset},
	)
	if err != nil {
		return writerLockError("acquire", err)
	}
	return nil
}

// TryAcquireWriterLock attempts to acquire the writer lock without blocking.
// Returns true if acquired, false if another process holds it.
func (lf *LockFile) TryAcquireWriterLock() (bool, error) {
	err := windows.LockFileEx(
		windows.Handle(lf.fd.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		writerLockRegionSize,
		0,
		// Zero-initialized Overlapped is correct for blocking byte-range locks;
		// hEvent is not needed when not using overlapped I/O.
		&windows.Overlapped{Offset: writerLockRegionOffset},
	)
	if err == nil {
		return true, nil
	}
	if err == windows.ERROR_LOCK_VIOLATION {
		return false, nil
	}
	return false, writerLockError("try acquire", err)
}

// ReleaseWriterLock releases the cross-process writer lock.
func (lf *LockFile) ReleaseWriterLock() error {
	err := windows.UnlockFileEx(
		windows.Handle(lf.fd.Fd()),
		0,
		writerLockRegionSize,
		0,
		// Zero-initialized Overlapped is correct for blocking byte-range locks;
		// hEvent is not needed when not using overlapped I/O.
		&windows.Overlapped{Offset: writerLockRegionOffset},
	)
	if err != nil {
		return writerLockError("release", err)
	}
	return nil
}
