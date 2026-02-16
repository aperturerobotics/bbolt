//go:build !windows && !solaris && !aix && !android

package bbolt

import (
	"io"
	"syscall"
)

// AcquireWriterLock acquires the cross-process writer lock.
// Blocks until the lock is available. Uses fcntl F_SETLKW with F_WRLCK
// on the writer lock region of the lock file.
func (lf *LockFile) AcquireWriterLock() error {
	lock := syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: int16(io.SeekStart),
		Start:  writerLockRegionOffset,
		Len:    writerLockRegionSize,
	}
	for {
		err := syscall.FcntlFlock(lf.fd.Fd(), syscall.F_SETLKW, &lock)
		if err == nil {
			return nil
		}
		if err != syscall.EINTR {
			return writerLockError("acquire", err)
		}
		// EINTR: retry the syscall.
	}
}

// TryAcquireWriterLock attempts to acquire the writer lock without blocking.
// Returns true if acquired, false if another process holds it.
func (lf *LockFile) TryAcquireWriterLock() (bool, error) {
	lock := syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: int16(io.SeekStart),
		Start:  writerLockRegionOffset,
		Len:    writerLockRegionSize,
	}
	for {
		err := syscall.FcntlFlock(lf.fd.Fd(), syscall.F_SETLK, &lock)
		if err == nil {
			return true, nil
		}
		if err == syscall.EINTR {
			continue
		}
		if err == syscall.EAGAIN || err == syscall.EACCES {
			return false, nil
		}
		return false, writerLockError("try acquire", err)
	}
}

// ReleaseWriterLock releases the cross-process writer lock.
func (lf *LockFile) ReleaseWriterLock() error {
	lock := syscall.Flock_t{
		Type:   syscall.F_UNLCK,
		Whence: int16(io.SeekStart),
		Start:  writerLockRegionOffset,
		Len:    writerLockRegionSize,
	}
	for {
		err := syscall.FcntlFlock(lf.fd.Fd(), syscall.F_SETLK, &lock)
		if err == nil {
			return nil
		}
		if err != syscall.EINTR {
			return writerLockError("release", err)
		}
		// EINTR: retry the syscall.
	}
}
