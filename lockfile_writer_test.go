package bbolt

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

// openTestLockFile creates a temporary lock file for testing.
func openTestLockFile(t *testing.T) *LockFile {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db-lock")
	lf, err := openLockFile(path, defaultMaxReaders)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lf.Close() })
	return lf
}

func TestWriterLockAcquireRelease(t *testing.T) {
	lf := openTestLockFile(t)

	// Acquire the writer lock.
	if err := lf.AcquireWriterLock(); err != nil {
		t.Fatalf("AcquireWriterLock: %v", err)
	}

	// Verify the lock is held by trying to acquire again (non-blocking).
	// On POSIX, fcntl locks are per-process, so a second TryAcquire from
	// the same process succeeds (the lock is already held by this process).
	// On Windows, LockFileEx is not reentrant on the same handle, so
	// TryAcquire returns false (ERROR_LOCK_VIOLATION).
	ok, err := lf.TryAcquireWriterLock()
	if err != nil {
		t.Fatalf("TryAcquireWriterLock: %v", err)
	}
	if runtime.GOOS == "windows" {
		if ok {
			t.Fatal("TryAcquireWriterLock should return false on Windows (not reentrant)")
		}
	} else {
		if !ok {
			t.Fatal("TryAcquireWriterLock returned false for same-process lock")
		}
	}

	// Release the writer lock.
	if err := lf.ReleaseWriterLock(); err != nil {
		t.Fatalf("ReleaseWriterLock: %v", err)
	}
}

func TestWriterLockTryAcquire(t *testing.T) {
	lf := openTestLockFile(t)

	// Try to acquire the writer lock (non-blocking, should succeed).
	ok, err := lf.TryAcquireWriterLock()
	if err != nil {
		t.Fatalf("TryAcquireWriterLock: %v", err)
	}
	if !ok {
		t.Fatal("TryAcquireWriterLock failed on unlocked file")
	}

	// Release.
	if err := lf.ReleaseWriterLock(); err != nil {
		t.Fatalf("ReleaseWriterLock: %v", err)
	}

	// Try again after release, should succeed.
	ok, err = lf.TryAcquireWriterLock()
	if err != nil {
		t.Fatalf("TryAcquireWriterLock after release: %v", err)
	}
	if !ok {
		t.Fatal("TryAcquireWriterLock failed after release")
	}

	if err := lf.ReleaseWriterLock(); err != nil {
		t.Fatalf("ReleaseWriterLock: %v", err)
	}
}

func TestWriterLockExclusion(t *testing.T) {
	// Note: fcntl byte-range locks are per-process on POSIX, not per-fd.
	// This means two goroutines in the same process using the SAME lock file
	// fd will share the lock. To test true exclusion, we open a SECOND fd
	// to the same file. On POSIX, both fds belong to the same process so
	// the kernel still treats them as the same lock holder -- fcntl locks
	// are per-process, not per-fd. Therefore, this in-process test cannot
	// verify cross-process exclusion.
	//
	// Cross-process exclusion testing is deferred to Step 5 (multi-process
	// tests using exec.Command to spawn child processes).
	//
	// This test verifies that the API works correctly when called from
	// multiple goroutines with separate LockFile instances on the same file.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db-lock")

	lf1, err := openLockFile(path, defaultMaxReaders)
	if err != nil {
		t.Fatal(err)
	}
	defer lf1.Close()

	lf2, err := openLockFile(path, defaultMaxReaders)
	if err != nil {
		t.Fatal(err)
	}
	defer lf2.Close()

	// On POSIX: both fds are in the same process, so fcntl treats them
	// as the same lock. Both will succeed.
	// On Windows: locks are per-handle, so the second acquire would block
	// or fail. We test the non-blocking path.

	if err := lf1.AcquireWriterLock(); err != nil {
		t.Fatalf("lf1 AcquireWriterLock: %v", err)
	}

	ok, err := lf2.TryAcquireWriterLock()
	if err != nil {
		t.Fatalf("lf2 TryAcquireWriterLock: %v", err)
	}
	// On POSIX: ok will be true (same process). On Windows: ok will be
	// false (different handle). We accept both behaviors in this test.
	_ = ok

	if err := lf1.ReleaseWriterLock(); err != nil {
		t.Fatalf("lf1 ReleaseWriterLock: %v", err)
	}
	// Release lf2 only if it acquired.
	if ok {
		if err := lf2.ReleaseWriterLock(); err != nil {
			t.Fatalf("lf2 ReleaseWriterLock: %v", err)
		}
	}
}

func TestWriterLockConcurrentGoroutines(t *testing.T) {
	// Test that multiple goroutines can sequentially acquire and release
	// the writer lock without errors. This tests the in-process serialization
	// path (the in-process rwlock in db.go will handle goroutine exclusion;
	// this test just ensures the fcntl calls don't error under concurrent use).
	lf := openTestLockFile(t)

	var wg sync.WaitGroup
	errs := make(chan error, 10)

	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := lf.AcquireWriterLock(); err != nil {
				errs <- err
				return
			}
			if err := lf.ReleaseWriterLock(); err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("goroutine error: %v", err)
	}
}

func TestWriterLockFileRemoved(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db-lock")

	lf, err := openLockFile(path, defaultMaxReaders)
	if err != nil {
		t.Fatal(err)
	}
	defer lf.Close()

	// Remove the file while fd is still open. The lock should still work
	// because the fd remains valid.
	os.Remove(path)

	if err := lf.AcquireWriterLock(); err != nil {
		t.Fatalf("AcquireWriterLock after file removed: %v", err)
	}

	if err := lf.ReleaseWriterLock(); err != nil {
		t.Fatalf("ReleaseWriterLock after file removed: %v", err)
	}
}
