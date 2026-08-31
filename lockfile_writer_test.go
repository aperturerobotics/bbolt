package bbolt

import (
	"os"
	"path/filepath"
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

	// Verify the lock is not reentrant.
	ok, err := lf.TryAcquireWriterLock()
	if err != nil {
		t.Fatalf("TryAcquireWriterLock: %v", err)
	}
	if ok {
		t.Fatal("TryAcquireWriterLock acquired an already-held process lock")
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

	if err := lf1.AcquireWriterLock(); err != nil {
		t.Fatalf("lf1 AcquireWriterLock: %v", err)
	}
	if ok, err := lf2.TryAcquireWriterLock(); err != nil {
		t.Fatalf("lf2 TryAcquireWriterLock: %v", err)
	} else if ok {
		t.Fatal("second handle acquired the process writer lock")
	}
	if err := lf1.ReleaseWriterLock(); err != nil {
		t.Fatalf("lf1 ReleaseWriterLock: %v", err)
	}

	if ok, err := lf2.TryAcquireWriterLock(); err != nil {
		t.Fatalf("lf2 TryAcquireWriterLock after release: %v", err)
	} else if !ok {
		t.Fatal("second handle did not acquire after release")
	}
	if err := lf2.ReleaseWriterLock(); err != nil {
		t.Fatalf("lf2 ReleaseWriterLock: %v", err)
	}
}

func TestWriterLockBlocksAcrossHandles(t *testing.T) {
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

	if err := lf1.AcquireWriterLock(); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	beforeProcessWriterLockHook = func(lf *LockFile) {
		if lf == lf2 {
			close(entered)
		}
	}
	defer func() { beforeProcessWriterLockHook = nil }()

	result := make(chan error, 1)
	go func() {
		result <- lf2.AcquireWriterLock()
	}()
	<-entered
	select {
	case err := <-result:
		t.Fatalf("second handle did not block: %v", err)
	default:
	}
	if err := lf1.ReleaseWriterLock(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := lf2.ReleaseWriterLock(); err != nil {
		t.Fatal(err)
	}
}

func TestWriterLockExcludesAliasedPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db-lock")
	alias := dir + string(os.PathSeparator) + "." + string(os.PathSeparator) + "test.db-lock"

	lf1, err := openLockFile(path, defaultMaxReaders)
	if err != nil {
		t.Fatal(err)
	}
	defer lf1.Close()

	lf2, err := openLockFile(alias, defaultMaxReaders)
	if err != nil {
		t.Fatal(err)
	}
	defer lf2.Close()

	if err := lf1.AcquireWriterLock(); err != nil {
		t.Fatal(err)
	}
	if ok, err := lf2.TryAcquireWriterLock(); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("aliased handle acquired the process writer lock")
	}
	if err := lf1.ReleaseWriterLock(); err != nil {
		t.Fatal(err)
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
