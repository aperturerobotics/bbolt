//go:build !windows && !plan9

package bbolt_test

import (
	"context"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	bolt "github.com/aperturerobotics/bbolt"
)

// TestCommitCounterReadDoesNotRaceClose proves that reading the commit counter
// concurrently with DB.Close cannot dereference the freed lock-file mmap.
// Before the lockFileMu guard, close() munmapped the lock-file mapping before
// clearing db.lockFile, so a CommitCounter reader that observed a non-nil
// lockFile then loaded through the unmapped pages and SIGSEGV'd (an
// unrecoverable fault, not a Go panic). The guard serializes the readers with
// the lock-file teardown: a closed DB returns 0 instead of faulting.
//
// Run under -race to also catch the unsynchronized read of lf.data versus the
// munmap write. Reverting the guard makes this test crash the test binary.
func TestCommitCounterReadDoesNotRaceClose(t *testing.T) {
	for iter := range 200 {
		dir := t.TempDir()
		db, err := bolt.Open(filepath.Join(dir, "test.db"), 0o600, nil)
		if err != nil {
			t.Fatalf("iter %d: open: %v", iter, err)
		}

		// Commit once so the counter is non-zero and the lock file is mapped.
		if err := db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucket([]byte("b"))
			if err != nil {
				return err
			}
			return b.Put([]byte("k"), []byte("v"))
		}); err != nil {
			t.Fatalf("iter %d: write: %v", iter, err)
		}

		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		start := make(chan struct{})
		for range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for range 20000 {
					_ = db.CommitCounter()
				}
			}()
		}

		// One reader blocks in WaitCommitCounter, which sets up fsnotify on the
		// lock file and re-reads CommitCounter on every event; lastSeen is max so
		// it waits until cancel rather than returning on the fast path.
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _ = db.WaitCommitCounter(ctx, ^uint64(0))
		}()

		close(start)
		runtime.Gosched()
		if err := db.Close(); err != nil {
			cancel()
			t.Fatalf("iter %d: close: %v", iter, err)
		}
		cancel()
		wg.Wait()

		if got := db.CommitCounter(); got != 0 {
			t.Fatalf("iter %d: CommitCounter after close = %d, want 0", iter, got)
		}
	}
}
