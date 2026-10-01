package bbolt

import (
	"path/filepath"
	"testing"
	"time"
)

// TestCoordinationRefreshSerializesStatistics requires coordination refresh
// to serialize with transaction completion and statistics readers.
func TestCoordinationRefreshSerializesStatistics(t *testing.T) {
	// Open the multi-process database mode used by coordinated World storage.
	path := filepath.Join(t.TempDir(), "stats.db")
	db, err := Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Commit through a second handle so the refresh reloads the freelist.
	other, err := Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("data"))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// A held statistics lock must prevent the refresh from publishing its count.
	db.statlock.Lock()
	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- db.RefreshForCoordinationLock()
	}()

	select {
	case err := <-refreshDone:
		db.statlock.Unlock()
		t.Fatalf("coordination refresh bypassed the statistics lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	db.statlock.Unlock()
	select {
	case err := <-refreshDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("coordination refresh did not finish after statistics were unlocked")
	}
}
