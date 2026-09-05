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
	db, err := Open(filepath.Join(t.TempDir(), "stats.db"), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

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
