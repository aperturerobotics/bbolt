package bbolt

import (
	"path/filepath"
	"testing"
	"time"
)

func TestCoordinationRefreshSerializesStatistics(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "stats.db"), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

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
	if err := <-refreshDone; err != nil {
		t.Fatal(err)
	}
}
