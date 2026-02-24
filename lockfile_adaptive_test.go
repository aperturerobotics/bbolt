package bbolt

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// Adaptive access mode tests.
//
// These tests have NO build tags and run on ALL platforms including Windows,
// ensuring the adaptive single/multi-process state machine is exercised
// in every CI environment (Linux amd64/arm64, macOS, Windows).

// --- LockFile-level tests ---

// TestLockFileWriterCount verifies the atomic writerCount field in
// the lock file header increments and decrements correctly.
func TestLockFileWriterCount(t *testing.T) {
	lf := openTestLockFile(t)

	if n := lf.IncrementWriterCount(); n != 1 {
		t.Fatalf("IncrementWriterCount()=%d, want 1", n)
	}
	if n := lf.IncrementWriterCount(); n != 2 {
		t.Fatalf("IncrementWriterCount()=%d, want 2", n)
	}
	if n := lf.IncrementWriterCount(); n != 3 {
		t.Fatalf("IncrementWriterCount()=%d, want 3", n)
	}
	if n := lf.DecrementWriterCount(); n != 2 {
		t.Fatalf("DecrementWriterCount()=%d, want 2", n)
	}
	if n := lf.DecrementWriterCount(); n != 1 {
		t.Fatalf("DecrementWriterCount()=%d, want 1", n)
	}
	if n := lf.DecrementWriterCount(); n != 0 {
		t.Fatalf("DecrementWriterCount()=%d, want 0", n)
	}
}

// TestLockFileAccessModeCAS verifies the atomic CAS and Set operations
// on the accessMode field follow the state machine correctly.
func TestLockFileAccessModeCAS(t *testing.T) {
	lf := openTestLockFile(t)

	// Initial mode is available (0).
	if m := lf.AccessMode(); m != accessModeAvailable {
		t.Fatalf("initial AccessMode()=%d, want %d", m, accessModeAvailable)
	}

	// CAS available->single succeeds.
	if !lf.CASAccessMode(accessModeAvailable, accessModeSingle) {
		t.Fatal("CAS(available->single) failed")
	}
	if m := lf.AccessMode(); m != accessModeSingle {
		t.Fatalf("AccessMode()=%d, want %d", m, accessModeSingle)
	}

	// CAS with wrong old value fails.
	if lf.CASAccessMode(accessModeAvailable, accessModeSingle) {
		t.Fatal("CAS(available->single) should fail when already single")
	}

	// CAS single->escalating succeeds.
	if !lf.CASAccessMode(accessModeSingle, accessModeEscalating) {
		t.Fatal("CAS(single->escalating) failed")
	}

	// Set to multi.
	lf.SetAccessMode(accessModeMulti)
	if m := lf.AccessMode(); m != accessModeMulti {
		t.Fatalf("AccessMode()=%d, want %d", m, accessModeMulti)
	}

	// Reset to available.
	lf.SetAccessMode(accessModeAvailable)
	if m := lf.AccessMode(); m != accessModeAvailable {
		t.Fatalf("AccessMode()=%d, want %d", m, accessModeAvailable)
	}
}

// TestLockFileAccessModeSharedVisibility verifies that two LockFile
// instances on the same file see each other's writerCount and accessMode
// changes via MAP_SHARED mmap. This simulates two processes sharing
// the lock file.
func TestLockFileAccessModeSharedVisibility(t *testing.T) {
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

	// lf1 enters single mode.
	lf1.IncrementWriterCount()
	if !lf1.CASAccessMode(accessModeAvailable, accessModeSingle) {
		t.Fatal("lf1 CAS(available->single) failed")
	}

	// lf2 should see accessMode=single via shared mmap.
	if m := lf2.AccessMode(); m != accessModeSingle {
		t.Fatalf("lf2 sees accessMode=%d, want %d", m, accessModeSingle)
	}

	// lf2 signals escalation (simulates second opener).
	if n := lf2.IncrementWriterCount(); n != 2 {
		t.Fatalf("lf2 IncrementWriterCount()=%d, want 2", n)
	}
	if !lf2.CASAccessMode(accessModeSingle, accessModeEscalating) {
		t.Fatal("lf2 CAS(single->escalating) failed")
	}

	// lf1 should see escalating.
	if m := lf1.AccessMode(); m != accessModeEscalating {
		t.Fatalf("lf1 sees accessMode=%d, want %d", m, accessModeEscalating)
	}

	// lf1 transitions to multi (simulates checkEscalation).
	lf1.SetAccessMode(accessModeMulti)
	if m := lf2.AccessMode(); m != accessModeMulti {
		t.Fatalf("lf2 sees accessMode=%d, want %d", m, accessModeMulti)
	}

	// Both exit. Last one resets mode.
	lf1.DecrementWriterCount()
	if n := lf2.DecrementWriterCount(); n != 0 {
		t.Fatalf("writerCount=%d, want 0", n)
	}
	lf2.SetAccessMode(accessModeAvailable)

	if m := lf1.AccessMode(); m != accessModeAvailable {
		t.Fatalf("after reset: lf1 sees accessMode=%d, want %d", m, accessModeAvailable)
	}
}

// --- DB-level tests ---

// TestDBAdaptiveSingleProcess verifies that a single writer-capable
// opener enters single-process mode and can perform read/write
// transactions without multi-process coordination overhead.
func TestDBAdaptiveSingleProcess(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if !db.singleProcess {
		t.Fatal("expected singleProcess=true")
	}
	if m := db.lockFile.AccessMode(); m != accessModeSingle {
		t.Fatalf("accessMode=%d, want %d", m, accessModeSingle)
	}

	// Write transaction works in single-process mode.
	if err := db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("test"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k"), []byte("v"))
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// Read transaction works.
	if err := db.View(func(tx *Tx) error {
		v := tx.Bucket([]byte("test")).Get([]byte("k"))
		if string(v) != "v" {
			return fmt.Errorf("got %q, want %q", v, "v")
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}

	// Still in single-process mode after transactions.
	if !db.singleProcess {
		t.Fatal("singleProcess should still be true after tx")
	}

	db.Close()

	// Verify lock file reset: writerCount=0, accessMode=available.
	lf, err := openLockFile(dbPath+"-lock", defaultMaxReaders)
	if err != nil {
		t.Fatalf("openLockFile: %v", err)
	}
	defer lf.Close()

	if m := lf.AccessMode(); m != accessModeAvailable {
		t.Fatalf("after close: accessMode=%d, want %d", m, accessModeAvailable)
	}
}

// TestDBAdaptiveEscalation verifies the full state machine transition
// when a second writer-capable process opens the DB: single -> escalating
// -> multi. Verified via checkEscalation at read transaction boundary.
func TestDBAdaptiveEscalation(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// First opener: single-process mode.
	db1, err := Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Open db1: %v", err)
	}

	if !db1.singleProcess {
		t.Fatal("db1: expected singleProcess=true")
	}

	// Second opener: triggers escalation.
	db2, err := Open(dbPath, 0600, &Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Open db2: %v", err)
	}

	if db2.singleProcess {
		t.Fatal("db2: expected singleProcess=false")
	}

	// accessMode should be escalating (db1 hasn't checked yet).
	if m := db1.lockFile.AccessMode(); m != accessModeEscalating {
		t.Fatalf("after db2 open: accessMode=%d, want %d", m, accessModeEscalating)
	}

	// Read tx on db1 triggers checkEscalation at beginTx.
	if err := db1.View(func(tx *Tx) error { return nil }); err != nil {
		t.Fatalf("db1 View: %v", err)
	}

	// db1 should have escalated to multi.
	if db1.singleProcess {
		t.Fatal("db1: expected singleProcess=false after escalation")
	}
	if m := db1.lockFile.AccessMode(); m != accessModeMulti {
		t.Fatalf("accessMode=%d, want %d", m, accessModeMulti)
	}

	// Close db2. Mode stays multi while db1 is open (writerCount > 0).
	db2.Close()
	if m := db1.lockFile.AccessMode(); m != accessModeMulti {
		t.Fatalf("after db2 close: accessMode=%d, want %d", m, accessModeMulti)
	}

	// Close db1. Mode resets to available (writerCount reaches 0).
	db1.Close()

	lf, err := openLockFile(dbPath+"-lock", defaultMaxReaders)
	if err != nil {
		t.Fatalf("openLockFile: %v", err)
	}
	defer lf.Close()

	if m := lf.AccessMode(); m != accessModeAvailable {
		t.Fatalf("after all close: accessMode=%d, want %d", m, accessModeAvailable)
	}
}

// TestDBAdaptiveCheckEscalationWriteTx verifies that checkEscalation
// fires at the start of write transactions (beginRWTx), not just reads.
func TestDBAdaptiveCheckEscalationWriteTx(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db1, err := Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Open db1: %v", err)
	}
	defer db1.Close()

	if err := db1.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("test"))
		return err
	}); err != nil {
		t.Fatalf("initial Update: %v", err)
	}

	if !db1.singleProcess {
		t.Fatal("expected singleProcess=true before escalation")
	}

	// Second opener triggers escalation.
	db2, err := Open(dbPath, 0600, &Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Open db2: %v", err)
	}
	defer db2.Close()

	// Write tx on db1 triggers checkEscalation at beginRWTx.
	if err := db1.Update(func(tx *Tx) error {
		return tx.Bucket([]byte("test")).Put([]byte("k"), []byte("v"))
	}); err != nil {
		t.Fatalf("Update after escalation: %v", err)
	}

	if db1.singleProcess {
		t.Fatal("expected singleProcess=false after write tx escalation")
	}
	if m := db1.lockFile.AccessMode(); m != accessModeMulti {
		t.Fatalf("accessMode=%d, want %d", m, accessModeMulti)
	}
}

// TestDBAdaptiveEscalationMidTransaction verifies that checkEscalation
// at tx.close() detects escalation that occurred while a transaction
// was in flight.
func TestDBAdaptiveEscalationMidTransaction(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db1, err := Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Open db1: %v", err)
	}
	defer db1.Close()

	if err := db1.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("test"))
		return err
	}); err != nil {
		t.Fatalf("initial Update: %v", err)
	}

	// Start a read tx while still in single mode.
	tx, err := db1.Begin(false)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	// db1 is still single (checkEscalation at beginTx saw mode=single).
	if !db1.singleProcess {
		t.Fatal("expected singleProcess=true during tx")
	}

	// Open db2 mid-tx (simulates another process opening while tx in flight).
	db2, err := Open(dbPath, 0600, &Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Open db2: %v", err)
	}
	defer db2.Close()

	// Lock file shows escalating, but db1 hasn't checked yet.
	if m := db1.lockFile.AccessMode(); m != accessModeEscalating {
		t.Fatalf("mid-tx accessMode=%d, want %d", m, accessModeEscalating)
	}
	if !db1.singleProcess {
		t.Fatal("db1 should still be singleProcess during tx")
	}

	// End the tx. checkEscalation fires at tx.close().
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Now db1 should have escalated.
	if db1.singleProcess {
		t.Fatal("expected singleProcess=false after tx.close")
	}
	if m := db1.lockFile.AccessMode(); m != accessModeMulti {
		t.Fatalf("after tx.close: accessMode=%d, want %d", m, accessModeMulti)
	}
}

// TestDBAdaptiveReadOnlyNoImpact verifies that read-only openers do
// not affect the writerCount or accessMode state machine. A write-
// capable opener should stay in single-process mode when a read-only
// opener is also present.
func TestDBAdaptiveReadOnlyNoImpact(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create DB with initial data.
	func() {
		db, err := Open(dbPath, 0600, nil)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer db.Close()
		if err := db.Update(func(tx *Tx) error {
			_, err := tx.CreateBucket([]byte("test"))
			return err
		}); err != nil {
			t.Fatalf("Update: %v", err)
		}
	}()

	// Reopen as writer: single-process.
	dbWriter, err := Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Open writer: %v", err)
	}
	defer dbWriter.Close()

	if !dbWriter.singleProcess {
		t.Fatal("writer: expected singleProcess=true")
	}

	// Open read-only. Should NOT trigger escalation.
	dbReader, err := Open(dbPath, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("Open reader: %v", err)
	}
	defer dbReader.Close()

	// Writer should still be in single-process mode.
	if !dbWriter.singleProcess {
		t.Fatal("writer: singleProcess should be true after read-only open")
	}
	if m := dbWriter.lockFile.AccessMode(); m != accessModeSingle {
		t.Fatalf("accessMode=%d, want %d", m, accessModeSingle)
	}
}

// TestDBAdaptiveReopenAfterTeardown verifies that after all writer-
// capable openers close (resetting accessMode to available), reopening
// enters single-process mode again.
func TestDBAdaptiveReopenAfterTeardown(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Escalate to multi, then close all.
	func() {
		db1, err := Open(dbPath, 0600, nil)
		if err != nil {
			t.Fatalf("Open db1: %v", err)
		}
		db2, err := Open(dbPath, 0600, &Options{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatalf("Open db2: %v", err)
		}
		_ = db1.View(func(tx *Tx) error { return nil }) // trigger escalation
		db2.Close()
		db1.Close()
	}()

	// Reopen: should be back to single-process.
	db, err := Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()

	if !db.singleProcess {
		t.Fatal("expected singleProcess=true after reopen")
	}
	if m := db.lockFile.AccessMode(); m != accessModeSingle {
		t.Fatalf("accessMode=%d, want %d", m, accessModeSingle)
	}
}

// TestDBAdaptiveMultipleWriteTransactions verifies that multiple write
// transactions in single-process mode work correctly without the
// overhead of refreshForWriter.
func TestDBAdaptiveMultipleWriteTransactions(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("test"))
		return err
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// Write 100 keys across multiple transactions.
	for i := range 10 {
		if err := db.Update(func(tx *Tx) error {
			b := tx.Bucket([]byte("test"))
			for j := range 10 {
				k := fmt.Sprintf("key-%d-%d", i, j)
				if err := b.Put([]byte(k), []byte("val")); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("Update round %d: %v", i, err)
		}
	}

	// Still single-process after many tx.
	if !db.singleProcess {
		t.Fatal("singleProcess should be true after multiple writes")
	}

	// Verify all keys exist.
	if err := db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("test"))
		for i := range 10 {
			for j := range 10 {
				k := fmt.Sprintf("key-%d-%d", i, j)
				if v := b.Get([]byte(k)); string(v) != "val" {
					return fmt.Errorf("%s=%q, want %q", k, v, "val")
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("verify: %v", err)
	}
}
