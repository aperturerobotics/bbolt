package bbolt

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	berrors "github.com/aperturerobotics/bbolt/errors"
)

func TestLockFileValidatePathChanged(t *testing.T) {
	lf := openTestLockFile(t)

	if err := os.Remove(lf.Path()); err != nil {
		t.Fatalf("remove lock file path: %v", err)
	}
	if err := os.WriteFile(lf.Path(), []byte("replacement"), 0o600); err != nil {
		t.Fatalf("write replacement lock file: %v", err)
	}

	err := lf.ValidatePath()
	if !errors.Is(err, berrors.ErrLockFileChanged) {
		t.Fatalf("ValidatePath() error = %v, want %v", err, berrors.ErrLockFileChanged)
	}
}

func TestDBBeginFailsWhenLockFileChanged(t *testing.T) {
	db := openTestDBWithBucket(t)
	defer db.Close()

	lockPath := db.Path() + "-lock"
	if err := os.Remove(lockPath); err != nil {
		t.Fatalf("remove lock file path: %v", err)
	}
	if err := os.WriteFile(lockPath, []byte("replacement"), 0o600); err != nil {
		t.Fatalf("write replacement lock file: %v", err)
	}

	_, err := db.Begin(false)
	if !errors.Is(err, berrors.ErrLockFileChanged) {
		t.Fatalf("Begin(false) error = %v, want %v", err, berrors.ErrLockFileChanged)
	}

	_, err = db.Begin(true)
	if !errors.Is(err, berrors.ErrDatabaseNotOpen) {
		t.Fatalf("Begin(true) error = %v, want %v", err, berrors.ErrDatabaseNotOpen)
	}
}

func TestDBBeginClosesWhenDatabaseFileRemoved(t *testing.T) {
	db := openTestDBWithBucket(t)
	defer db.Close()

	dbPath := db.Path()
	if err := os.Remove(dbPath); err != nil {
		t.Fatalf("remove db file path: %v", err)
	}

	_, err := db.Begin(false)
	if !errors.Is(err, berrors.ErrLockFileChanged) {
		t.Fatalf("Begin(false) error = %v, want %v", err, berrors.ErrLockFileChanged)
	}
	if db.opened {
		t.Fatal("expected database to close after db file path removal")
	}
}

func TestTxCommitFailsWhenLockFileRemoved(t *testing.T) {
	db := openTestDBWithBucket(t)
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("Begin(true): %v", err)
	}

	b := tx.Bucket([]byte("test"))
	if b == nil {
		t.Fatal("bucket not found")
	}
	if err := b.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	lockPath := db.Path() + "-lock"
	if err := os.Remove(lockPath); err != nil {
		t.Fatalf("remove lock file path: %v", err)
	}

	err = tx.Commit()
	if !errors.Is(err, berrors.ErrLockFileChanged) {
		t.Fatalf("Commit() error = %v, want %v", err, berrors.ErrLockFileChanged)
	}
}

func TestTxCommitFailsWhenLockFileRemovedAfterCommitStarts(t *testing.T) {
	db := openTestDBWithBucket(t)
	defer db.Close()

	lockPath := db.Path() + "-lock"
	db.ops.beforeCommitPhase = func(phase string) {
		if phase != "before-rebalance" {
			return
		}
		db.ops.beforeCommitPhase = nil
		if err := os.Remove(lockPath); err != nil {
			t.Fatalf("remove lock file path: %v", err)
		}
	}

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("Begin(true): %v", err)
	}

	b := tx.Bucket([]byte("test"))
	if b == nil {
		t.Fatal("bucket not found")
	}
	if err := b.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	err = tx.Commit()
	if !errors.Is(err, berrors.ErrLockFileChanged) {
		t.Fatalf("Commit() error = %v, want %v", err, berrors.ErrLockFileChanged)
	}
}

func TestTxCommitReturnsCommitPhasePanicAsError(t *testing.T) {
	db := openTestDBWithBucket(t)
	defer db.Close()

	db.ops.beforeCommitPhase = func(phase string) {
		if phase != "before-spill" {
			return
		}
		panic("page 2 already freed")
	}

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("Begin(true): %v", err)
	}

	b := tx.Bucket([]byte("test"))
	if b == nil {
		t.Fatal("bucket not found")
	}
	if err := b.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	err = tx.Commit()
	if err == nil || err.Error() != "panic: page 2 already freed" {
		t.Fatalf("Commit() error = %v, want panic error", err)
	}
}

func openTestDBWithBucket(t *testing.T) *DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("test"))
		return err
	}); err != nil {
		db.Close()
		t.Fatalf("create bucket: %v", err)
	}
	return db
}
