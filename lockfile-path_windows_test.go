//go:build windows

package bbolt

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	berrors "github.com/aperturerobotics/bbolt/errors"
)

// TestMappedLockFilePathIdentity accepts the unchanged path while its shared
// coordination mapping remains live and rejects a different file identity.
func TestMappedLockFilePathIdentity(t *testing.T) {
	// Open the coordinated storage mode used by installed desktop applications.
	db, err := Open(filepath.Join(t.TempDir(), "desktop.bdb"), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.ValidatePath(); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("account"))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// Windows prevents replacing an open mapped file. Point the same validation
	// at another existing file to require identity comparison, not existence.
	originalPath := db.lockFile.path
	otherPath := filepath.Join(t.TempDir(), "different-lock")
	if err := os.WriteFile(otherPath, []byte("different file"), 0o600); err != nil {
		t.Fatal(err)
	}
	db.lockFile.path = otherPath
	err = db.ValidatePath()
	db.lockFile.path = originalPath
	if !errors.Is(err, berrors.ErrLockFileChanged) {
		t.Fatalf("different coordination identity was accepted: %v", err)
	}
}
