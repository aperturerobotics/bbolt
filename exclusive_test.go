package bbolt_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bolt "github.com/aperturerobotics/bbolt"
	berrors "github.com/aperturerobotics/bbolt/errors"
)

// TestOpen_ExclusiveReplacesFile checks that an exclusive open excludes other
// handles, and that an open waiting for it follows a replacement renamed over
// the path.
func TestOpen_ExclusiveReplacesFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("an open file cannot be replaced by rename on windows")
	}

	// An exclusive open times out while another handle is open.
	path := filepath.Join(t.TempDir(), "db")
	shared, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	_, err = bolt.Open(path, 0o600, &bolt.Options{Exclusive: true, Timeout: 100 * time.Millisecond})
	require.ErrorIs(t, err, berrors.ErrTimeout)
	require.NoError(t, shared.Close())

	// Hold the file exclusively, then start an open that waits for it.
	held, err := bolt.Open(path, 0o600, &bolt.Options{Exclusive: true, Timeout: time.Second})
	require.NoError(t, err)
	type result struct {
		db  *bolt.DB
		err error
	}
	opened := make(chan result, 1)
	go func() {
		db, err := bolt.Open(path, 0o600, nil)
		opened <- result{db, err}
	}()

	// Rename a replacement holding a marker bucket over the held file.
	replacement, err := bolt.Open(path+".new", 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, replacement.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("replacement"))
		return err
	}))
	require.NoError(t, replacement.Close())
	require.NoError(t, os.Rename(path+".new", path))
	require.NoError(t, held.Close())

	// The waiting open reads the replacement.
	res := <-opened
	require.NoError(t, res.err)
	defer res.db.Close()
	require.NoError(t, res.db.View(func(tx *bolt.Tx) error {
		require.NotNil(t, tx.Bucket([]byte("replacement")))
		return nil
	}))
}
