package bbolt_test

import (
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "github.com/aperturerobotics/bbolt"
	"github.com/aperturerobotics/bbolt/internal/btesting"
)

// TestDB_ShrinksAfterDelete checks that deleting most of a database returns
// its space to the file system after later commits, and that the remaining
// data survives a reopen.
func TestDB_ShrinksAfterDelete(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("a mapped file cannot shrink on windows")
	}

	// Write a small bucket, then a large one after it.
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	value := make([]byte, 2048)
	put := func(bucket string, from, to int) {
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte(bucket))
			if err != nil {
				return err
			}
			for i := from; i < to; i++ {
				key := binary.BigEndian.AppendUint32(nil, uint32(i))
				if err := b.Put(key, value); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	put("keep", 0, 100)
	for i := range 32 {
		put("drop", i*1000, (i+1)*1000)
	}
	full := fileSize(db.Path())
	require.Greater(t, full, int64(64<<20))

	// Delete the large bucket, then commit small updates so its pages are
	// released and trimmed from the end of the file.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte("drop"))
	}))
	for i := range 4 {
		put("keep", 100+i, 101+i)
	}
	shrunk := fileSize(db.Path())
	require.Less(t, shrunk, full/4)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		require.LessOrEqual(t, tx.Size(), shrunk)
		return nil
	}))

	// Grow again past the shrunk size, then reopen and read every key.
	put("keep", 1000, 9000)
	db.MustClose()
	db.MustReopen()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		require.Equal(t, 8104, tx.Bucket([]byte("keep")).Stats().KeyN)
		require.Nil(t, tx.Bucket([]byte("drop")))
		return nil
	}))
}
