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

// TestDB_DrainsLiveTail checks that live pages written after data that is
// later deleted move down the file, so the file shrinks below them, and that
// the moved data survives a reopen.
func TestDB_DrainsLiveTail(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("a mapped file cannot shrink on windows")
	}

	// Write a large bucket, then a small one after it at the end of the file.
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
	for i := range 32 {
		put("drop", i*1000, (i+1)*1000)
	}
	put("keep", 0, 1000)
	full := fileSize(db.Path())
	require.Greater(t, full, int64(64<<20))

	// Delete the large bucket, then commit small unrelated updates so the
	// drain moves the small bucket down and the end of the file is trimmed.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte("drop"))
	}))
	for i := range 64 {
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("counter"))
			if err != nil {
				return err
			}
			return b.Put([]byte("n"), binary.BigEndian.AppendUint32(nil, uint32(i)))
		}))
	}
	require.Less(t, fileSize(db.Path()), full/4)

	// Reopen and read every moved key.
	db.MustClose()
	db.MustReopen()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("keep"))
		require.Equal(t, 1000, b.Stats().KeyN)
		for i := range 1000 {
			require.Equal(t, value, b.Get(binary.BigEndian.AppendUint32(nil, uint32(i))))
		}
		return nil
	}))
}
