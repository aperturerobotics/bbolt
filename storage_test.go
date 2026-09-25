package bbolt_test

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "github.com/aperturerobotics/bbolt"
)

// memStorage is a Storage over a byte slice that outlives the DB.
type memStorage struct {
	mtx    sync.Mutex
	data   []byte
	syncs  int
	closed bool
}

func (m *memStorage) ReadAt(p []byte, off int64) (int, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *memStorage) WriteAt(p []byte, off int64) (int, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if end := off + int64(len(p)); end > int64(len(m.data)) {
		m.data = append(m.data, make([]byte, end-int64(len(m.data)))...)
	}
	return copy(m.data[off:], p), nil
}

func (m *memStorage) Size() (int64, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return int64(len(m.data)), nil
}

func (m *memStorage) Truncate(size int64) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if size <= int64(len(m.data)) {
		m.data = m.data[:size]
		return nil
	}
	m.data = append(m.data, make([]byte, size-int64(len(m.data)))...)
	return nil
}

func (m *memStorage) Sync() error {
	m.mtx.Lock()
	m.syncs++
	m.mtx.Unlock()
	return nil
}

func (m *memStorage) Close() error {
	m.mtx.Lock()
	m.closed = true
	m.mtx.Unlock()
	return nil
}

// TestOpenStorage checks that a database on a Storage grows past several heap
// remaps, reopens from the stored bytes, and copies out through WriteTo.
func TestOpenStorage(t *testing.T) {
	s := &memStorage{}
	db, err := bolt.OpenStorage(s, nil)
	require.NoError(t, err)

	// Write about 4 MB in batches so the heap buffer remaps while committed
	// pages are read back.
	value := bytes.Repeat([]byte{'v'}, 400)
	for batch := range 20 {
		err := db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("b"))
			if err != nil {
				return err
			}
			for i := range 500 {
				key := fmt.Appendf(nil, "%04d-%04d", batch, i)
				if err := b.Put(key, value); err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)
	}
	require.Positive(t, s.syncs)
	require.NoError(t, db.Close())
	require.True(t, s.closed)

	// Reopen from the stored bytes and check every key and the tree.
	s.closed = false
	db, err = bolt.OpenStorage(s, nil)
	require.NoError(t, err)
	defer db.Close()
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("b"))
		require.NotNil(t, b)
		require.Equal(t, 20*500, b.Stats().KeyN)
		require.Equal(t, value, b.Get([]byte("0019-0499")))
		for err := range tx.Check() {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	// WriteTo copies the database from the Storage.
	var copied bytes.Buffer
	err = db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(&copied)
		return err
	})
	require.NoError(t, err)
	copyDB, err := bolt.OpenStorage(&memStorage{data: copied.Bytes()}, nil)
	require.NoError(t, err)
	defer copyDB.Close()
	err = copyDB.View(func(tx *bolt.Tx) error {
		require.Equal(t, value, tx.Bucket([]byte("b")).Get([]byte("0000-0000")))
		return nil
	})
	require.NoError(t, err)
}
