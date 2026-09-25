package bbolt

import (
	"io"
	"os"
	"unsafe"

	"github.com/aperturerobotics/bbolt/internal/common"
)

// Storage is a data file the DB reads and writes through positional I/O
// instead of an operating system handle. See OpenStorage.
type Storage interface {
	io.ReaderAt
	io.WriterAt
	// Size returns the file length.
	Size() (int64, error)
	// Truncate sets the file length.
	Truncate(size int64) error
	// Sync makes every earlier write durable.
	Sync() error
	// Close releases the file.
	Close() error
}

// fileStorage is the Storage of a database opened by path.
type fileStorage struct {
	*os.File
}

// Size returns the file length.
func (f fileStorage) Size() (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// mapHeap stands in for mmap on a Storage: it holds the data file in a heap
// buffer of sz bytes. A remap copies prev, which every write keeps current;
// the first map reads the file.
func (db *DB) mapHeap(sz int, prev []byte) error {
	buf := make([]byte, sz)
	copy(buf, prev)
	if prev == nil {
		size, err := db.storage.Size()
		if err != nil {
			return err
		}
		if _, err := db.storage.ReadAt(buf[:min(int(size), sz)], 0); err != nil {
			return err
		}
	}
	db.dataref = buf
	db.data = (*[common.MaxMapSize]byte)(unsafe.Pointer(&buf[0]))
	db.datasz = sz
	return nil
}

// writeHeap writes to the Storage and to the heap buffer mapping it, as a
// write to a file shows through its shared memory map.
func (db *DB) writeHeap(b []byte, off int64) (int, error) {
	n, err := db.storage.WriteAt(b, off)
	if off < int64(len(db.dataref)) {
		copy(db.dataref[off:], b[:n])
	}
	return n, err
}
