package bbolt

import (
	"fmt"
	"os"
	"sync/atomic"
	"unsafe"
)

// Lock file magic number and version.
const (
	lockFileMagic   = 0xBB01D100
	lockFileVersion = 1
)

// Access mode constants for the adaptive single/multi-process state machine.
// Stored in the lockFileHeader.accessMode field (mmap'd, cross-process visible).
const (
	// accessModeAvailable means no writer-capable process has the DB open.
	accessModeAvailable uint32 = 0
	// accessModeSingle means exactly one writer-capable process has the DB open.
	// That process holds a persistent writer fcntl lock and skips per-tx coordination.
	accessModeSingle uint32 = 1
	// accessModeEscalating means a second writer-capable process has opened the DB
	// and is waiting for the first process to release its persistent writer lock.
	accessModeEscalating uint32 = 2
	// accessModeMulti means full multi-process coordination is active.
	// Per-tx fcntl locks, reader table ops, and refreshForWriter are all used.
	accessModeMulti uint32 = 3
)

// Lock file layout constants.
const (
	// lockFileHeaderSize is the size of the lock file header in bytes.
	lockFileHeaderSize = 64

	// writerLockRegionOffset is the byte offset of the writer lock region.
	writerLockRegionOffset = 64

	// writerLockRegionSize is the size of the writer lock region in bytes.
	writerLockRegionSize = 64

	// readerTableOffset is the byte offset where the reader table begins.
	readerTableOffset = 128

	// readerSlotSize is the size of each reader slot in bytes.
	// Aligned to cache line size to prevent false sharing.
	readerSlotSize = 64

	// defaultMaxReaders is the default number of reader slots.
	defaultMaxReaders = 126
)

// lockFileHeader is the on-disk/mmap header of the lock file.
// It occupies the first 64 bytes.
type lockFileHeader struct {
	magic       uint32
	version     uint32
	maxReaders  uint32
	numReaders  uint32
	writerCount uint32   // number of writer-capable openers (atomic)
	accessMode  uint32   // adaptive access mode (atomic), see accessMode* constants
	_           [40]byte // padding to 64 bytes
}

// readerSlot documents the memory layout of a single reader entry in
// the reader table. Fields are accessed via pointer arithmetic in
// lockfile_reader.go; this struct exists only for the compile-time
// size assertion below.
//
//nolint:unused
type readerSlot struct {
	pid  uint32   //nolint:unused
	_    uint32   //nolint:unused
	txid uint64   //nolint:unused
	_pad [48]byte //nolint:unused
}

// Compile-time assertions that struct sizes match the expected constants.
const _ = -uint(unsafe.Sizeof(lockFileHeader{}) - lockFileHeaderSize) // lockFileHeader must be 64 bytes
const _ = -uint(lockFileHeaderSize - unsafe.Sizeof(lockFileHeader{})) // lockFileHeader must be 64 bytes
const _ = -uint(unsafe.Sizeof(readerSlot{}) - readerSlotSize)         // readerSlot must be 64 bytes
const _ = -uint(readerSlotSize - unsafe.Sizeof(readerSlot{}))         // readerSlot must be 64 bytes

// lockFileSize returns the total size of a lock file with the given
// number of reader slots.
func lockFileSize(maxReaders int) int {
	return readerTableOffset + maxReaders*readerSlotSize
}

// LockFile represents a shared memory lock file used for cross-process
// coordination. It is memory-mapped MAP_SHARED so that atomic operations
// on the mapped bytes are visible across processes.
type LockFile struct {
	fd         *os.File
	data       []byte
	path       string
	maxReaders int
}

// MaxReaders returns the maximum number of reader slots.
func (lf *LockFile) MaxReaders() int {
	return lf.maxReaders
}

// header returns a pointer to the lockFileHeader in the mmap'd region.
func (lf *LockFile) header() *lockFileHeader {
	return (*lockFileHeader)(unsafe.Pointer(&lf.data[0]))
}

// openLockFile opens or creates a lock file at the given path.
// The file is mmap'd MAP_SHARED with PROT_READ|PROT_WRITE so that
// multiple processes can coordinate via shared memory.
//
// If the file does not exist it is created and initialized with the
// magic number, version, and max readers header. If it already exists,
// the header is validated.
func openLockFile(path string, maxReaders int) (*LockFile, error) {
	if maxReaders <= 0 {
		maxReaders = defaultMaxReaders
	}

	sz := lockFileSize(maxReaders)

	// Open or create the lock file.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("bbolt: open lock file: %w", err)
	}

	// Check current file size to determine if the file is new.
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("bbolt: stat lock file: %w", err)
	}

	// Grow the file if it's too small.
	if info.Size() < int64(sz) {
		if err := f.Truncate(int64(sz)); err != nil {
			f.Close()
			return nil, fmt.Errorf("bbolt: truncate lock file: %w", err)
		}
	}

	// mmap the file with MAP_SHARED + PROT_READ|PROT_WRITE.
	data, err := mmapLockFile(f, sz)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("bbolt: mmap lock file: %w", err)
	}

	lf := &LockFile{
		fd:         f,
		data:       data,
		path:       path,
		maxReaders: maxReaders,
	}

	// Use CAS on the magic field to safely handle concurrent openers.
	// Multiple processes/goroutines may race to create/open the lock
	// file. One will win the CAS(0 -> magic) and initialize the rest
	// of the header; others will see the magic already set.
	h := lf.header()
	magicPtr := (*uint32)(unsafe.Pointer(&lf.data[0]))
	if atomic.CompareAndSwapUint32(magicPtr, 0, lockFileMagic) {
		// We won the init race. Write the rest of the header.
		atomic.StoreUint32(&h.version, lockFileVersion)
		atomic.StoreUint32(&h.maxReaders, uint32(maxReaders))
		atomic.StoreUint32(&h.numReaders, 0)
	}

	// Validate the header. Read values before any Close could unmap.
	magic := atomic.LoadUint32(magicPtr)
	if magic != lockFileMagic {
		lf.Close()
		return nil, fmt.Errorf("bbolt: invalid lock file magic: %#x", magic)
	}
	version := atomic.LoadUint32(&h.version)
	if version != lockFileVersion {
		lf.Close()
		return nil, fmt.Errorf("bbolt: unsupported lock file version: %d", version)
	}

	return lf, nil
}

// accessModePtr returns a pointer to the accessMode field in the mmap'd header
// for use with sync/atomic operations.
func (lf *LockFile) accessModePtr() *uint32 {
	return &lf.header().accessMode
}

// AccessMode returns the current access mode atomically.
func (lf *LockFile) AccessMode() uint32 {
	return atomic.LoadUint32(lf.accessModePtr())
}

// CASAccessMode performs an atomic compare-and-swap on the access mode.
func (lf *LockFile) CASAccessMode(old, new uint32) bool {
	return atomic.CompareAndSwapUint32(lf.accessModePtr(), old, new)
}

// SetAccessMode stores the access mode atomically.
func (lf *LockFile) SetAccessMode(mode uint32) {
	atomic.StoreUint32(lf.accessModePtr(), mode)
}

// IncrementWriterCount atomically increments the writer-capable opener count
// and returns the new value.
func (lf *LockFile) IncrementWriterCount() uint32 {
	return atomic.AddUint32(&lf.header().writerCount, 1)
}

// DecrementWriterCount atomically decrements the writer-capable opener count
// and returns the new value.
func (lf *LockFile) DecrementWriterCount() uint32 {
	return atomic.AddUint32(&lf.header().writerCount, ^uint32(0))
}

// Close unmaps the lock file data and closes the file descriptor.
func (lf *LockFile) Close() error {
	var errs []error

	if lf.data != nil {
		if err := munmapLockFile(lf.data); err != nil {
			errs = append(errs, fmt.Errorf("bbolt: munmap lock file: %w", err))
		}
		lf.data = nil
	}

	if lf.fd != nil {
		if err := lf.fd.Close(); err != nil {
			errs = append(errs, fmt.Errorf("bbolt: close lock file: %w", err))
		}
		lf.fd = nil
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
