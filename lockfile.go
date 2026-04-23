package bbolt

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	berrors "github.com/aperturerobotics/bbolt/errors"
)

// processLockFiles tracks how many LockFile instances are open per path
// within this process. Used by clearStaleWriterState to avoid probing
// the fcntl lock when another opener in the same process holds it
// (POSIX fcntl locks are per-process, not per-fd, so the probe would
// always succeed and incorrectly reset live state).
var (
	processLockFiles   = make(map[string]int)
	processLockFilesMu sync.Mutex
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
	magic      uint32
	version    uint32
	maxReaders uint32
	numReaders uint32
	// writerCount is the number of writer-capable openers (atomic).
	// Unlike reader slots which use PID-based detection, writerCount
	// is just a counter. Stale counts from crashed processes are
	// recovered by clearStaleWriterState during openLockFile, which
	// probes the fcntl writer lock to determine liveness.
	writerCount   uint32
	accessMode    uint32   // adaptive access mode (atomic), see accessMode* constants
	commitCounter uint64   // atomic commit counter, incremented after each write tx commit
	_             [32]byte // padding to 64 bytes
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

// Compile-time size assertions: if sizes don't match, these const
// declarations cause an overflow to negative uint, triggering a compile error.
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

// Path returns the filesystem path of the lock file.
func (lf *LockFile) Path() string {
	return lf.path
}

// ValidatePath returns an error if the lock file path no longer refers to the
// same open coordination file descriptor.
func (lf *LockFile) ValidatePath() error {
	if lf == nil || lf.fd == nil {
		return nil
	}

	fdInfo, err := lf.fd.Stat()
	if err != nil {
		return fmt.Errorf("bbolt: stat open lock file (%s): %w", lf.path, err)
	}
	pathInfo, err := os.Stat(lf.path)
	if err != nil {
		return fmt.Errorf("%w: stat lock file path (%s): %v", berrors.ErrLockFileChanged, lf.path, err)
	}
	if os.SameFile(fdInfo, pathInfo) {
		return nil
	}

	return fmt.Errorf("%w: lock file path (%s) no longer matches the open coordination file", berrors.ErrLockFileChanged, lf.path)
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

	// Use the magic field as the publish marker for the header. Multiple
	// processes/goroutines may race to initialize a new lock file, so serialize
	// the zero-magic path through the writer lock and publish magic last.
	h := lf.header()
	magicPtr := (*uint32)(unsafe.Pointer(&lf.data[0]))
	if atomic.LoadUint32(magicPtr) == 0 {
		if err := lf.AcquireWriterLock(); err != nil {
			lf.Close()
			return nil, err
		}
		if atomic.LoadUint32(magicPtr) == 0 {
			atomic.StoreUint32(&h.version, lockFileVersion)
			atomic.StoreUint32(&h.maxReaders, uint32(maxReaders))
			atomic.StoreUint32(&h.numReaders, 0)
			atomic.StoreUint32(magicPtr, lockFileMagic)
		}
		if err := lf.ReleaseWriterLock(); err != nil {
			lf.Close()
			return nil, err
		}
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

	// Clear stale writer state left by crashed processes, but only if
	// this is the first opener in the current process for this path.
	// POSIX fcntl locks are per-process (not per-fd), so the liveness
	// probe would always succeed when another opener in the same process
	// holds the lock, incorrectly resetting live state.
	processLockFilesMu.Lock()
	firstOpener := processLockFiles[path] == 0
	processLockFiles[path]++
	processLockFilesMu.Unlock()

	if firstOpener {
		lf.clearStaleWriterState()
	}

	return lf, nil
}

func openReadOnlyLockFile(path string, maxReaders int) (*LockFile, error) {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
			return nil, nil
		}
		return nil, fmt.Errorf("bbolt: stat read-only lock file: %w", err)
	}
	lf, err := openLockFile(path, maxReaders)
	if err == nil {
		return lf, nil
	}
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
		return nil, nil
	}
	return nil, err
}

// clearStaleWriterState resets writerCount and accessMode when no live
// writer process holds the fcntl lock. This recovers from process crashes
// that left the count elevated without decrementing.
//
// Uses TryAcquireWriterLock (non-blocking fcntl F_SETLK) as a liveness
// probe: if the lock is available, no other process is between its
// enterAccessMode and exitAccessMode calls, so the count is stale.
func (lf *LockFile) clearStaleWriterState() {
	h := lf.header()
	if atomic.LoadUint32(&h.writerCount) == 0 {
		return
	}
	lf.ClearStaleReaders()
	if lf.hasReaderSlots() {
		return
	}
	acquired, err := lf.TryAcquireWriterLock()
	if err != nil || !acquired {
		return // another writer is alive, count is accurate
	}
	// No writer holds the lock. Reset stale state.
	atomic.StoreUint32(&h.writerCount, 0)
	atomic.StoreUint32(&h.accessMode, accessModeAvailable)
	_ = lf.ReleaseWriterLock()
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

// commitCounterPtr returns a pointer to the commitCounter field in the mmap'd
// header for use with sync/atomic operations.
func (lf *LockFile) commitCounterPtr() *uint64 {
	return &lf.header().commitCounter
}

// CommitCounter returns the current commit counter value atomically.
// The counter is incremented after each successful write transaction commit.
// Readable by any process via atomic load on the shared mmap'd memory.
func (lf *LockFile) CommitCounter() uint64 {
	return atomic.LoadUint64(lf.commitCounterPtr())
}

// commitCounterNotifyOffset is a byte offset in the lock file header padding
// used solely to trigger fsnotify watchers in other processes. A pwrite to
// this offset after each atomic counter increment generates a filesystem
// write event without affecting any meaningful data.
const commitCounterNotifyOffset = 32

// IncrementCommitCounter atomically increments the commit counter and returns
// the new value. Also writes a byte to the lock file to trigger fsnotify
// watchers in other processes.
func (lf *LockFile) IncrementCommitCounter() uint64 {
	val := atomic.AddUint64(lf.commitCounterPtr(), 1)
	// Pwrite a byte to trigger fsnotify watchers. The offset is in the
	// padding region of the header and the value is meaningless.
	if lf.fd != nil {
		_, _ = lf.fd.WriteAt([]byte{byte(val)}, commitCounterNotifyOffset)
	}
	return val
}

// IncrementWriterCount atomically increments the writer-capable opener count
// and returns the new value.
func (lf *LockFile) IncrementWriterCount() uint32 {
	return atomic.AddUint32(&lf.header().writerCount, 1)
}

// DecrementWriterCount atomically decrements the writer-capable opener count
// and returns the new value. If the count is already 0, returns 0 without
// decrementing to prevent underflow.
func (lf *LockFile) DecrementWriterCount() uint32 {
	ptr := &lf.header().writerCount
	for {
		current := atomic.LoadUint32(ptr)
		if current == 0 {
			return 0
		}
		if atomic.CompareAndSwapUint32(ptr, current, current-1) {
			return current - 1
		}
	}
}

// Close unmaps the lock file data and closes the file descriptor.
func (lf *LockFile) Close() error {
	processLockFilesMu.Lock()
	if n := processLockFiles[lf.path]; n <= 1 {
		delete(processLockFiles, lf.path)
	} else {
		processLockFiles[lf.path] = n - 1
	}
	processLockFilesMu.Unlock()

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
