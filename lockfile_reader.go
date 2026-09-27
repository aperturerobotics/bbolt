package bbolt

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

// txidIdle is the sentinel value stored in a reader slot's txid field
// when the slot is owned but no transaction is active. Using MAX_UINT64
// ensures it is always greater than any real transaction ID and will
// never be selected as the minimum in OldestReaderTxid.
const txidIdle = ^uint64(0)

// ErrReaderTableFull is returned when all reader slots are occupied.
var ErrReaderTableFull = errors.New("bbolt: reader table full")

// slotPidPtr returns a pointer to the pid field of the given reader slot
// in the mmap'd lock file data. The pointer is suitable for use with
// sync/atomic operations.
func (lf *LockFile) slotPidPtr(slot int) *uint32 {
	off := readerTableOffset + slot*readerSlotSize
	return (*uint32)(unsafe.Pointer(&lf.data[off]))
}

// slotTxidPtr returns a pointer to the txid field of the given reader
// slot in the mmap'd lock file data. The txid field starts at offset 8
// within the slot (after pid uint32 + reserved uint32).
func (lf *LockFile) slotTxidPtr(slot int) *uint64 {
	off := readerTableOffset + slot*readerSlotSize + 8
	return (*uint64)(unsafe.Pointer(&lf.data[off]))
}

// slotStartPtr returns a pointer to the owner start field of the given
// reader slot. The field starts at offset 16 within the slot and holds the
// owning process's start time, or zero when the platform cannot report it.
func (lf *LockFile) slotStartPtr(slot int) *uint64 {
	off := readerTableOffset + slot*readerSlotSize + 16
	return (*uint64)(unsafe.Pointer(&lf.data[off]))
}

// selfStart is this process's start time, as recorded in its reader slots.
var selfStart = sync.OnceValue(func() uint64 {
	return processStart(uint32(os.Getpid()))
})

// AcquireReaderSlot claims an empty reader slot in the lock file using
// atomic compare-and-swap. The slot's pid is set to the current process
// ID, the owner start field to this process's start time, and the txid is
// initialized to txidIdle.
//
// Returns the slot index on success. Returns ErrReaderTableFull if all
// slots are occupied.
func (lf *LockFile) AcquireReaderSlot() (int, error) {
	pid := uint32(os.Getpid())
	for i := 0; i < lf.maxReaders; i++ {
		ptr := lf.slotPidPtr(i)
		if atomic.CompareAndSwapUint32(ptr, 0, pid) {
			// Slot claimed. Mark txid as idle (no active transaction).
			atomic.StoreUint64(lf.slotTxidPtr(i), txidIdle)
			atomic.StoreUint64(lf.slotStartPtr(i), selfStart())
			return i, nil
		}
	}
	return -1, ErrReaderTableFull
}

// ReleaseReaderSlot releases a previously acquired reader slot by
// clearing its pid field. The caller must ensure no transaction is
// active on the slot before releasing it.
func (lf *LockFile) ReleaseReaderSlot(slot int) {
	atomic.StoreUint64(lf.slotTxidPtr(slot), txidIdle)
	atomic.StoreUint64(lf.slotStartPtr(slot), 0)
	atomic.StoreUint32(lf.slotPidPtr(slot), 0)
}

// SetSlotTxid sets the transaction ID for a reader slot. This is called
// when beginning a read transaction to register the snapshot txid with
// other processes.
func (lf *LockFile) SetSlotTxid(slot int, txid uint64) {
	atomic.StoreUint64(lf.slotTxidPtr(slot), txid)
}

// ClearSlotTxid marks a reader slot as idle by storing the txidIdle
// sentinel value. This is called when ending a read transaction. The
// slot remains owned by this process.
func (lf *LockFile) ClearSlotTxid(slot int) {
	atomic.StoreUint64(lf.slotTxidPtr(slot), txidIdle)
}

// OldestReaderTxid returns the minimum active transaction ID across all
// reader slots. If no readers have an active transaction, the provided
// fallback value is returned (typically the current meta txid).
//
// Stale readers (dead processes) are cleared before scanning.
func (lf *LockFile) OldestReaderTxid(fallback uint64) uint64 {
	lf.ClearStaleReaders()

	min := fallback
	for i := 0; i < lf.maxReaders; i++ {
		pid := atomic.LoadUint32(lf.slotPidPtr(i))
		if pid == 0 {
			continue
		}
		txid := atomic.LoadUint64(lf.slotTxidPtr(i))
		if txid == txidIdle {
			continue
		}
		if txid < min {
			min = txid
		}
	}
	return min
}

// ClearStaleReaders checks all occupied reader slots for dead processes
// and clears them. A slot is stale when its process no longer exists, or
// when a live process with its pid started at a different time than the
// slot's owner. The start check matters where pids repeat, such as a
// container whose server is always pid 1. Returns the number of stale
// slots cleared.
func (lf *LockFile) ClearStaleReaders() int {
	cleared := 0
	for i := 0; i < lf.maxReaders; i++ {
		pid := atomic.LoadUint32(lf.slotPidPtr(i))
		if pid == 0 {
			continue
		}
		if !processAlive(pid) || !sameOwner(pid, atomic.LoadUint64(lf.slotStartPtr(i))) {
			atomic.StoreUint64(lf.slotTxidPtr(i), txidIdle)
			atomic.StoreUint64(lf.slotStartPtr(i), 0)
			atomic.StoreUint32(lf.slotPidPtr(i), 0)
			cleared++
		}
	}
	return cleared
}

// sameOwner reports whether the live process pid is the one that recorded
// start in a reader slot. An unknown start on either side keeps the slot.
func sameOwner(pid uint32, start uint64) bool {
	if start == 0 {
		return true
	}
	current := processStart(pid)
	return current == 0 || current == start
}

func (lf *LockFile) hasReaderSlots() bool {
	for i := 0; i < lf.maxReaders; i++ {
		if atomic.LoadUint32(lf.slotPidPtr(i)) != 0 {
			return true
		}
	}
	return false
}
