package bbolt

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// openTestLockFileN creates a temporary lock file with a custom maxReaders
// count suitable for testing. Uses t.Cleanup for automatic teardown.
func openTestLockFileN(t *testing.T, maxReaders int) *LockFile {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db-lock")
	lf, err := openLockFile(path, maxReaders)
	if err != nil {
		t.Fatalf("openLockFile: %v", err)
	}
	t.Cleanup(func() { lf.Close() })
	return lf
}

func TestAcquireReleaseReaderSlot(t *testing.T) {
	lf := openTestLockFileN(t, 4)

	pid := uint32(os.Getpid())

	// Acquire a slot.
	slot, err := lf.AcquireReaderSlot()
	if err != nil {
		t.Fatalf("AcquireReaderSlot: %v", err)
	}
	if slot < 0 || slot >= 4 {
		t.Fatalf("unexpected slot index: %d", slot)
	}

	// Verify pid is set.
	got := atomic.LoadUint32(lf.slotPidPtr(slot))
	if got != pid {
		t.Errorf("slot pid = %d, want %d", got, pid)
	}

	// Verify txid is idle.
	txid := atomic.LoadUint64(lf.slotTxidPtr(slot))
	if txid != txidIdle {
		t.Errorf("slot txid = %d, want txidIdle (%d)", txid, txidIdle)
	}

	// Release the slot.
	lf.ReleaseReaderSlot(slot)

	// Verify pid is cleared.
	got = atomic.LoadUint32(lf.slotPidPtr(slot))
	if got != 0 {
		t.Errorf("after release: slot pid = %d, want 0", got)
	}
}

func TestMultipleReaderSlots(t *testing.T) {
	lf := openTestLockFileN(t, 8)

	pid := uint32(os.Getpid())
	slots := make([]int, 5)

	// Acquire 5 slots.
	for i := range slots {
		slot, err := lf.AcquireReaderSlot()
		if err != nil {
			t.Fatalf("AcquireReaderSlot[%d]: %v", i, err)
		}
		slots[i] = slot
	}

	// Verify all have the correct pid and unique indices.
	seen := make(map[int]bool)
	for i, slot := range slots {
		if seen[slot] {
			t.Fatalf("duplicate slot index: %d", slot)
		}
		seen[slot] = true

		got := atomic.LoadUint32(lf.slotPidPtr(slot))
		if got != pid {
			t.Errorf("slot[%d] pid = %d, want %d", i, got, pid)
		}
	}

	// Release all.
	for _, slot := range slots {
		lf.ReleaseReaderSlot(slot)
	}
}

func TestReaderSlotFull(t *testing.T) {
	maxReaders := 3
	lf := openTestLockFileN(t, maxReaders)

	// Fill all slots.
	for i := 0; i < maxReaders; i++ {
		_, err := lf.AcquireReaderSlot()
		if err != nil {
			t.Fatalf("AcquireReaderSlot[%d]: %v", i, err)
		}
	}

	// Next acquire should fail.
	_, err := lf.AcquireReaderSlot()
	if err != ErrReaderTableFull {
		t.Fatalf("expected ErrReaderTableFull, got: %v", err)
	}
}

func TestSetClearTxid(t *testing.T) {
	lf := openTestLockFileN(t, 4)

	slot, err := lf.AcquireReaderSlot()
	if err != nil {
		t.Fatalf("AcquireReaderSlot: %v", err)
	}
	defer lf.ReleaseReaderSlot(slot)

	// Initially idle.
	txid := atomic.LoadUint64(lf.slotTxidPtr(slot))
	if txid != txidIdle {
		t.Errorf("initial txid = %d, want txidIdle", txid)
	}

	// Set a txid.
	lf.SetSlotTxid(slot, 42)
	txid = atomic.LoadUint64(lf.slotTxidPtr(slot))
	if txid != 42 {
		t.Errorf("after SetSlotTxid: txid = %d, want 42", txid)
	}

	// Clear txid back to idle.
	lf.ClearSlotTxid(slot)
	txid = atomic.LoadUint64(lf.slotTxidPtr(slot))
	if txid != txidIdle {
		t.Errorf("after ClearSlotTxid: txid = %d, want txidIdle", txid)
	}
}

func TestOldestReaderTxid(t *testing.T) {
	lf := openTestLockFileN(t, 8)

	// No readers: should return fallback.
	fallback := uint64(100)
	got := lf.OldestReaderTxid(fallback)
	if got != fallback {
		t.Errorf("no readers: OldestReaderTxid = %d, want %d", got, fallback)
	}

	// Acquire slots and set various txids.
	slot1, _ := lf.AcquireReaderSlot()
	slot2, _ := lf.AcquireReaderSlot()
	slot3, _ := lf.AcquireReaderSlot()
	defer lf.ReleaseReaderSlot(slot1)
	defer lf.ReleaseReaderSlot(slot2)
	defer lf.ReleaseReaderSlot(slot3)

	// Slot1: idle (no active tx)
	// Slot2: txid 50
	// Slot3: txid 30
	lf.SetSlotTxid(slot2, 50)
	lf.SetSlotTxid(slot3, 30)

	got = lf.OldestReaderTxid(fallback)
	if got != 30 {
		t.Errorf("OldestReaderTxid = %d, want 30", got)
	}

	// Clear slot3, now slot2 is the only active reader.
	lf.ClearSlotTxid(slot3)
	got = lf.OldestReaderTxid(fallback)
	if got != 50 {
		t.Errorf("after clearing slot3: OldestReaderTxid = %d, want 50", got)
	}

	// Clear slot2, no active readers. Should return fallback.
	lf.ClearSlotTxid(slot2)
	got = lf.OldestReaderTxid(fallback)
	if got != fallback {
		t.Errorf("all idle: OldestReaderTxid = %d, want %d", got, fallback)
	}
}

func TestOldestReaderTxidWithZeroTxid(t *testing.T) {
	lf := openTestLockFileN(t, 4)

	slot, _ := lf.AcquireReaderSlot()
	defer lf.ReleaseReaderSlot(slot)

	// A reader at txid 0 is valid and should be the minimum.
	lf.SetSlotTxid(slot, 0)

	got := lf.OldestReaderTxid(100)
	if got != 0 {
		t.Errorf("OldestReaderTxid = %d, want 0", got)
	}
}

func TestReaderSlotReuse(t *testing.T) {
	lf := openTestLockFileN(t, 4)

	// Acquire and release a slot.
	slot1, _ := lf.AcquireReaderSlot()
	lf.ReleaseReaderSlot(slot1)

	// Acquire again: should be able to reuse the same slot.
	slot2, err := lf.AcquireReaderSlot()
	if err != nil {
		t.Fatalf("AcquireReaderSlot after release: %v", err)
	}
	// The slot index may or may not be the same, but we should succeed.
	_ = slot2
	lf.ReleaseReaderSlot(slot2)
}

func TestClearStaleReaders(t *testing.T) {
	lf := openTestLockFileN(t, 8)

	// We can't easily spawn and kill a process in a unit test.
	// Instead, simulate a stale reader by directly writing a bogus pid
	// into a slot. PID 1 (init/launchd) will always be alive so we
	// use a very large pid that almost certainly does not exist.
	//
	// NOTE: This test checks the mechanics of ClearStaleReaders but
	// cannot fully verify cross-process stale detection without a
	// subprocess harness (see Step 5 multi-process tests).

	stalePid := uint32(4000000) // unlikely to exist
	staleSlot := 2

	// Manually write a stale reader into the slot.
	atomic.StoreUint32(lf.slotPidPtr(staleSlot), stalePid)
	atomic.StoreUint64(lf.slotTxidPtr(staleSlot), 10)

	// Verify it's there.
	if atomic.LoadUint32(lf.slotPidPtr(staleSlot)) != stalePid {
		t.Fatal("failed to set stale pid")
	}

	// Clear stale readers.
	cleared := lf.ClearStaleReaders()
	if cleared < 1 {
		t.Errorf("ClearStaleReaders = %d, want >= 1", cleared)
	}

	// Verify the stale slot was cleared.
	pid := atomic.LoadUint32(lf.slotPidPtr(staleSlot))
	if pid != 0 {
		t.Errorf("stale slot pid = %d, want 0", pid)
	}
	txid := atomic.LoadUint64(lf.slotTxidPtr(staleSlot))
	if txid != txidIdle {
		t.Errorf("stale slot txid = %d, want txidIdle", txid)
	}
}

func TestClearStaleReadersPreservesLiveSlots(t *testing.T) {
	lf := openTestLockFileN(t, 8)

	// Acquire a real slot (our process is alive).
	slot, _ := lf.AcquireReaderSlot()
	lf.SetSlotTxid(slot, 42)

	// ClearStaleReaders should not touch our slot.
	cleared := lf.ClearStaleReaders()
	if cleared != 0 {
		t.Errorf("ClearStaleReaders = %d, want 0 (no stale)", cleared)
	}

	pid := atomic.LoadUint32(lf.slotPidPtr(slot))
	if pid != uint32(os.Getpid()) {
		t.Errorf("live slot pid = %d, want %d", pid, os.Getpid())
	}
	txid := atomic.LoadUint64(lf.slotTxidPtr(slot))
	if txid != 42 {
		t.Errorf("live slot txid = %d, want 42", txid)
	}

	lf.ReleaseReaderSlot(slot)
}

func TestOldestReaderTxidClearsStale(t *testing.T) {
	lf := openTestLockFileN(t, 8)

	// Insert a stale reader with a very old txid.
	stalePid := uint32(4000001)
	atomic.StoreUint32(lf.slotPidPtr(0), stalePid)
	atomic.StoreUint64(lf.slotTxidPtr(0), 5)

	// Acquire a real slot with a higher txid.
	slot, _ := lf.AcquireReaderSlot()
	lf.SetSlotTxid(slot, 50)
	defer lf.ReleaseReaderSlot(slot)

	// OldestReaderTxid should clear the stale reader first, then return 50.
	got := lf.OldestReaderTxid(100)
	if got != 50 {
		t.Errorf("OldestReaderTxid = %d, want 50 (stale reader should be cleared)", got)
	}

	// Verify the stale slot was cleared.
	pid := atomic.LoadUint32(lf.slotPidPtr(0))
	if pid != 0 {
		t.Errorf("stale slot pid = %d after OldestReaderTxid, want 0", pid)
	}
}
