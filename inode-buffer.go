package bbolt

import (
	"sync"

	"github.com/aperturerobotics/bbolt/internal/common"
)

// inodeBuffer holds transaction-local page entries. Nodes may split or share
// slices of this buffer, so only closing the transaction returns it for reuse.
type inodeBuffer struct {
	entries [4096]common.Inode
}

var inodeBuffers = sync.Pool{New: func() any { return new(inodeBuffer) }}

// allocInodes reserves a disjoint array within this transaction's buffers.
// Large arrays retain ordinary allocation rather than growing pooled buffers.
func (tx *Tx) allocInodes(length, capacity int) common.Inodes {
	if capacity > len(inodeBuffer{}.entries) {
		return make(common.Inodes, length, capacity)
	}
	if len(tx.inodeTail) < capacity {
		buffer := inodeBuffers.Get().(*inodeBuffer)
		tx.inodeBuffers = append(tx.inodeBuffers, buffer)
		tx.inodeTail = buffer.entries[:]
	}
	entries := tx.inodeTail[:length:capacity]
	tx.inodeTail = tx.inodeTail[capacity:]
	return entries
}

// releaseInodes clears every pointer before buffers outlive their transaction.
// This releases references to mapped pages and caller-owned key/value bytes.
func (tx *Tx) releaseInodes() {
	for _, buffer := range tx.inodeBuffers {
		clear(buffer.entries[:])
		inodeBuffers.Put(buffer)
	}
	tx.inodeBuffers = nil
	tx.inodeTail = nil
}

// growInodes retains spare capacity for later inserts into a mutable page.
func (n *node) growInodes(length int) {
	if length <= cap(n.inodes) {
		return
	}
	capacity := max(length, 2*cap(n.inodes), 16)
	var entries common.Inodes
	if n.bucket != nil && n.bucket.tx != nil {
		entries = n.bucket.tx.allocInodes(len(n.inodes), capacity)
	} else {
		entries = make(common.Inodes, len(n.inodes), capacity)
	}
	copy(entries, n.inodes)
	n.inodes = entries
}
