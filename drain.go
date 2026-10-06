package bbolt

import (
	"github.com/aperturerobotics/bbolt/internal/common"
)

// drainTail moves up to db.TailDrainPages live pages at the end of the file
// into free space below them. Allocation takes the lowest free span that fits,
// so spilling a node relocates it; once the run at the end of the file is free,
// commitFreelist trims it and the file shrinks.
//
// The run starts one past the highest free or pending page and is drained from
// its first page, so each commit extends the free span below it. A page is
// moved by materializing the nodes on the path to it, which spill rewrites.
// Pages are found by searching the root bucket and its top-level buckets for
// the first key on the page; the drain stops at a page of a deeper bucket.
func (tx *Tx) drainTail() {
	budget := tx.db.TailDrainPages
	if budget <= 0 || tx.db.NoFreelistSync {
		return
	}

	// Drain only when the free pages can hold the whole run.
	hwm := tx.meta.Pgid()
	start := tx.db.freelist.LastFreed() + 1
	if start == 1 || int(hwm-start) > tx.db.freelist.FreeCount() {
		return
	}

	// Relocate each node in the run while free space below the run holds it.
	var buckets []*Bucket
	for id := start; id < hwm && budget > 0; {
		p := tx.page(id)
		n := int(p.Overflow()) + 1
		if p.IsBranchPage() || p.IsLeafPage() {
			if !tx.db.freelist.AllocatesBelow(n, start) {
				return
			}
			if !tx.relocate(&buckets, p) {
				return
			}
		}
		id += common.Pgid(n)
		budget -= n
	}
}

// relocate materializes the nodes on the path to the branch or leaf page p so
// spill rewrites them, and reports whether a searched bucket holds p. It loads
// the top-level buckets into buckets on first use.
func (tx *Tx) relocate(buckets *[]*Bucket, p *common.Page) bool {
	// Search the root bucket before loading the top-level buckets.
	if tx.root.materializePage(p) {
		return true
	}
	if *buckets == nil {
		*buckets = []*Bucket{}
		_ = tx.root.ForEachBucket(func(name []byte) error {
			*buckets = append(*buckets, tx.root.Bucket(name))
			return nil
		})
	}
	for _, b := range *buckets {
		if b.materializePage(p) {
			return true
		}
	}
	return false
}

// materializePage materializes the nodes on the path to page p when the bucket
// holds p, and reports whether it does.
func (b *Bucket) materializePage(p *common.Page) bool {
	// An inline bucket has no pages; an empty root page has no key to search.
	root := b.RootPage()
	if root == 0 {
		return false
	}
	if root == p.Id() {
		b.node(root, nil)
		return true
	}
	if p.Count() == 0 {
		return false
	}

	// Search for the first key on the page and check that the path passes it.
	key := p.LeafPageElement(0).Key()
	if p.IsBranchPage() {
		key = p.BranchPageElement(0).Key()
	}
	c := b.Cursor()
	c.search(key, root)
	for _, ref := range c.stack {
		if ref.page != nil && ref.page.Id() == p.Id() || ref.node != nil && ref.node.pgid == p.Id() {
			c.node()
			return true
		}
	}
	return false
}
