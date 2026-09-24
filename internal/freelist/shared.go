package freelist

import (
	"fmt"
	"math"
	"sort"

	"github.com/aperturerobotics/bbolt/internal/common"
)

type txPending struct {
	ids              []common.Pgid
	alloctx          []common.Txid // txids allocating the ids
	lastReleaseBegin common.Txid   // beginning txid of last matching releaseRange
}

type shared struct {
	Interface

	readonlyTXIDs map[common.Txid]int         // refcount of readonly transaction IDs.
	allocs        map[common.Pgid]common.Txid // mapping of Txid that allocated a pgid.
	cache         map[common.Pgid]struct{}    // fast lookup of all free and pending page ids.
	pending       map[common.Txid]*txPending  // mapping of soon-to-be free page ids by tx.
}

func newShared() *shared {
	return &shared{
		pending: make(map[common.Txid]*txPending),
		allocs:  make(map[common.Pgid]common.Txid),
		cache:   make(map[common.Pgid]struct{}),
	}
}

func (t *shared) pendingPageIds() map[common.Txid]*txPending {
	return t.pending
}

func (t *shared) PendingCount() int {
	var count int
	for _, txp := range t.pending {
		count += len(txp.ids)
	}
	return count
}

func (t *shared) Count() int {
	return t.FreeCount() + t.PendingCount()
}

func (t *shared) Freed(pgId common.Pgid) bool {
	_, ok := t.cache[pgId]
	return ok
}

func (t *shared) Free(txid common.Txid, p *common.Page) {
	if p.Id() <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.Id()))
	}

	// Free page and all its overflow pages.
	txp := t.pending[txid]
	if txp == nil {
		txp = &txPending{}
		t.pending[txid] = txp
	}
	allocTxid, ok := t.allocs[p.Id()]
	common.Verify(func() {
		if allocTxid == txid {
			panic(fmt.Sprintf("free: freed page (%d) was allocated by the same transaction (%d)", p.Id(), txid))
		}
	})
	if ok {
		delete(t.allocs, p.Id())
	}

	for id := p.Id(); id <= p.Id()+common.Pgid(p.Overflow()); id++ {
		// Verify that page is not already free.
		if _, ok := t.cache[id]; ok {
			panic(fmt.Sprintf("page %d already freed", id))
		}
		// Add to the freelist and cache.
		txp.ids = append(txp.ids, id)
		txp.alloctx = append(txp.alloctx, allocTxid)
		t.cache[id] = struct{}{}
	}
}

func (t *shared) Rollback(txid common.Txid) {
	// Remove page ids from cache.
	txp := t.pending[txid]
	if txp == nil {
		return
	}
	for i, pgid := range txp.ids {
		delete(t.cache, pgid)
		tx := txp.alloctx[i]
		if tx == 0 {
			continue
		}
		if tx != txid {
			// Pending free aborted; restore page back to alloc list.
			t.allocs[pgid] = tx
		} else {
			// A writing TXN should never free a page which was allocated by itself.
			panic(fmt.Sprintf("rollback: freed page (%d) was allocated by the same transaction (%d)", pgid, txid))
		}
	}
	// Remove pages from pending list and mark as free if allocated by txid.
	delete(t.pending, txid)

	// Remove pgids which are allocated by this txid
	for pgid, tid := range t.allocs {
		if tid == txid {
			delete(t.allocs, pgid)
		}
	}
}

func (t *shared) AddReadonlyTXID(tid common.Txid) {
	if t.readonlyTXIDs == nil {
		t.readonlyTXIDs = make(map[common.Txid]int)
	}
	t.readonlyTXIDs[tid]++
}

func (t *shared) RemoveReadonlyTXID(tid common.Txid) {
	if t.readonlyTXIDs == nil {
		return
	}
	if t.readonlyTXIDs[tid] <= 1 {
		delete(t.readonlyTXIDs, tid)
	} else {
		t.readonlyTXIDs[tid]--
	}
}

type txIDx []common.Txid

func (t txIDx) Len() int           { return len(t) }
func (t txIDx) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t txIDx) Less(i, j int) bool { return t[i] < t[j] }

func (t *shared) ReleasePendingPages() {
	// Build a sorted unique list of readonly transaction IDs.
	sortedTXIDs := make(txIDx, 0, len(t.readonlyTXIDs))
	for tid := range t.readonlyTXIDs {
		sortedTXIDs = append(sortedTXIDs, tid)
	}
	sort.Sort(sortedTXIDs)

	// Free all pending pages prior to the earliest open transaction.
	maxTxid := common.Txid(math.MaxUint64)
	minid := maxTxid
	if len(sortedTXIDs) > 0 {
		minid = sortedTXIDs[0]
	}
	if minid > 0 {
		t.release(minid - 1)
	}
	// Release unused txid extents.
	for _, tid := range sortedTXIDs {
		if minid < tid {
			t.releaseRange(minid, tid-1)
		}
		if tid == maxTxid {
			return
		}
		minid = tid + 1
	}
	t.releaseRange(minid, maxTxid)
	// Any page both allocated and freed in an extent is safe to release.
}

func (t *shared) DeferFreePages(txid common.Txid) {
	ids := append(common.Pgids(nil), t.freePageIds()...)
	if len(ids) == 0 {
		return
	}
	txp := t.pending[txid]
	if txp == nil {
		txp = &txPending{}
		t.pending[txid] = txp
	}
	txp.ids = append(txp.ids, ids...)
	txp.alloctx = append(txp.alloctx, make([]common.Txid, len(ids))...)
	t.Init(nil)
}

func (t *shared) release(txid common.Txid) {
	m := make(common.Pgids, 0)
	for tid, txp := range t.pending {
		if tid <= txid {
			// Move transaction's pending pages to the available freelist.
			// Don't remove from the cache since the page is still free.
			m = append(m, txp.ids...)
			delete(t.pending, tid)
		}
	}
	t.mergeSpans(m)
}

func (t *shared) releaseRange(begin, end common.Txid) {
	if begin > end {
		return
	}
	m := common.Pgids{}
	for tid, txp := range t.pending {
		if tid < begin || tid > end {
			continue
		}
		// Don't recompute freed pages if ranges haven't updated.
		if txp.lastReleaseBegin == begin {
			continue
		}
		for i := 0; i < len(txp.ids); i++ {
			if atx := txp.alloctx[i]; atx < begin || atx > end {
				continue
			}
			m = append(m, txp.ids[i])
			txp.ids[i] = txp.ids[len(txp.ids)-1]
			txp.ids = txp.ids[:len(txp.ids)-1]
			txp.alloctx[i] = txp.alloctx[len(txp.alloctx)-1]
			txp.alloctx = txp.alloctx[:len(txp.alloctx)-1]
			i--
		}
		txp.lastReleaseBegin = begin
		if len(txp.ids) == 0 {
			delete(t.pending, tid)
		}
	}
	t.mergeSpans(m)
}

// Copyall copies a list of all free ids and all pending ids in one sorted list.
// f.count returns the minimum length required for dst.
func (t *shared) Copyall(dst []common.Pgid) {
	common.Mergepgids(dst, t.freePageIds(), t.sortedPendingIds())
}

// sortedPendingIds returns the ids of all pending pages in ascending order.
func (t *shared) sortedPendingIds() common.Pgids {
	ids := make(common.Pgids, 0, t.PendingCount())
	for _, txp := range t.pending {
		ids = append(ids, txp.ids...)
	}
	sort.Sort(ids)
	return ids
}

func (t *shared) Reload(p *common.Page) {
	t.Read(p)
	t.NoSyncReload(t.freePageIds())
}

func (t *shared) NoSyncReload(pgIds common.Pgids) {
	// Build a cache of only pending pages.
	pcache := make(map[common.Pgid]struct{})
	for _, txp := range t.pending {
		for _, pendingID := range txp.ids {
			pcache[pendingID] = struct{}{}
		}
	}

	// Check each page in the freelist and build a new available freelist
	// with any pages not in the pending lists.
	a := []common.Pgid{}
	for _, id := range pgIds {
		if _, ok := pcache[id]; !ok {
			a = append(a, id)
		}
	}

	t.Init(a)
}

// reindex rebuilds the free cache based on available and pending free lists.
func (t *shared) reindex() {
	free := t.freePageIds()
	pending := t.pendingPageIds()
	t.cache = make(map[common.Pgid]struct{}, len(free))
	for _, id := range free {
		t.cache[id] = struct{}{}
	}
	for _, txp := range pending {
		for _, pendingID := range txp.ids {
			t.cache[pendingID] = struct{}{}
		}
	}
}

func (t *shared) Read(p *common.Page) {
	if !p.IsFreelistPage() {
		panic(fmt.Sprintf("invalid freelist page: %d, page type is %s", p.Id(), p.Typ()))
	}
	t.initSpans(p.FreelistPageSpans())
}

func (t *shared) EstimatedWritePageSize() int {
	// Every pending page adds at most one span.
	return common.FreelistPageSize(t.freeSpanCount() + t.PendingCount())
}

// Write stores the free and pending pages as one sorted list of spans.
// Pending pages are included: after a reopen no transaction can still read
// them, and a process that reloads the page defers free pages for its own
// active readers.
func (t *shared) Write(p *common.Page) {
	free := t.freeSpans()
	pending := t.sortedPendingIds()
	spans := make([]common.FreelistSpan, 0, len(free)+len(pending))
	var next int
	for _, span := range free {
		for ; next < len(pending) && pending[next] < span.Start; next++ {
			spans = appendSpan(spans, common.FreelistSpan{Start: pending[next], Len: 1})
		}
		spans = appendSpan(spans, span)
	}
	for _, id := range pending[next:] {
		spans = appendSpan(spans, common.FreelistSpan{Start: id, Len: 1})
	}
	p.WriteFreelistPage(spans)
}

// appendSpan appends span to spans, extending the last span when span
// continues it. Spans must be appended in ascending order.
func appendSpan(spans []common.FreelistSpan, span common.FreelistSpan) []common.FreelistSpan {
	if n := len(spans); n != 0 && spans[n-1].End() == span.Start {
		spans[n-1].Len += span.Len
		return spans
	}
	return append(spans, span)
}

// idSpans returns the spans covering the sorted page ids.
func idSpans(ids common.Pgids) []common.FreelistSpan {
	var spans []common.FreelistSpan
	for _, id := range ids {
		spans = appendSpan(spans, common.FreelistSpan{Start: id, Len: 1})
	}
	return spans
}
