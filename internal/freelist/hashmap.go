package freelist

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/aperturerobotics/bbolt/internal/common"
)

// hashMap indexes free spans by their first and last page for merging, and
// orders them in a spanTree for lowest-first allocation.
type hashMap struct {
	*shared

	freePagesCount uint64                 // count of free pages
	spans          spanTree               // free spans ordered by start page
	forwardMap     map[common.Pgid]uint64 // key is start pgid, value is its span size
	backwardMap    map[common.Pgid]uint64 // key is end pgid, value is its span size
}

func (f *hashMap) Init(pgids common.Pgids) {
	if !sort.SliceIsSorted([]common.Pgid(pgids), func(i, j int) bool { return pgids[i] < pgids[j] }) {
		panic("pgids not sorted")
	}
	f.initSpans(idSpans(pgids))
}

func (f *hashMap) initSpans(spans []common.FreelistSpan) {
	f.freePagesCount = 0
	f.spans = spanTree{}
	f.forwardMap = make(map[common.Pgid]uint64, len(spans))
	f.backwardMap = make(map[common.Pgid]uint64, len(spans))
	for _, span := range spans {
		f.addSpan(span.Start, span.Len)
	}
	f.reindex()
}

// Allocate takes n contiguous pages from the start of the lowest free span
// that holds them.
func (f *hashMap) Allocate(txid common.Txid, n int) common.Pgid {
	if n == 0 {
		return 0
	}
	span := f.spans.lowestFit(uint64(n))
	if span == nil {
		return 0
	}

	// Take the pages and return the rest of the span to the freelist.
	pid, size := span.start, span.size
	f.delSpan(pid, size)
	if remain := size - uint64(n); remain != 0 {
		f.addSpan(pid+common.Pgid(n), remain)
	}
	f.allocs[pid] = txid
	for i := common.Pgid(0); i < common.Pgid(n); i++ {
		delete(f.cache, pid+i)
	}
	return pid
}

// TrimTail removes the free span ending at page pgid-1 and returns its start,
// or returns pgid when that page is not free.
func (f *hashMap) TrimTail(pgid common.Pgid) common.Pgid {
	size, ok := f.backwardMap[pgid-1]
	if !ok {
		return pgid
	}
	start := pgid - common.Pgid(size)
	f.delSpan(start, size)
	for id := start; id < pgid; id++ {
		delete(f.cache, id)
	}
	return start
}

func (f *hashMap) AllocatesBelow(n int, pgid common.Pgid) bool {
	span := f.spans.lowestFit(uint64(n))
	return span != nil && span.start+common.Pgid(n) <= pgid
}

func (f *hashMap) lastFree() common.Pgid {
	span := f.spans.last()
	if span == nil {
		return 0
	}
	return span.start + common.Pgid(span.size) - 1
}

func (f *hashMap) FreeCount() int {
	common.Verify(func() {
		expectedFreePageCount := f.hashmapFreeCountSlow()
		common.Assert(int(f.freePagesCount) == expectedFreePageCount,
			"freePagesCount (%d) is out of sync with free pages map (%d)", f.freePagesCount, expectedFreePageCount)
	})
	return int(f.freePagesCount)
}

func (f *hashMap) freePageIds() common.Pgids {
	return common.FreelistSpanIds(f.freeSpans())
}

func (f *hashMap) freeSpans() []common.FreelistSpan {
	spans := make([]common.FreelistSpan, 0, len(f.forwardMap))
	f.spans.walk(func(start common.Pgid, size uint64) {
		spans = append(spans, common.FreelistSpan{Start: start, Len: size})
	})
	return spans
}

func (f *hashMap) freeSpanCount() int {
	return len(f.forwardMap)
}

func (f *hashMap) hashmapFreeCountSlow() int {
	count := 0
	for _, size := range f.forwardMap {
		count += int(size)
	}
	return count
}

func (f *hashMap) addSpan(start common.Pgid, size uint64) {
	f.backwardMap[start-1+common.Pgid(size)] = size
	f.forwardMap[start] = size
	f.spans.insert(start, size)
	f.freePagesCount += size
}

func (f *hashMap) delSpan(start common.Pgid, size uint64) {
	delete(f.forwardMap, start)
	delete(f.backwardMap, start+common.Pgid(size-1))
	f.spans.remove(start)
	f.freePagesCount -= size
}

func (f *hashMap) mergeSpans(ids common.Pgids) {
	common.Verify(func() {
		ids1Tree := f.idsFromSpanTree()
		ids2Forward := f.idsFromForwardMap()
		ids3Backward := f.idsFromBackwardMap()

		if !reflect.DeepEqual(ids1Tree, ids2Forward) {
			panic(fmt.Sprintf("Detected mismatch, f.spans: %v, f.forwardMap: %v", f.freeSpans(), f.forwardMap))
		}
		if !reflect.DeepEqual(ids1Tree, ids3Backward) {
			panic(fmt.Sprintf("Detected mismatch, f.spans: %v, f.backwardMap: %v", f.freeSpans(), f.backwardMap))
		}

		sort.Sort(ids)
		prev := common.Pgid(0)
		for _, id := range ids {
			// The ids shouldn't have duplicated free ID.
			if prev == id {
				panic(fmt.Sprintf("detected duplicated free ID: %d in ids: %v", id, ids))
			}
			prev = id

			// The ids shouldn't have any overlap with the existing free spans.
			if _, ok := ids1Tree[id]; ok {
				panic(fmt.Sprintf("detected overlapped free page ID: %d between ids: %v and existing f.spans: %v", id, ids, f.freeSpans()))
			}
		}
	})
	for _, id := range ids {
		// try to see if we can merge and update
		f.mergeWithExistingSpan(id)
	}
}

// mergeWithExistingSpan merges pid to the existing free spans, try to merge it backward and forward
func (f *hashMap) mergeWithExistingSpan(pid common.Pgid) {
	prev := pid - 1
	next := pid + 1

	preSize, mergeWithPrev := f.backwardMap[prev]
	nextSize, mergeWithNext := f.forwardMap[next]
	newStart := pid
	newSize := uint64(1)

	if mergeWithPrev {
		//merge with previous span
		start := prev + 1 - common.Pgid(preSize)
		f.delSpan(start, preSize)

		newStart -= common.Pgid(preSize)
		newSize += preSize
	}

	if mergeWithNext {
		// merge with next span
		f.delSpan(next, nextSize)
		newSize += nextSize
	}

	f.addSpan(newStart, newSize)
}

// idsFromSpanTree gets all free page IDs from f.spans.
// used by test only.
func (f *hashMap) idsFromSpanTree() map[common.Pgid]struct{} {
	ids := make(map[common.Pgid]struct{})
	f.spans.walk(func(start common.Pgid, size uint64) {
		for i := range common.Pgid(size) {
			id := start + i
			if _, ok := ids[id]; ok {
				panic(fmt.Sprintf("detected duplicated free page ID: %d in f.spans: %v", id, f.freeSpans()))
			}
			ids[id] = struct{}{}
		}
	})
	return ids
}

// idsFromForwardMap get all free page IDs from f.forwardMap.
// used by test only.
func (f *hashMap) idsFromForwardMap() map[common.Pgid]struct{} {
	ids := make(map[common.Pgid]struct{})
	for start, size := range f.forwardMap {
		for i := 0; i < int(size); i++ {
			id := start + common.Pgid(i)
			if _, ok := ids[id]; ok {
				panic(fmt.Sprintf("detected duplicated free page ID: %d in f.forwardMap: %v", id, f.forwardMap))
			}
			ids[id] = struct{}{}
		}
	}
	return ids
}

// idsFromBackwardMap get all free page IDs from f.backwardMap.
// used by test only.
func (f *hashMap) idsFromBackwardMap() map[common.Pgid]struct{} {
	ids := make(map[common.Pgid]struct{})
	for end, size := range f.backwardMap {
		for i := 0; i < int(size); i++ {
			id := end - common.Pgid(i)
			if _, ok := ids[id]; ok {
				panic(fmt.Sprintf("detected duplicated free page ID: %d in f.backwardMap: %v", id, f.backwardMap))
			}
			ids[id] = struct{}{}
		}
	}
	return ids
}

func NewHashMapFreelist() Interface {
	hm := &hashMap{
		shared:      newShared(),
		forwardMap:  make(map[common.Pgid]uint64),
		backwardMap: make(map[common.Pgid]uint64),
	}
	hm.Interface = hm
	return hm
}
