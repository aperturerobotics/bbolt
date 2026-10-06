package freelist

import (
	"fmt"
	"sort"

	"github.com/aperturerobotics/bbolt/internal/common"
)

type array struct {
	*shared

	ids []common.Pgid // all free and available free page ids.
}

func (f *array) Init(ids common.Pgids) {
	f.ids = ids
	f.reindex()
}

func (f *array) initSpans(spans []common.FreelistSpan) {
	f.Init(common.FreelistSpanIds(spans))
}

func (f *array) Allocate(txid common.Txid, n int) common.Pgid {
	if len(f.ids) == 0 {
		return 0
	}

	var initial, previd common.Pgid
	for i, id := range f.ids {
		if id <= 1 {
			panic(fmt.Sprintf("invalid page allocation: %d", id))
		}

		// Reset initial page if this is not contiguous.
		if previd == 0 || id-previd != 1 {
			initial = id
		}

		// If we found a contiguous block then remove it and return it.
		if (id-initial)+1 == common.Pgid(n) {
			// If we're allocating off the beginning then take the fast path
			// and just adjust the existing slice. This will use extra memory
			// temporarily but the append() in free() will realloc the slice
			// as is necessary.
			if (i + 1) == n {
				f.ids = f.ids[i+1:]
			} else {
				copy(f.ids[i-n+1:], f.ids[i+1:])
				f.ids = f.ids[:len(f.ids)-n]
			}

			// Remove from the free cache.
			for i := common.Pgid(0); i < common.Pgid(n); i++ {
				delete(f.cache, initial+i)
			}
			f.allocs[initial] = txid
			return initial
		}

		previd = id
	}
	return 0
}

// TrimTail removes the free pages that end at page pgid-1 and returns the
// first of them, or returns pgid when that page is not free.
func (f *array) TrimTail(pgid common.Pgid) common.Pgid {
	n := len(f.ids)
	for n > 0 && f.ids[n-1] == pgid-1 {
		n--
		pgid--
		delete(f.cache, pgid)
	}
	f.ids = f.ids[:n]
	return pgid
}

func (f *array) AllocatesBelow(n int, pgid common.Pgid) bool {
	// Allocate takes the first run of n contiguous pages.
	var run int
	for i, id := range f.ids {
		if id >= pgid {
			return false
		}
		if i == 0 || id != f.ids[i-1]+1 {
			run = 0
		}
		if run++; run >= n {
			return true
		}
	}
	return false
}

func (f *array) lastFree() common.Pgid {
	if len(f.ids) == 0 {
		return 0
	}
	return f.ids[len(f.ids)-1]
}

func (f *array) FreeCount() int {
	return len(f.ids)
}

func (f *array) freePageIds() common.Pgids {
	return f.ids
}

func (f *array) freeSpans() []common.FreelistSpan {
	return idSpans(f.ids)
}

func (f *array) freeSpanCount() int {
	var n int
	for i, id := range f.ids {
		if i == 0 || id != f.ids[i-1]+1 {
			n++
		}
	}
	return n
}

func (f *array) mergeSpans(ids common.Pgids) {
	sort.Sort(ids)
	common.Verify(func() {
		idsIdx := make(map[common.Pgid]struct{})
		for _, id := range f.ids {
			// The existing f.ids shouldn't have duplicated free ID.
			if _, ok := idsIdx[id]; ok {
				panic(fmt.Sprintf("detected duplicated free page ID: %d in existing f.ids: %v", id, f.ids))
			}
			idsIdx[id] = struct{}{}
		}

		prev := common.Pgid(0)
		for _, id := range ids {
			// The ids shouldn't have duplicated free ID. Note page 0 and 1
			// are reserved for meta pages, so they can never be free page IDs.
			if prev == id {
				panic(fmt.Sprintf("detected duplicated free ID: %d in ids: %v", id, ids))
			}
			prev = id

			// The ids shouldn't have any overlap with the existing f.ids.
			if _, ok := idsIdx[id]; ok {
				panic(fmt.Sprintf("detected overlapped free page ID: %d between ids: %v and existing f.ids: %v", id, ids, f.ids))
			}
		}
	})
	f.ids = common.Pgids(f.ids).Merge(ids)
}

func NewArrayFreelist() Interface {
	a := &array{
		shared: newShared(),
		ids:    []common.Pgid{},
	}
	a.Interface = a
	return a
}
