package common

import (
	"fmt"
	"os"
	"sort"
	"unsafe"
)

const PageHeaderSize = unsafe.Sizeof(Page{})

const MinKeysPerPage = 2

const BranchPageElementSize = unsafe.Sizeof(branchPageElement{})
const LeafPageElementSize = unsafe.Sizeof(leafPageElement{})
const pgidSize = unsafe.Sizeof(Pgid(0))

// freelistCountSize is the size of the span count leading a freelist page.
const freelistCountSize = unsafe.Sizeof(uint64(0))

const (
	BranchPageFlag   = 0x01
	LeafPageFlag     = 0x02
	MetaPageFlag     = 0x04
	FreelistPageFlag = 0x20
)

const (
	BucketLeafFlag = 0x01
)

type Pgid uint64

type Page struct {
	id       Pgid
	flags    uint16
	count    uint16
	overflow uint32
}

func NewPage(id Pgid, flags, count uint16, overflow uint32) *Page {
	return &Page{
		id:       id,
		flags:    flags,
		count:    count,
		overflow: overflow,
	}
}

// Typ returns a human-readable page type string used for debugging.
func (p *Page) Typ() string {
	if p.IsBranchPage() {
		return "branch"
	} else if p.IsLeafPage() {
		return "leaf"
	} else if p.IsMetaPage() {
		return "meta"
	} else if p.IsFreelistPage() {
		return "freelist"
	}
	return fmt.Sprintf("unknown<%02x>", p.flags)
}

func (p *Page) IsBranchPage() bool {
	return p.flags == BranchPageFlag
}

func (p *Page) IsLeafPage() bool {
	return p.flags == LeafPageFlag
}

func (p *Page) IsMetaPage() bool {
	return p.flags == MetaPageFlag
}

func (p *Page) IsFreelistPage() bool {
	return p.flags == FreelistPageFlag
}

// Meta returns a pointer to the metadata section of the page.
func (p *Page) Meta() *Meta {
	return (*Meta)(UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p)))
}

func (p *Page) FastCheck(id Pgid) {
	if p.id != id {
		Assert(false, "Page expected to be: %v, but self identifies as %v", id, p.id)
	}
	// Only one flag of page-type can be set.
	if !p.IsBranchPage() && !p.IsLeafPage() && !p.IsMetaPage() && !p.IsFreelistPage() {
		Assert(false, "page %v: has unexpected type/flags: %x", p.id, p.flags)
	}
}

// LeafPageElement retrieves the leaf node by index
func (p *Page) LeafPageElement(index uint16) *leafPageElement {
	return (*leafPageElement)(UnsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p),
		LeafPageElementSize, int(index)))
}

// LeafPageElements retrieves a list of leaf nodes.
func (p *Page) LeafPageElements() []leafPageElement {
	if p.count == 0 {
		return nil
	}
	data := UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
	elems := unsafe.Slice((*leafPageElement)(data), int(p.count))
	return elems
}

// BranchPageElement retrieves the branch node by index
func (p *Page) BranchPageElement(index uint16) *branchPageElement {
	return (*branchPageElement)(UnsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p),
		unsafe.Sizeof(branchPageElement{}), int(index)))
}

// BranchPageElements retrieves a list of branch nodes.
func (p *Page) BranchPageElements() []branchPageElement {
	if p.count == 0 {
		return nil
	}
	data := UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
	elems := unsafe.Slice((*branchPageElement)(data), int(p.count))
	return elems
}

// FreelistSpan is a run of Len consecutive free pages beginning at Start.
type FreelistSpan struct {
	Start Pgid
	Len   uint64
}

// End returns the page id following the span.
func (s FreelistSpan) End() Pgid {
	return s.Start + Pgid(s.Len)
}

// FreelistSpanIds returns the page ids covered by spans.
func FreelistSpanIds(spans []FreelistSpan) Pgids {
	var n uint64
	for _, span := range spans {
		n += span.Len
	}
	ids := make(Pgids, 0, n)
	for _, span := range spans {
		for id := span.Start; id < span.End(); id++ {
			ids = append(ids, id)
		}
	}
	return ids
}

// FreelistPageSize returns the bytes a freelist page needs to hold n spans.
func FreelistPageSize(n int) int {
	return int(PageHeaderSize) + int(freelistCountSize) + n*int(unsafe.Sizeof(FreelistSpan{}))
}

// FreelistPageSpans returns the spans stored in a freelist page, sorted by
// start page. The page body holds a uint64 span count followed by the spans.
// The returned slice aliases the page.
func (p *Page) FreelistPageSpans() []FreelistSpan {
	if !p.IsFreelistPage() {
		Assert(false, "can't get freelist spans from a non-freelist page: %2x", p.flags)
	}

	data := UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
	count := *(*uint64)(data)
	if count == 0 {
		return nil
	}
	if count > uint64(^uint(0)>>1) {
		panic(fmt.Sprintf("freelist span count %d overflows int", count))
	}
	return unsafe.Slice((*FreelistSpan)(UnsafeAdd(data, freelistCountSize)), int(count))
}

// WriteFreelistPage marks p as a freelist page holding spans. The page must
// have room for FreelistPageSize(len(spans)) bytes.
func (p *Page) WriteFreelistPage(spans []FreelistSpan) {
	p.flags = FreelistPageFlag
	p.count = 0
	data := UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
	*(*uint64)(data) = uint64(len(spans))
	if len(spans) != 0 {
		copy(unsafe.Slice((*FreelistSpan)(UnsafeAdd(data, freelistCountSize)), len(spans)), spans)
	}
}

// dump writes n bytes of the page to STDERR as hex output.
func (p *Page) hexdump(n int) {
	buf := UnsafeByteSlice(unsafe.Pointer(p), 0, 0, n)
	fmt.Fprintf(os.Stderr, "%x\n", buf)
}

func (p *Page) PageElementSize() uintptr {
	if p.IsLeafPage() {
		return LeafPageElementSize
	}
	return BranchPageElementSize
}

func (p *Page) Id() Pgid {
	return p.id
}

func (p *Page) SetId(target Pgid) {
	p.id = target
}

func (p *Page) Flags() uint16 {
	return p.flags
}

func (p *Page) SetFlags(v uint16) {
	p.flags = v
}

func (p *Page) Count() uint16 {
	return p.count
}

func (p *Page) SetCount(target uint16) {
	p.count = target
}

func (p *Page) Overflow() uint32 {
	return p.overflow
}

func (p *Page) SetOverflow(target uint32) {
	p.overflow = target
}

func (p *Page) String() string {
	return fmt.Sprintf("ID: %d, Type: %s, count: %d, overflow: %d", p.id, p.Typ(), p.count, p.overflow)
}

type Pages []*Page

func (s Pages) Len() int           { return len(s) }
func (s Pages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Pages) Less(i, j int) bool { return s[i].id < s[j].id }

// branchPageElement represents a node on a branch page.
type branchPageElement struct {
	pos   uint32
	ksize uint32
	pgid  Pgid
}

func (n *branchPageElement) Pos() uint32 {
	return n.pos
}

func (n *branchPageElement) SetPos(v uint32) {
	n.pos = v
}

func (n *branchPageElement) Ksize() uint32 {
	return n.ksize
}

func (n *branchPageElement) SetKsize(v uint32) {
	n.ksize = v
}

func (n *branchPageElement) Pgid() Pgid {
	return n.pgid
}

func (n *branchPageElement) SetPgid(v Pgid) {
	n.pgid = v
}

// Key returns a byte slice of the node key.
func (n *branchPageElement) Key() []byte {
	return UnsafeByteSlice(unsafe.Pointer(n), 0, int(n.pos), int(n.pos)+int(n.ksize))
}

// leafPageElement represents a node on a leaf page.
type leafPageElement struct {
	flags uint32
	pos   uint32
	ksize uint32
	vsize uint32
}

func NewLeafPageElement(flags, pos, ksize, vsize uint32) *leafPageElement {
	return &leafPageElement{
		flags: flags,
		pos:   pos,
		ksize: ksize,
		vsize: vsize,
	}
}

func (n *leafPageElement) Flags() uint32 {
	return n.flags
}

func (n *leafPageElement) SetFlags(v uint32) {
	n.flags = v
}

func (n *leafPageElement) Pos() uint32 {
	return n.pos
}

func (n *leafPageElement) SetPos(v uint32) {
	n.pos = v
}

func (n *leafPageElement) Ksize() uint32 {
	return n.ksize
}

func (n *leafPageElement) SetKsize(v uint32) {
	n.ksize = v
}

func (n *leafPageElement) Vsize() uint32 {
	return n.vsize
}

func (n *leafPageElement) SetVsize(v uint32) {
	n.vsize = v
}

// Key returns a byte slice of the node key.
func (n *leafPageElement) Key() []byte {
	i := int(n.pos)
	j := i + int(n.ksize)
	return UnsafeByteSlice(unsafe.Pointer(n), 0, i, j)
}

// Value returns a byte slice of the node value.
func (n *leafPageElement) Value() []byte {
	i := int(n.pos) + int(n.ksize)
	j := i + int(n.vsize)
	return UnsafeByteSlice(unsafe.Pointer(n), 0, i, j)
}

func (n *leafPageElement) IsBucketEntry() bool {
	return n.flags&uint32(BucketLeafFlag) != 0
}

func (n *leafPageElement) Bucket() *InBucket {
	if n.IsBucketEntry() {
		return LoadBucket(n.Value())
	} else {
		return nil
	}
}

// PageInfo represents human readable information about a page.
type PageInfo struct {
	ID            int
	Type          string
	Count         int
	OverflowCount int
}

type Pgids []Pgid

func (s Pgids) Len() int           { return len(s) }
func (s Pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Pgids) Less(i, j int) bool { return s[i] < s[j] }

// Merge returns the sorted union of a and b.
func (s Pgids) Merge(b Pgids) Pgids {
	// Return the opposite slice if one is nil.
	if len(s) == 0 {
		return b
	}
	if len(b) == 0 {
		return s
	}
	merged := make(Pgids, len(s)+len(b))
	Mergepgids(merged, s, b)
	return merged
}

// Mergepgids copies the sorted union of a and b into dst.
// If dst is too small, it panics.
func Mergepgids(dst, a, b Pgids) {
	if len(dst) < len(a)+len(b) {
		panic(fmt.Errorf("mergepgids bad len %d < %d + %d", len(dst), len(a), len(b)))
	}
	// Copy in the opposite slice if one is nil.
	if len(a) == 0 {
		copy(dst, b)
		return
	}
	if len(b) == 0 {
		copy(dst, a)
		return
	}

	// Merged will hold all elements from both lists.
	merged := dst[:0]

	// Assign lead to the slice with a lower starting value, follow to the higher value.
	lead, follow := a, b
	if b[0] < a[0] {
		lead, follow = b, a
	}

	// Continue while there are elements in the lead.
	for len(lead) > 0 {
		// Merge largest prefix of lead that is ahead of follow[0].
		n := sort.Search(len(lead), func(i int) bool { return lead[i] > follow[0] })
		merged = append(merged, lead[:n]...)
		if n >= len(lead) {
			break
		}

		// Swap lead and follow.
		lead, follow = follow, lead[n:]
	}

	// Append what's left in follow.
	_ = append(merged, follow...)
}
