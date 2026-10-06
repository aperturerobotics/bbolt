package freelist

import (
	"github.com/aperturerobotics/bbolt/internal/common"
)

// spanTree holds free spans ordered by start page in a treap. Each node also
// records the longest span in its subtree, so lowestFit finds the first span
// that holds a request in O(log n). Allocating from the lowest fitting span
// keeps live pages at the start of the file and lets the free pages at its end
// be truncated.
type spanTree struct {
	root *spanNode
}

// spanNode is one free span in a spanTree.
type spanNode struct {
	start       common.Pgid
	size        uint64
	longest     uint64 // longest span size in this subtree
	priority    uint64 // heap order; derived from start
	left, right *spanNode
}

// longestSize returns the longest span size under n, or 0 when n is nil.
func (n *spanNode) longestSize() uint64 {
	if n == nil {
		return 0
	}
	return n.longest
}

// update recomputes n.longest from n and its children.
func (n *spanNode) update() {
	n.longest = max(n.size, n.left.longestSize(), n.right.longestSize())
}

// spanPriority mixes start into a well-distributed heap priority (splitmix64),
// keeping the tree balanced without random state.
func spanPriority(start common.Pgid) uint64 {
	z := uint64(start) + 0x9e3779b97f4a7c15
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// insert adds the span starting at start. The span must not overlap another.
func (t *spanTree) insert(start common.Pgid, size uint64) {
	node := &spanNode{start: start, size: size, longest: size, priority: spanPriority(start)}
	t.root = insertSpan(t.root, node)
}

// remove deletes the span starting at start.
func (t *spanTree) remove(start common.Pgid) {
	t.root = removeSpan(t.root, start)
}

// lowestFit returns the lowest span of at least size pages, or nil.
func (t *spanTree) lowestFit(size uint64) *spanNode {
	n := t.root
	if n.longestSize() < size {
		return nil
	}
	for {
		switch {
		case n.left.longestSize() >= size:
			n = n.left
		case n.size >= size:
			return n
		default:
			n = n.right
		}
	}
}

// last returns the span with the highest start, or nil.
func (t *spanTree) last() *spanNode {
	n := t.root
	for n != nil && n.right != nil {
		n = n.right
	}
	return n
}

// walk calls fn for each span in ascending start order.
func (t *spanTree) walk(fn func(start common.Pgid, size uint64)) {
	var visit func(n *spanNode)
	visit = func(n *spanNode) {
		if n == nil {
			return
		}
		visit(n.left)
		fn(n.start, n.size)
		visit(n.right)
	}
	visit(t.root)
}

// insertSpan inserts node under n and returns the new subtree root.
func insertSpan(n, node *spanNode) *spanNode {
	if n == nil {
		return node
	}
	if node.priority > n.priority {
		node.left, node.right = splitSpans(n, node.start)
		node.update()
		return node
	}
	if node.start < n.start {
		n.left = insertSpan(n.left, node)
	} else {
		n.right = insertSpan(n.right, node)
	}
	n.update()
	return n
}

// removeSpan removes the span starting at start under n and returns the new
// subtree root.
func removeSpan(n *spanNode, start common.Pgid) *spanNode {
	if n == nil {
		return nil
	}
	switch {
	case start == n.start:
		return joinSpans(n.left, n.right)
	case start < n.start:
		n.left = removeSpan(n.left, start)
	default:
		n.right = removeSpan(n.right, start)
	}
	n.update()
	return n
}

// splitSpans splits n into the spans starting before key and the rest.
func splitSpans(n *spanNode, key common.Pgid) (lo, hi *spanNode) {
	if n == nil {
		return nil, nil
	}
	if n.start < key {
		n.right, hi = splitSpans(n.right, key)
		n.update()
		return n, hi
	}
	lo, n.left = splitSpans(n.left, key)
	n.update()
	return lo, n
}

// joinSpans joins lo and hi, where every span in lo starts before hi.
func joinSpans(lo, hi *spanNode) *spanNode {
	if lo == nil {
		return hi
	}
	if hi == nil {
		return lo
	}
	if lo.priority > hi.priority {
		lo.right = joinSpans(lo.right, hi)
		lo.update()
		return lo
	}
	hi.left = joinSpans(lo, hi.left)
	hi.update()
	return hi
}
