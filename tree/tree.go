// Package tree provides the basic node structures for disk-based B+ tree implementation.
package tree

// Key is a comparable type for B+ tree keys.
type Key interface {
	Less(other Key) bool
	Equal(other Key) bool
}

// IntKey is a sample implementation of Key for integers.
type IntKey int

func (k IntKey) Less(other Key) bool {
	ok, okType := other.(IntKey)
	if !okType {
		panic("type mismatch in IntKey.Less")
	}
	return k < ok
}

func (k IntKey) Equal(other Key) bool {
	ok, okType := other.(IntKey)
	if !okType {
		panic("type mismatch in IntKey.Equal")
	}
	return k == ok
}

// FloatKey is an implementation of Key for floating-point numbers.
type FloatKey float64

func (k FloatKey) Less(other Key) bool {
	ok, okType := other.(FloatKey)
	if !okType {
		panic("type mismatch in FloatKey.Less")
	}
	return k < ok
}

func (k FloatKey) Equal(other Key) bool {
	ok, okType := other.(FloatKey)
	if !okType {
		panic("type mismatch in FloatKey.Equal")
	}
	return k == ok
}

// StringKey is an implementation of Key for strings.
type StringKey string

func (k StringKey) Less(other Key) bool {
	ok, okType := other.(StringKey)
	if !okType {
		panic("type mismatch in StringKey.Less")
	}
	return k < ok
}

func (k StringKey) Equal(other Key) bool {
	ok, okType := other.(StringKey)
	if !okType {
		panic("type mismatch in StringKey.Equal")
	}
	return k == ok
}

// Node is the interface for all B+ tree nodes.
type Node[V any] interface {
	isLeaf() bool
	GetPageID() uint32
	SetPageID(pageID uint32)
}

// IntermNode is an internal node in the B+ tree.
type IntermNode[K Key, V any] struct {
	Pointers []uint32 // Page IDs of child nodes, len = len(Keys)+1
	Keys     []K
	pageID   uint32
	deleted  bool // Indicates if this node is marked for deletion
}

func (n *IntermNode[K, V]) isLeaf() bool { return false }

func (n *IntermNode[K, V]) GetPageID() uint32 { return n.pageID }

func (n *IntermNode[K, V]) SetPageID(pageID uint32) { n.pageID = pageID }

func (n *IntermNode[K, V]) IsDeleted() bool { return n.deleted }

func (n *IntermNode[K, V]) SetDeleted(deleted bool) { n.deleted = deleted }

// LeafPair holds a key-value pair in a leaf node.
type LeafPair[K Key, V any] struct {
	K     K
	Value V
}

// LeafNode is a leaf node in the B+ tree.
type LeafNode[K Key, V any] struct {
	Pairs    []LeafPair[K, V]
	nextPage uint32 // Page ID of next leaf node
	prevPage uint32 // Page ID of previous leaf node
	pageID   uint32
	deleted  bool // Indicates if this node is marked for deletion
}

func (l *LeafNode[K, V]) isLeaf() bool { return true }

func (l *LeafNode[K, V]) GetPageID() uint32 { return l.pageID }

func (l *LeafNode[K, V]) SetPageID(pageID uint32) { l.pageID = pageID }

func (l *LeafNode[K, V]) GetNextPage() uint32 { return l.nextPage }

func (l *LeafNode[K, V]) GetPrevPage() uint32 { return l.prevPage }

func (l *LeafNode[K, V]) SetNextPage(nextPage uint32) { l.nextPage = nextPage }

func (l *LeafNode[K, V]) SetPrevPage(prevPage uint32) { l.prevPage = prevPage }

func (l *LeafNode[K, V]) IsDeleted() bool { return l.deleted }

func (l *LeafNode[K, V]) SetDeleted(deleted bool) { l.deleted = deleted }
