// Package tree provides a generic, optimized B+ tree implementation for use in databases and other storage systems.
package tree

import (
	"errors"
)

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
}

// Tree represents a B+ tree.
type Tree[K Key, V any] struct {
	Root  Node[V]
	Order int
}

// IntermNode is an internal node in the B+ tree.
type IntermNode[K Key, V any] struct {
	Pointers []Node[V] // len = len(Keys)+1
	Keys     []K
}

func (n *IntermNode[K, V]) isLeaf() bool { return false }

// LeafPair holds a key-value pair in a leaf node.
type LeafPair[K Key, V any] struct {
	K     K
	Value V
}

// LeafNode is a leaf node in the B+ tree.
type LeafNode[K Key, V any] struct {
	Pairs []LeafPair[K, V]
	next  *LeafNode[K, V]
	prev  *LeafNode[K, V]
}

func (l *LeafNode[K, V]) isLeaf() bool { return true }

// NewTree creates a new B+ tree with the given order. Order must be >= 3.
func NewTree[K Key, V any](order int) (*Tree[K, V], error) {
	if order < 3 {
		return nil, errors.New("order must be >= 3")
	}
	return &Tree[K, V]{
		Root:  &LeafNode[K, V]{},
		Order: order,
	}, nil
}

// Delete removes a key from the tree.
func (t *Tree[K, V]) Delete(key K) error {
	if t.Root == nil {
		return errors.New("tree is empty")
	}

	// Check if key exists
	_, err := t.Search(key)
	if err != nil {
		return err
	}

	// Delete recursively
	underflow, err := delete[K, V](key, t.Root, t.Order)
	if err != nil {
		return err
	}

	// Handle root underflow
	if underflow && !t.Root.isLeaf() {
		root, ok := t.Root.(*IntermNode[K, V])
		if !ok {
			return errors.New("expected internal root node")
		}
		if len(root.Keys) == 0 {
			// Root has only one child, make it the new root
			t.Root = root.Pointers[0]
		}
	}

	return nil
}

// delete recursively deletes a key and handles underflow with borrowing.
func delete[K Key, V any](key K, node Node[V], order int) (bool, error) {
	if node.isLeaf() {
		return deleteFromLeaf[K, V](key, node, order)
	}
	return deleteFromInternal[K, V](key, node, order)
}

// deleteFromLeaf handles deletion from a leaf node.
func deleteFromLeaf[K Key, V any](key K, node Node[V], order int) (bool, error) {
	n, ok := node.(*LeafNode[K, V])
	if !ok {
		return false, errors.New("expected a leaf node")
	}

	// Find the key to delete
	index := leafBinarySearch[K, V](key, n.Pairs)
	if index == -1 {
		return false, nil // Key not found
	}

	// Remove the key-value pair
	n.Pairs = removeAt[LeafPair[K, V]](n.Pairs, index)

	// Check for underflow
	minKeys := (order - 1) / 2
	if len(n.Pairs) < minKeys {
		return true, nil // Underflow occurred
	}

	return false, nil // No underflow
}

// deleteFromInternal handles deletion from an internal node.
func deleteFromInternal[K Key, V any](key K, node Node[V], order int) (bool, error) {
	n, ok := node.(*IntermNode[K, V])
	if !ok {
		return false, errors.New("expected an internal node")
	}

	// Find the child to recurse into
	childIndex := upperBound[K](key, n.Keys)
	if childIndex >= len(n.Pointers) {
		return false, errors.New("invalid child index")
	}

	// Recursively delete from child
	underflow, err := delete[K, V](key, n.Pointers[childIndex], order)
	if err != nil {
		return false, err
	}

	if !underflow {
		return false, nil // No underflow in child
	}

	// Handle underflow in child with borrowing
	return handleUnderflow[K, V](n, childIndex, order)
}

// handleUnderflow tries to borrow from siblings or merge nodes.
func handleUnderflow[K Key, V any](node *IntermNode[K, V], childIndex int, order int) (bool, error) {
	// Try to borrow from left sibling
	if childIndex > 0 {
		leftSibling := node.Pointers[childIndex-1]
		if canBorrowFrom[K, V](leftSibling, order) {
			borrowFromLeft(node, childIndex)
			return false, nil
		}
	}

	// Try to borrow from right sibling
	if childIndex < len(node.Pointers)-1 {
		rightSibling := node.Pointers[childIndex+1]
		if canBorrowFrom[K, V](rightSibling, order) {
			borrowFromRight(node, childIndex)
			return false, nil
		}
	}

	// Cannot borrow, will need to merge
	//if there is left neighbour try to merge with it
	if childIndex > 0 {
		mergeLeft[K, V](node, childIndex)
		// now delete the separator key from the node
		node.Keys = removeAt[K](node.Keys, childIndex-1)
		// also remove the pointer
		node.Pointers = removeAt[Node[V]](node.Pointers, childIndex)

		// now check for underflow
		if len(node.Keys) < (order-1)/2 {
			return true, nil
		}
		return false, nil
	}
	if childIndex < len(node.Pointers)-1 {
		mergeRight[K, V](node, childIndex)
		removeAt[K](node.Keys, childIndex)
		removeAt[Node[V]](node.Pointers, childIndex+1)

		if len(node.Keys) < (order-1)/2 {
			return true, nil
		}
		return false, nil
	}
	return false, errors.New("unexpected: no siblings available for merge")
}

// mergeLeft merges the child with its left sibling.
func mergeLeft[K Key, V any](node *IntermNode[K, V], childIndex int) {
	leftSibling := node.Pointers[childIndex-1]
	child := node.Pointers[childIndex]

	if leftSibling.isLeaf() {
		leftLeaf := leftSibling.(*LeafNode[K, V])
		childLeaf := child.(*LeafNode[K, V])

		// Move all elements from child to left sibling
		leftLeaf.Pairs = append(leftLeaf.Pairs, childLeaf.Pairs...)

		// Update leaf linking
		leftLeaf.next = childLeaf.next
		if childLeaf.next != nil {
			childLeaf.next.prev = leftLeaf
		}
	} else {
		leftInterm := leftSibling.(*IntermNode[K, V])
		childInterm := child.(*IntermNode[K, V])

		// Add separator key to left sibling
		separatorKey := node.Keys[childIndex-1]
		leftInterm.Keys = append(leftInterm.Keys, separatorKey)

		// Move all keys and pointers from child to left sibling
		leftInterm.Keys = append(leftInterm.Keys, childInterm.Keys...)
		leftInterm.Pointers = append(leftInterm.Pointers, childInterm.Pointers...)
	}
}

// mergeRight merges the child with its right sibling.
func mergeRight[K Key, V any](node *IntermNode[K, V], childIndex int) {
	rightSibling := node.Pointers[childIndex+1]
	child := node.Pointers[childIndex]

	if rightSibling.isLeaf() {
		rightLeaf := rightSibling.(*LeafNode[K, V])
		childLeaf := child.(*LeafNode[K, V])

		// Move all elements from right sibling to child
		childLeaf.Pairs = append(childLeaf.Pairs, rightLeaf.Pairs...)

		// Update leaf linking
		childLeaf.next = rightLeaf.next
		if rightLeaf.next != nil {
			rightLeaf.next.prev = childLeaf
		}
	} else {
		rightInterm := rightSibling.(*IntermNode[K, V])
		childInterm := child.(*IntermNode[K, V])

		// Add separator key to child
		separatorKey := node.Keys[childIndex]
		childInterm.Keys = append(childInterm.Keys, separatorKey)

		// Move all keys and pointers from right sibling to child
		childInterm.Keys = append(childInterm.Keys, rightInterm.Keys...)
		childInterm.Pointers = append(childInterm.Pointers, rightInterm.Pointers...)
	}
}

// canBorrowFrom checks if a node has extra elements to borrow.
func canBorrowFrom[K Key, V any](node Node[V], order int) bool {
	if node.isLeaf() {
		leaf, ok := node.(*LeafNode[K, V])
		if !ok {
			return false
		}
		minKeys := (order - 1) / 2
		return len(leaf.Pairs) > minKeys
	}

	interm, ok := node.(*IntermNode[K, V])
	if !ok {
		return false
	}
	minKeys := (order - 1) / 2
	return len(interm.Keys) > minKeys
}

// borrowFromLeft borrows an element from the left sibling.
func borrowFromLeft[K Key, V any](node *IntermNode[K, V], childIndex int) {
	leftSibling := node.Pointers[childIndex-1]
	child := node.Pointers[childIndex]

	if leftSibling.isLeaf() {
		leftLeaf := leftSibling.(*LeafNode[K, V])
		childLeaf := child.(*LeafNode[K, V])

		// Move the rightmost element from left to child
		borrowed := leftLeaf.Pairs[len(leftLeaf.Pairs)-1]
		leftLeaf.Pairs = leftLeaf.Pairs[:len(leftLeaf.Pairs)-1]

		// Insert at the beginning of child
		childLeaf.Pairs = insertAt[LeafPair[K, V]](childLeaf.Pairs, 0, borrowed)

		// Update the separator key
		node.Keys[childIndex-1] = borrowed.K
	} else {
		leftInterm := leftSibling.(*IntermNode[K, V])
		childInterm := child.(*IntermNode[K, V])

		// Move the rightmost key and pointer from left to child
		borrowedKey := leftInterm.Keys[len(leftInterm.Keys)-1]
		borrowedPtr := leftInterm.Pointers[len(leftInterm.Pointers)-1]

		leftInterm.Keys = leftInterm.Keys[:len(leftInterm.Keys)-1]
		leftInterm.Pointers = leftInterm.Pointers[:len(leftInterm.Pointers)-1]

		// Insert at the beginning of child
		childInterm.Keys = insertAt[K](childInterm.Keys, 0, borrowedKey)
		childInterm.Pointers = insertAt[Node[V]](childInterm.Pointers, 0, borrowedPtr)

		// Update the separator key
		node.Keys[childIndex-1] = borrowedKey
	}
}

// borrowFromRight borrows an element from the right sibling.
func borrowFromRight[K Key, V any](node *IntermNode[K, V], childIndex int) {
	rightSibling := node.Pointers[childIndex+1]
	child := node.Pointers[childIndex]

	if rightSibling.isLeaf() {
		rightLeaf := rightSibling.(*LeafNode[K, V])
		childLeaf := child.(*LeafNode[K, V])

		// Move the leftmost element from right to child
		borrowed := rightLeaf.Pairs[0]
		rightLeaf.Pairs = rightLeaf.Pairs[1:]

		// Insert at the end of child
		childLeaf.Pairs = append(childLeaf.Pairs, borrowed)

		// Update the separator key
		node.Keys[childIndex] = rightLeaf.Pairs[0].K
	} else {
		rightInterm := rightSibling.(*IntermNode[K, V])
		childInterm := child.(*IntermNode[K, V])

		// Move the leftmost key and pointer from right to child
		borrowedKey := rightInterm.Keys[0]
		borrowedPtr := rightInterm.Pointers[0]

		rightInterm.Keys = rightInterm.Keys[1:]
		rightInterm.Pointers = rightInterm.Pointers[1:]

		// Insert at the end of child
		childInterm.Keys = append(childInterm.Keys, borrowedKey)
		childInterm.Pointers = append(childInterm.Pointers, borrowedPtr)

		// Update the separator key
		node.Keys[childIndex] = rightInterm.Keys[0]
	}
}

// Insert inserts a key-value pair into the tree.
func (t *Tree[K, V]) Insert(key K, value V) error {
	if t.Root == nil {
		return errors.New("tree root is nil")
	}
	promotedKey, newRight, err := insert[K, V](key, value, t.Root, t.Order)
	if err != nil {
		return err
	}
	if promotedKey == nil && newRight == nil {
		return nil
	}
	// Root was split — create a new root
	newRoot := &IntermNode[K, V]{
		Keys:     []K{*promotedKey},
		Pointers: []Node[V]{t.Root, newRight},
	}
	t.Root = newRoot
	return nil
}

// insert recursively inserts a key-value pair and handles node splits.
func insert[K Key, V any](key K, value V, node Node[V], order int) (*K, Node[V], error) {
	if node.isLeaf() {
		n, ok := node.(*LeafNode[K, V])
		if !ok {
			return nil, nil, errors.New("expected a leaf node")
		}
		// Find insert position
		index := leafUpperBound[K, V](key, n.Pairs)
		// Check for duplicate key
		if index < len(n.Pairs) && n.Pairs[index].K.Equal(key) {
			return nil, nil, errors.New("duplicate key")
		}
		newElem := LeafPair[K, V]{key, value}
		newSlice := insertAt[LeafPair[K, V]](n.Pairs, index, newElem)
		if len(n.Pairs) == order-1 {
			// Split
			num := (order - 1) / 2
			n.Pairs = newSlice[:num]
			right := newSlice[num:]
			r := &LeafNode[K, V]{
				Pairs: right,
				prev:  n,
				next:  n.next,
			}
			if n.next != nil {
				n.next.prev = r
			}
			n.next = r
			return &right[0].K, r, nil
		}
		n.Pairs = newSlice
		return nil, nil, nil
	}
	// Internal node
	n, ok := node.(*IntermNode[K, V])
	if !ok {
		return nil, nil, errors.New("expected an internal node")
	}
	index := upperBound[K](key, n.Keys)
	promotedKey, newRight, err := insert[K, V](key, value, n.Pointers[index], order)
	if err != nil {
		return nil, nil, err
	}
	if promotedKey == nil && newRight == nil {
		return nil, nil, nil
	}
	// Insert promotedKey and newRight
	n.Keys = insertAt[K](n.Keys, index, *promotedKey)
	n.Pointers = insertAt[Node[V]](n.Pointers, index+1, newRight)
	if len(n.Keys) == order {
		num := (order - 1) / 2
		midKey := n.Keys[num]
		rightKeys := append([]K{}, n.Keys[num+1:]...)
		rightPtrs := append([]Node[V]{}, n.Pointers[num+1:]...)
		n.Keys = n.Keys[:num]
		n.Pointers = n.Pointers[:num+1]
		rightNode := &IntermNode[K, V]{
			Keys:     rightKeys,
			Pointers: rightPtrs,
		}
		return &midKey, rightNode, nil
	}
	return nil, nil, nil
}

// Search returns the value for a key, or an error if not found.
func (t *Tree[K, V]) Search(key K) (V, error) {
	if t.Root == nil {
		var zero V
		return zero, errors.New("tree is empty")
	}
	return dfs[K, V](key, t.Root)
}

// dfs recursively searches for a key in the tree.
func dfs[K Key, V any](key K, node Node[V]) (V, error) {
	if !node.isLeaf() {
		n, ok := node.(*IntermNode[K, V])
		if !ok {
			var zero V
			return zero, errors.New("expected an internal node")
		}
		index := upperBound[K](key, n.Keys)
		return dfs[K, V](key, n.Pointers[index])
	}
	n, ok := node.(*LeafNode[K, V])
	if !ok {
		var zero V
		return zero, errors.New("expected a leaf node")
	}
	ind := leafBinarySearch[K, V](key, n.Pairs)
	if ind == -1 {
		var zero V
		return zero, errors.New("not found")
	}
	return n.Pairs[ind].Value, nil
}
