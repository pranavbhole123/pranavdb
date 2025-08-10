package index

import (
	"errors"
	"fmt"
	"pranavdb/tree"
)

// DiskTree represents a disk-based B+ tree that stores nodes in an IndexFile
type DiskTree[K tree.Key, V any] struct {
	indexFile *IndexFile[K, V]
	order     int
}

// NewDiskTree creates a new disk-based B+ tree
func NewDiskTree[K tree.Key, V any](filepath string, order int) (*DiskTree[K, V], error) {
	if order < 3 {
		return nil, errors.New("order must be >= 3")
	}

	// Create the index file
	indexFile, err := NewIndexFile[K, V](filepath, order)
	if err != nil {
		return nil, err
	}

	return &DiskTree[K, V]{
		indexFile: indexFile,
		order:     order,
	}, nil
}

// OpenDiskTree opens an existing disk-based B+ tree
func OpenDiskTree[K tree.Key, V any](filepath string) (*DiskTree[K, V], error) {
	// Open the index file
	indexFile, err := OpenIndexFile[K, V](filepath)
	if err != nil {
		return nil, err
	}

	return &DiskTree[K, V]{
		indexFile: indexFile,
		order:     indexFile.GetOrder(),
	}, nil
}

// Close closes the disk tree and the underlying index file
func (t *DiskTree[K, V]) Close() error {
	return t.indexFile.Close()
}

// GetOrder returns the tree order
func (t *DiskTree[K, V]) GetOrder() int {
	return t.order
}

// GetRoot returns the current root page ID
func (t *DiskTree[K, V]) GetRoot() uint32 {
	return t.indexFile.GetRoot()
}

// Insert inserts a key-value pair into the tree
func (t *DiskTree[K, V]) Insert(key K, value V) error {
	rootPageID := t.indexFile.GetRoot()

	if rootPageID == 0 {
		// First insertion - create root leaf node
		return t.createFirstRoot(key, value)
	}

	// Load root node
	root, err := t.indexFile.readNode(rootPageID)
	if err != nil {
		return err
	}

	// Insert recursively
	promotedKey, newRightPageID, err := t.insertRecursive(key, value, root, rootPageID)
	if err != nil {
		return err
	}

	if promotedKey == nil && newRightPageID == 0 {
		return nil // No split occurred
	}

	// Root was split - create new root
	return t.createNewRoot(promotedKey, rootPageID, newRightPageID)
}

// createFirstRoot creates the first root node (leaf node)
func (t *DiskTree[K, V]) createFirstRoot(key K, value V) error {
	// Create leaf node
	leaf := &tree.LeafNode[K, V]{
		Pairs: []tree.LeafPair[K, V]{
			{K: key, Value: value},
		},
	}

	// Allocate page for root
	rootPageID, err := t.indexFile.allocatePage()
	if err != nil {
		return err
	}

	// Write root to disk
	if err := t.indexFile.writeNode(leaf, rootPageID); err != nil {
		return err
	}

	// Update root pointer
	return t.indexFile.SetRoot(rootPageID)
}

// createNewRoot creates a new root when the old root splits
func (t *DiskTree[K, V]) createNewRoot(promotedKey *K, leftPageID, rightPageID uint32) error {
	// Create internal node
	interm := &tree.IntermNode[K, V]{
		Keys:     []K{*promotedKey},
		Pointers: []uint32{leftPageID, rightPageID},
	}

	// Allocate page for new root
	rootPageID, err := t.indexFile.allocatePage()
	if err != nil {
		return err
	}

	// Write new root to disk
	if err := t.indexFile.writeNode(interm, rootPageID); err != nil {
		return err
	}

	// Update root pointer
	return t.indexFile.SetRoot(rootPageID)
}

// insertRecursive recursively inserts a key-value pair and handles splits
func (t *DiskTree[K, V]) insertRecursive(key K, value V, node tree.Node[V], pageID uint32) (*K, uint32, error) {
	// Check if it's a leaf node using type assertion
	if _, ok := node.(*tree.LeafNode[K, V]); ok {
		return t.insertIntoLeaf(key, value, node, pageID)
	}
	return t.insertIntoInternal(key, value, node, pageID)
}

// insertIntoLeaf handles insertion into a leaf node
func (t *DiskTree[K, V]) insertIntoLeaf(key K, value V, node tree.Node[V], pageID uint32) (*K, uint32, error) {
	leaf, ok := node.(*tree.LeafNode[K, V])
	if !ok {
		return nil, 0, errors.New("expected leaf node")
	}

	// Find insert position
	index := t.leafUpperBound(key, leaf.Pairs)

	// Check for duplicate
	if index < len(leaf.Pairs) && leaf.Pairs[index].K.Equal(key) {
		return nil, 0, errors.New("duplicate key")
	}

	// Insert new key-value
	newElem := tree.LeafPair[K, V]{key, value}
	newSlice := insertAt(leaf.Pairs, index, newElem)

	// No split
	if len(newSlice) < t.order {
		leaf.Pairs = newSlice
		return nil, 0, t.indexFile.writeNode(leaf, pageID)
	}

	// Split
	splitIndex := len(newSlice) / 2
	leftPairs := newSlice[:splitIndex]
	rightPairs := newSlice[splitIndex:]

	// Create right leaf
	rightLeaf := &tree.LeafNode[K, V]{Pairs: rightPairs}
	rightLeaf.SetNextPage(leaf.GetNextPage())
	rightLeaf.SetPrevPage(pageID)

	// Allocate page for right leaf
	rightPageID, err := t.indexFile.allocatePage()
	if err != nil {
		return nil, 0, err
	}

	// Update leaf pointers
	leaf.Pairs = leftPairs
	leaf.SetNextPage(rightPageID)

	// If there was a next leaf, fix its PrevPage
	if rightLeaf.GetNextPage() != 0 {
		nextLeaf, err := t.indexFile.readNode(rightLeaf.GetNextPage())
		if err != nil {
			return nil, 0, err
		}
		if nextLeafNode, ok := nextLeaf.(*tree.LeafNode[K, V]); ok {
			nextLeafNode.SetPrevPage(rightPageID)
			if err := t.indexFile.writeNode(nextLeafNode, rightLeaf.GetNextPage()); err != nil {
				return nil, 0, err
			}
		}
	}

	// Write both leaves
	if err := t.indexFile.writeNode(leaf, pageID); err != nil {
		return nil, 0, err
	}
	if err := t.indexFile.writeNode(rightLeaf, rightPageID); err != nil {
		return nil, 0, err
	}

	// Promote first key of right leaf
	promotedKey := &rightPairs[0].K
	return promotedKey, rightPageID, nil
}

// insertIntoInternal handles insertion into an internal node
func (t *DiskTree[K, V]) insertIntoInternal(key K, value V, node tree.Node[V], pageID uint32) (*K, uint32, error) {
	interm, ok := node.(*tree.IntermNode[K, V])
	if !ok {
		return nil, 0, errors.New("expected internal node")
	}

	// Find child to recurse into
	childIndex := t.upperBound(key, interm.Keys)
	if childIndex >= len(interm.Pointers) {
		return nil, 0, errors.New("invalid child index")
	}

	// Load child node
	childPageID := interm.Pointers[childIndex]
	child, err := t.indexFile.readNode(childPageID)
	if err != nil {
		return nil, 0, err
	}

	// Recursively insert into child
	promotedKey, newRightPageID, err := t.insertRecursive(key, value, child, childPageID)
	if err != nil {
		return nil, 0, err
	}

	if promotedKey == nil && newRightPageID == 0 {
		return nil, 0, nil // No split in child
	}

	// Child was split - insert promoted key and new right pointer
	interm.Keys = insertAt(interm.Keys, childIndex, *promotedKey)
	interm.Pointers = insertAt(interm.Pointers, childIndex+1, newRightPageID)

	// Check if internal node needs to split
	if len(interm.Keys) < t.order {
		// No split needed - just update the node
		if err := t.indexFile.writeNode(interm, pageID); err != nil {
			return nil, 0, err
		}
		return nil, 0, nil
	}

	// Internal node split needed
	splitIndex := (t.order - 1) / 2
	midKey := interm.Keys[splitIndex]
	rightKeys := interm.Keys[splitIndex+1:]
	rightPointers := interm.Pointers[splitIndex+1:]

	// Update left node
	interm.Keys = interm.Keys[:splitIndex]
	interm.Pointers = interm.Pointers[:splitIndex+1]
	if err := t.indexFile.writeNode(interm, pageID); err != nil {
		return nil, 0, err
	}

	// Create right internal node
	rightInterm := &tree.IntermNode[K, V]{
		Keys:     rightKeys,
		Pointers: rightPointers,
	}

	// Allocate page for right node
	rightPageID, err := t.indexFile.allocatePage()
	if err != nil {
		return nil, 0, err
	}

	// Write right node to disk
	if err := t.indexFile.writeNode(rightInterm, rightPageID); err != nil {
		return nil, 0, err
	}

	// Return promoted key and right page ID
	return &midKey, rightPageID, nil
}

// Search searches for a key in the tree and returns its associated value
func (t *DiskTree[K, V]) Search(key K) (V, error) {
	rootPageID := t.indexFile.GetRoot()
	if rootPageID == 0 {
		var zero V
		return zero, errors.New("tree is empty")
	}

	// Load root node
	root, err := t.indexFile.readNode(rootPageID)
	if err != nil {
		var zero V
		return zero, fmt.Errorf("failed to load root node: %w", err)
	}

	return t.dfs(key, root)
}

// RangeSearch searches for all key-value pairs in the range [startKey, endKey)
func (t *DiskTree[K, V]) RangeSearch(startKey, endKey K) ([]tree.LeafPair[K, V], error) {
	rootPageID := t.indexFile.GetRoot()
	if rootPageID == 0 {
		return nil, errors.New("tree is empty")
	}

	// Load root node
	root, err := t.indexFile.readNode(rootPageID)
	if err != nil {
		return nil, fmt.Errorf("failed to load root node: %w", err)
	}

	// Find the leftmost leaf that could contain startKey
	leftmostLeaf, err := t.findLeftmostLeaf(root)
	if err != nil {
		return nil, err
	}

	var results []tree.LeafPair[K, V]
	currentLeaf := leftmostLeaf

	// Traverse leaf nodes and collect results
	for currentLeaf != nil {
		for _, pair := range currentLeaf.Pairs {
			// Check if key is in range [startKey, endKey)
			if !pair.K.Less(startKey) && pair.K.Less(endKey) {
				results = append(results, pair)
			}
			// If we've passed endKey, we're done
			if !pair.K.Less(endKey) {
				return results, nil
			}
		}

		// Move to next leaf
		if currentLeaf.GetNextPage() != 0 {
			nextLeaf, err := t.indexFile.readNode(currentLeaf.GetNextPage())
			if err != nil {
				return nil, fmt.Errorf("failed to load next leaf: %w", err)
			}
			nextLeafNode, ok := nextLeaf.(*tree.LeafNode[K, V])
			if !ok {
				return nil, errors.New("expected leaf node")
			}
			currentLeaf = nextLeafNode
		} else {
			currentLeaf = nil // No more leaves
		}
	}

	return results, nil
}

// Min returns the minimum key-value pair in the tree
func (t *DiskTree[K, V]) Min() (*tree.LeafPair[K, V], error) {
	rootPageID := t.indexFile.GetRoot()
	if rootPageID == 0 {
		return nil, errors.New("tree is empty")
	}

	// Load root node
	root, err := t.indexFile.readNode(rootPageID)
	if err != nil {
		return nil, fmt.Errorf("failed to load root node: %w", err)
	}

	// Find leftmost leaf
	leftmostLeaf, err := t.findLeftmostLeaf(root)
	if err != nil {
		return nil, err
	}

	if len(leftmostLeaf.Pairs) == 0 {
		return nil, errors.New("leftmost leaf is empty")
	}

	return &leftmostLeaf.Pairs[0], nil
}

// Max returns the maximum key-value pair in the tree
func (t *DiskTree[K, V]) Max() (*tree.LeafPair[K, V], error) {
	rootPageID := t.indexFile.GetRoot()
	if rootPageID == 0 {
		return nil, errors.New("tree is empty")
	}

	// Load root node
	root, err := t.indexFile.readNode(rootPageID)
	if err != nil {
		return nil, fmt.Errorf("failed to load root node: %w", err)
	}

	// Find rightmost leaf
	rightmostLeaf, err := t.findRightmostLeaf(root)
	if err != nil {
		return nil, err
	}

	if len(rightmostLeaf.Pairs) == 0 {
		return nil, errors.New("rightmost leaf is empty")
	}

	return &rightmostLeaf.Pairs[len(rightmostLeaf.Pairs)-1], nil
}

// findLeftmostLeaf finds the leftmost leaf node starting from the given node
func (t *DiskTree[K, V]) findLeftmostLeaf(node tree.Node[V]) (*tree.LeafNode[K, V], error) {
	// Check if it's a leaf node using type assertion
	if leaf, ok := node.(*tree.LeafNode[K, V]); ok {
		return leaf, nil
	}

	// Internal node - find leftmost child
	interm, ok := node.(*tree.IntermNode[K, V])
	if !ok {
		return nil, errors.New("expected an internal node")
	}

	if len(interm.Pointers) == 0 {
		return nil, errors.New("internal node has no children")
	}

	// Load leftmost child
	leftmostChildPageID := interm.Pointers[0]
	leftmostChild, err := t.indexFile.readNode(leftmostChildPageID)
	if err != nil {
		return nil, fmt.Errorf("failed to load leftmost child: %w", err)
	}

	// Recursively find leftmost leaf
	return t.findLeftmostLeaf(leftmostChild)
}

// findRightmostLeaf finds the rightmost leaf node starting from the given node
func (t *DiskTree[K, V]) findRightmostLeaf(node tree.Node[V]) (*tree.LeafNode[K, V], error) {
	// Check if it's a leaf node using type assertion
	if leaf, ok := node.(*tree.LeafNode[K, V]); ok {
		return leaf, nil
	}

	// Internal node - find rightmost child
	interm, ok := node.(*tree.IntermNode[K, V])
	if !ok {
		return nil, errors.New("expected an internal node")
	}

	if len(interm.Pointers) == 0 {
		return nil, errors.New("internal node has no children")
	}

	// Load rightmost child
	rightmostChildPageID := interm.Pointers[len(interm.Pointers)-1]
	rightmostChild, err := t.indexFile.readNode(rightmostChildPageID)
	if err != nil {
		return nil, fmt.Errorf("failed to load rightmost child: %w", err)
	}

	// Recursively find rightmost leaf
	return t.findRightmostLeaf(rightmostChild)
}

// dfs recursively searches for a key in the tree, loading nodes from disk as needed
func (t *DiskTree[K, V]) dfs(key K, node tree.Node[V]) (V, error) {
	// Check if it's a leaf node using type assertion
	if _, ok := node.(*tree.LeafNode[K, V]); !ok {
		// Internal node - find child to recurse into
		interm, ok := node.(*tree.IntermNode[K, V])
		if !ok {
			var zero V
			return zero, errors.New("expected an internal node")
		}

		// Find the appropriate child index
		index := t.upperBound(key, interm.Keys)
		if index >= len(interm.Pointers) {
			var zero V
			return zero, errors.New("invalid child index in internal node")
		}

		// Load child node from disk
		childPageID := interm.Pointers[index]
		child, err := t.indexFile.readNode(childPageID)
		if err != nil {
			var zero V
			return zero, fmt.Errorf("failed to load child node: %w", err)
		}

		// Recursively search in child
		return t.dfs(key, child)
	}

	// Leaf node - search for the key
	leaf, ok := node.(*tree.LeafNode[K, V])
	if !ok {
		var zero V
		return zero, errors.New("expected a leaf node")
	}

	// Binary search in leaf pairs
	ind := t.leafBinarySearch(key, leaf.Pairs)
	if ind == -1 {
		var zero V
		return zero, errors.New("key not found")
	}

	return leaf.Pairs[ind].Value, nil
}

// leafBinarySearch performs binary search on leaf pairs to find a key
func (t *DiskTree[K, V]) leafBinarySearch(key K, pairs []tree.LeafPair[K, V]) int {
	left, right := 0, len(pairs)-1

	for left <= right {
		mid := left + (right-left)/2
		if pairs[mid].K.Equal(key) {
			return mid
		}
		if pairs[mid].K.Less(key) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return -1 // Key not found
}

// Helper methods (these need to be implemented or imported from tree package)
func (t *DiskTree[K, V]) leafUpperBound(key K, pairs []tree.LeafPair[K, V]) int {
	// Binary search to find the first pair with key >= target
	left, right := 0, len(pairs)

	for left < right {
		mid := left + (right-left)/2
		if pairs[mid].K.Less(key) {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left
}

func (t *DiskTree[K, V]) upperBound(key K, keys []K) int {
	// Return first index with keys[i] > key
	left, right := 0, len(keys)
	for left < right {
		mid := left + (right-left)/2
		// if key >= keys[mid] then go right
		if !key.Less(keys[mid]) { // key >= keys[mid]
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}


// insertAt is a helper function to insert an element at a specific index
func insertAt[T any](slice []T, index int, elem T) []T {
	if index < 0 || index > len(slice) {
		return slice
	}

	result := make([]T, len(slice)+1)
	copy(result[:index], slice[:index])
	result[index] = elem
	copy(result[index+1:], slice[index:])
	return result
}

// Print displays the tree structure level by level
func (t *DiskTree[K, V]) Print() error {
	rootPageID := t.indexFile.GetRoot()
	if rootPageID == 0 {
		fmt.Println("Tree is empty")
		return nil
	}

	// Load root node
	root, err := t.indexFile.readNode(rootPageID)
	if err != nil {
		return fmt.Errorf("failed to load root node: %w", err)
	}

	type LevelNode struct {
		node   tree.Node[V]
		pageID uint32
		level  int
	}

	queue := []LevelNode{{root, rootPageID, 0}}
	currentLevel := 0
	fmt.Printf("Level %d: ", currentLevel)

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		if item.level != currentLevel {
			currentLevel = item.level
			fmt.Println()
			fmt.Printf("Level %d: ", currentLevel)
		}

		// Check if it's a leaf node using type assertion
		if leaf, ok := item.node.(*tree.LeafNode[K, V]); ok {
			fmt.Printf("[Page %d: ", item.pageID)
			for _, pair := range leaf.Pairs {
				fmt.Printf("(%v: %v) ", pair.K, pair.Value)
			}
			fmt.Print("] ")
		} else {
			// Internal node
			interm, ok := item.node.(*tree.IntermNode[K, V])
			if !ok {
				return errors.New("expected an internal node")
			}
			fmt.Printf("[Page %d: ", item.pageID)
			for _, k := range interm.Keys {
				fmt.Printf("%v ", k)
			}
			fmt.Print("] ")

			// Add children to queue
			for _, childPageID := range interm.Pointers {
				child, err := t.indexFile.readNode(childPageID)
				if err != nil {
					return fmt.Errorf("failed to load child node %d: %w", childPageID, err)
				}
				queue = append(queue, LevelNode{child, childPageID, item.level + 1})
			}
		}
	}
	fmt.Println()
	return nil
}
