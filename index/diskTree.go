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


// Max returns the maximum key-value pair in the tree


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

	// Check if the leaf node is deleted
	

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
			deletedStatus := ""
			/////////////////////////////////////////////////////////////////////////////////////////////////////////////
			fmt.Printf("[Page %d%s: ", item.pageID, deletedStatus)
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
			deletedStatus := ""
			//////////////////////////////////////////////////////////////////////////////////////////////////////////////
			fmt.Printf("[Page %d%s: ", item.pageID, deletedStatus)
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

// Delete removes a key-value pair from the disk B+ tree.
func (t *DiskTree[K, V]) Delete(key K) error {
	// Check empty
	rootPageID := t.indexFile.GetRoot()
	if rootPageID == 0 {
		return errors.New("tree is empty")
	}

	// Ensure key exists first (optional but safe)
	_, err := t.Search(key)
	if err != nil {
		return err
	}

	// Perform recursive delete starting at root
	underflow, err := t.deleteRecursive(key, rootPageID)
	if err != nil {
		return err
	}

	// Handle root underflow: if root is internal and becomes empty, make its only child the root
	if underflow {
		rootNode, err := t.indexFile.readNode(rootPageID)
		if err != nil {
			return err
		}
		if interm, ok := rootNode.(*tree.IntermNode[K, V]); ok {
			// If no keys left but has one pointer, promote that child as root
			if len(interm.Keys) == 0 && len(interm.Pointers) == 1 {
				if err := t.indexFile.SetRoot(interm.Pointers[0]); err != nil {
					return err
				}
				// Optionally free old root page
				//tryFreePage(t.indexFile, rootPageID)
				t.indexFile.freePage(rootPageID)
			}
		}
	}

	return nil
}

// deleteRecursive deletes key starting at pageID. Returns whether caller (this node) underflows.
func (t *DiskTree[K, V]) deleteRecursive(key K, pageID uint32) (bool, error) {
	node, err := t.indexFile.readNode(pageID)
	if err != nil {
		return false, err
	}

	if leaf, ok := node.(*tree.LeafNode[K, V]); ok {
		return t.deleteFromLeaf(key, leaf, pageID)
	}
	return t.deleteFromInternal(key, node.(*tree.IntermNode[K, V]), pageID)
}

func (t *DiskTree[K, V]) deleteFromLeaf(key K, leaf *tree.LeafNode[K, V], pageID uint32) (bool, error) {
	// Find the key to delete using exact-match binary search
	index := t.leafBinarySearch(key, leaf.Pairs)
	if index == -1 {
		// Key not found
		return false, nil
	}

	// Remove the key-value pair
	leaf.Pairs = removeAtLeafPair(leaf.Pairs, index)

	// Write the leaf back to disk
	if err := t.indexFile.writeNode(leaf, pageID); err != nil {
		return false, err
	}

	// Check for underflow using same rule as memory version
	minKeys := (t.order - 1) / 2
	return len(leaf.Pairs) < minKeys, nil
}

// deleteFromInternal handles deletion from an internal node.
func (t *DiskTree[K, V]) deleteFromInternal(key K, interm *tree.IntermNode[K, V], pageID uint32) (bool, error) {
	// choose child (use same upperBound semantics used elsewhere)
	childIndex := t.upperBound(key, interm.Keys)
	if childIndex >= len(interm.Pointers) {
		return false, errors.New("invalid child index")
	}

	childPageID := interm.Pointers[childIndex]

	// recurse
	childUnderflow, err := t.deleteRecursive(key, childPageID)
	if err != nil {
		return false, err
	}
	if !childUnderflow {
		// no structural change required; still might need to update separators if we deleted first key from a leaf earlier
		// but deleteFromLeaf handles updating parents when removing first key and no underflow
		return false, nil
	}

	// child underflow -> try borrow or merge
	return t.handleUnderflow(interm, pageID, childIndex)
}

// handleUnderflow tries borrow from siblings or merges and returns whether this node underflows.
func (t *DiskTree[K, V]) handleUnderflow(node *tree.IntermNode[K, V], nodePageID uint32, childIndex int) (bool, error) {
	// try borrow from left sibling
	if childIndex > 0 {
		leftPageID := node.Pointers[childIndex-1]
		if t.canBorrowFrom(leftPageID) {
			if err := t.borrowFromLeft(node, nodePageID, childIndex); err != nil {
				return false, err
			}
			if err := t.indexFile.writeNode(node, nodePageID); err != nil {
				return false, err
			}
			return false, nil
		}
	}

	// try borrow from right sibling
	if childIndex < len(node.Pointers)-1 {
		rightPageID := node.Pointers[childIndex+1]
		if t.canBorrowFrom(rightPageID) {
			if err := t.borrowFromRight(node, nodePageID, childIndex); err != nil {
				return false, err
			}
			if err := t.indexFile.writeNode(node, nodePageID); err != nil {
				return false, err
			}
			return false, nil
		}
	}

	// can't borrow -> merge
	// prefer merge with left if exists
	if childIndex > 0 {
		// merge child (childIndex) into left sibling (childIndex-1)
		if err := t.mergeLeft(node, nodePageID, childIndex); err != nil {
			return false, err
		}
		// Remove separator key and pointer for childIndex
		node.Keys = removeAtK(node.Keys, childIndex-1)
		node.Pointers = removeAtUint32(node.Pointers, childIndex)
		// write node
		if err := t.indexFile.writeNode(node, nodePageID); err != nil {
			return false, err
		}
	} else if childIndex < len(node.Pointers)-1 {
		// merge right sibling into child
		if err := t.mergeRight(node, nodePageID, childIndex); err != nil {
			return false, err
		}
		// remove separator key at childIndex and remove right pointer
		node.Keys = removeAtK(node.Keys, childIndex)
		node.Pointers = removeAtUint32(node.Pointers, childIndex+1)
		if err := t.indexFile.writeNode(node, nodePageID); err != nil {
			return false, err
		}
	} else {
		return false, errors.New("no sibling to merge with; inconsistent state")
	}

	// check for underflow in this internal node
	minKeys := (t.order - 1) / 2
	if len(node.Keys) < minKeys {
		return true, nil
	}
	return false, nil
}

// canBorrowFrom checks if the node at page has > minKeys (so can lend)
func (t *DiskTree[K, V]) canBorrowFrom(pageID uint32) bool {
	n, err := t.indexFile.readNode(pageID)
	if err != nil {
		// if we can't read treat as not borrowable
		return false
	}
	if leaf, ok := n.(*tree.LeafNode[K, V]); ok {
		minKeys := (t.order - 1) / 2
		return len(leaf.Pairs) > minKeys
	}
	if interm, ok := n.(*tree.IntermNode[K, V]); ok {
		minKeys := (t.order - 1) / 2
		return len(interm.Keys) > minKeys
	}
	return false
}

// borrowFromLeft borrows one item from left sibling into child at childIndex.
func (t *DiskTree[K, V]) borrowFromLeft(parent *tree.IntermNode[K, V], parentPageID uint32, childIndex int) error {
	leftPageID := parent.Pointers[childIndex-1]
	childPageID := parent.Pointers[childIndex]

	leftNode, err := t.indexFile.readNode(leftPageID)
	if err != nil {
		return err
	}
	childNode, err := t.indexFile.readNode(childPageID)
	if err != nil {
		return err
	}

	// If leaves
	if leftLeaf, ok := leftNode.(*tree.LeafNode[K, V]); ok {
		childLeaf := childNode.(*tree.LeafNode[K, V])

		// move rightmost element from leftLeaf to beginning of childLeaf
		borrowed := leftLeaf.Pairs[len(leftLeaf.Pairs)-1]
		leftLeaf.Pairs = leftLeaf.Pairs[:len(leftLeaf.Pairs)-1]
		childLeaf.Pairs = insertAt(childLeaf.Pairs, 0, borrowed)

		// update parent separator to reflect new smallest key in child
		parent.Keys[childIndex-1] = childLeaf.Pairs[0].K

		// write modified nodes
		if err := t.indexFile.writeNode(leftLeaf, leftPageID); err != nil {
			return err
		}
		if err := t.indexFile.writeNode(childLeaf, childPageID); err != nil {
			return err
		}
		return nil
	}

	// Internal nodes
	leftInterm := leftNode.(*tree.IntermNode[K, V])
	childInterm := childNode.(*tree.IntermNode[K, V])

	// move rightmost key/pointer from leftInterm to front of childInterm
	bKey := leftInterm.Keys[len(leftInterm.Keys)-1]
	bPtr := leftInterm.Pointers[len(leftInterm.Pointers)-1]
	leftInterm.Keys = leftInterm.Keys[:len(leftInterm.Keys)-1]
	leftInterm.Pointers = leftInterm.Pointers[:len(leftInterm.Pointers)-1]

	childInterm.Keys = insertAt(childInterm.Keys, 0, bKey)
	childInterm.Pointers = insertAtUint32(childInterm.Pointers, 0, bPtr)

	// update parent separator
	parent.Keys[childIndex-1] = bKey

	// write modified nodes
	if err := t.indexFile.writeNode(leftInterm, leftPageID); err != nil {
		return err
	}
	if err := t.indexFile.writeNode(childInterm, childPageID); err != nil {
		return err
	}
	return nil
}

// borrowFromRight borrows one item from right sibling into child at childIndex.
func (t *DiskTree[K, V]) borrowFromRight(parent *tree.IntermNode[K, V], parentPageID uint32, childIndex int) error {
	rightPageID := parent.Pointers[childIndex+1]
	childPageID := parent.Pointers[childIndex]

	rightNode, err := t.indexFile.readNode(rightPageID)
	if err != nil {
		return err
	}
	childNode, err := t.indexFile.readNode(childPageID)
	if err != nil {
		return err
	}

	// leaves
	if rightLeaf, ok := rightNode.(*tree.LeafNode[K, V]); ok {
		childLeaf := childNode.(*tree.LeafNode[K, V])

		// move leftmost element from right to end of child
		borrowed := rightLeaf.Pairs[0]
		rightLeaf.Pairs = rightLeaf.Pairs[1:]
		childLeaf.Pairs = append(childLeaf.Pairs, borrowed)

		// update parent separator to new smallest key in right sibling
		if len(rightLeaf.Pairs) > 0 {
			parent.Keys[childIndex] = rightLeaf.Pairs[0].K
		} else {
			// right leaf became empty — unusual; handled in merge path usually
		}

		// write modified nodes
		if err := t.indexFile.writeNode(rightLeaf, rightPageID); err != nil {
			return err
		}
		if err := t.indexFile.writeNode(childLeaf, childPageID); err != nil {
			return err
		}
		return nil
	}

	// internal nodes
	rightInterm := rightNode.(*tree.IntermNode[K, V])
	childInterm := childNode.(*tree.IntermNode[K, V])

	// move leftmost key/pointer from rightInterm to end of childInterm
	bKey := rightInterm.Keys[0]
	bPtr := rightInterm.Pointers[0]
	rightInterm.Keys = rightInterm.Keys[1:]
	rightInterm.Pointers = rightInterm.Pointers[1:]

	childInterm.Keys = append(childInterm.Keys, bKey)
	childInterm.Pointers = append(childInterm.Pointers, bPtr)

	// update parent separator to reflect new smallest in rightInterm
	if len(rightInterm.Keys) > 0 {
		parent.Keys[childIndex] = rightInterm.Keys[0]
	}

	// write modified nodes
	if err := t.indexFile.writeNode(rightInterm, rightPageID); err != nil {
		return err
	}
	if err := t.indexFile.writeNode(childInterm, childPageID); err != nil {
		return err
	}
	return nil
}

// mergeLeft merges child at childIndex into left sibling (childIndex-1)
func (t *DiskTree[K, V]) mergeLeft(parent *tree.IntermNode[K, V], parentPageID uint32, childIndex int) error {
	leftPageID := parent.Pointers[childIndex-1]
	childPageID := parent.Pointers[childIndex]

	leftNode, err := t.indexFile.readNode(leftPageID)
	if err != nil {
		return err
	}
	childNode, err := t.indexFile.readNode(childPageID)
	if err != nil {
		return err
	}

	// If leaves: append child pairs to left leaf
	if leftLeaf, ok := leftNode.(*tree.LeafNode[K, V]); ok {
		childLeaf := childNode.(*tree.LeafNode[K, V])
		leftLeaf.Pairs = append(leftLeaf.Pairs, childLeaf.Pairs...)

		// fix linked list pointers
		leftLeaf.SetNextPage(childLeaf.GetNextPage())
		if childLeaf.GetNextPage() != 0 {
			nextNode, err := t.indexFile.readNode(childLeaf.GetNextPage())
			if err == nil {
				if nextLeaf, ok := nextNode.(*tree.LeafNode[K, V]); ok {
					nextLeaf.SetPrevPage(leftPageID)
					_ = t.indexFile.writeNode(nextLeaf, childLeaf.GetNextPage())
				}
			}
		}

		// write merged left
		if err := t.indexFile.writeNode(leftLeaf, leftPageID); err != nil {
			return err
		}

		// mark child deleted and try free
		// we need to make a function to add  the page to the 
		
		//childLeaf.SetDeleted(true)
		//t.indexFile.freePage(childLeaf.GetPageID())


		if err := t.indexFile.writeNode(childLeaf, childPageID); err != nil {
			return err
		}
		//tryFreePage(t.indexFile, childPageID)
		t.indexFile.freePage(childPageID)
		return nil
	}

	// internal nodes: move separator key and child's keys/pointers
	leftInterm := leftNode.(*tree.IntermNode[K, V])
	childInterm := childNode.(*tree.IntermNode[K, V])

	separator := parent.Keys[childIndex-1]
	leftInterm.Keys = append(leftInterm.Keys, separator)
	leftInterm.Keys = append(leftInterm.Keys, childInterm.Keys...)
	leftInterm.Pointers = append(leftInterm.Pointers, childInterm.Pointers...)

	if err := t.indexFile.writeNode(leftInterm, leftPageID); err != nil {
		return err
	}

	//childInterm.SetDeleted(true)
	//t.indexFile.freePage(childInterm.GetPageID())

	if err := t.indexFile.writeNode(childInterm, childPageID); err != nil {
		return err
	}
	//tryFreePage(t.indexFile, childPageID)
	t.indexFile.freePage(childPageID)
	return nil
}

// mergeRight merges right sibling into child at childIndex
func (t *DiskTree[K, V]) mergeRight(parent *tree.IntermNode[K, V], parentPageID uint32, childIndex int) error {
	childPageID := parent.Pointers[childIndex]
	rightPageID := parent.Pointers[childIndex+1]

	childNode, err := t.indexFile.readNode(childPageID)
	if err != nil {
		return err
	}
	rightNode, err := t.indexFile.readNode(rightPageID)
	if err != nil {
		return err
	}

	// leaves: append right pairs into child
	if childLeaf, ok := childNode.(*tree.LeafNode[K, V]); ok {
		rightLeaf := rightNode.(*tree.LeafNode[K, V])
		childLeaf.Pairs = append(childLeaf.Pairs, rightLeaf.Pairs...)

		// fix linked list pointers
		childLeaf.SetNextPage(rightLeaf.GetNextPage())
		if rightLeaf.GetNextPage() != 0 {
			nextNode, err := t.indexFile.readNode(rightLeaf.GetNextPage())
			if err == nil {
				if nextLeaf, ok := nextNode.(*tree.LeafNode[K, V]); ok {
					nextLeaf.SetPrevPage(childPageID)
					_ = t.indexFile.writeNode(nextLeaf, rightLeaf.GetNextPage())
				}
			}
		}

		// write merged child
		if err := t.indexFile.writeNode(childLeaf, childPageID); err != nil {
			return err
		}

		// mark right deleted and try free
		//rightLeaf.SetDeleted(true)
		//t.indexFile.freePage(rightLeaf.GetPageID())

		if err := t.indexFile.writeNode(rightLeaf, rightPageID); err != nil {
			return err
		}
		//tryFreePage(t.indexFile, rightPageID)
		t.indexFile.freePage(rightPageID)
		return nil
	}

	// internal nodes
	childInterm := childNode.(*tree.IntermNode[K, V])
	rightInterm := rightNode.(*tree.IntermNode[K, V])

	separator := parent.Keys[childIndex]
	childInterm.Keys = append(childInterm.Keys, separator)
	childInterm.Keys = append(childInterm.Keys, rightInterm.Keys...)
	childInterm.Pointers = append(childInterm.Pointers, rightInterm.Pointers...)

	if err := t.indexFile.writeNode(childInterm, childPageID); err != nil {
		return err
	}

	//rightInterm.SetDeleted(true)
	//t.indexFile.freePage(rightInterm.GetPageID())


	if err := t.indexFile.writeNode(rightInterm, rightPageID); err != nil {
		return err
	}
	//tryFreePage(t.indexFile, rightPageID)
	t.indexFile.freePage(rightPageID)

	return nil
}




// ---------- small helpers for slice removal/insert on concrete types ----------

func removeAtLeafPair[K tree.Key, V any](s []tree.LeafPair[K, V], i int) []tree.LeafPair[K, V] {
	if i < 0 || i >= len(s) {
		return s
	}
	return append(s[:i], s[i+1:]...)
}

func removeAtK[K tree.Key](s []K, i int) []K {
	if i < 0 || i >= len(s) {
		return s
	}
	return append(s[:i], s[i+1:]...)
}

func removeAtUint32(s []uint32, i int) []uint32 {
	if i < 0 || i >= len(s) {
		return s
	}
	return append(s[:i], s[i+1:]...)
}

func insertAtUint32(s []uint32, i int, v uint32) []uint32 {
	if i < 0 || i > len(s) {
		return s
	}
	res := make([]uint32, len(s)+1)
	copy(res[:i], s[:i])
	res[i] = v
	copy(res[i+1:], s[i:])
	return res
}
