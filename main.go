package main

import (
	"fmt"
	"pranavdb/tree"
)

func main() {
	fmt.Println("Testing B+ Tree...")

	// Initialize the tree with an empty leaf node as root
	tr := &tree.Tree{
		Root:  &tree.LeafNode{},
		Order: 3, // small order for easy splitting
	}

	// Insert fake key-value pairs
	tr.Insert(tree.Key{Value: 10}, "val10")
	tr.Insert(tree.Key{Value: 20}, "val20")
	tr.Insert(tree.Key{Value: 5}, "val5")
	tr.Insert(tree.Key{Value: 15}, "val15")
	tr.Insert(tree.Key{Value: 25}, "val25")
	tr.Insert(tree.Key{Value: 1}, "val1")

	// Print the tree structure
	tr.Print()

	// Test search
	fmt.Println("Search 15 →", tr.Search(tree.Key{Value: 15}))
	fmt.Println("Search 100 →", tr.Search(tree.Key{Value: 100}))
}
