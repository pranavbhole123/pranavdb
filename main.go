package main

import (
	"fmt"
	"pranavdb/tree"
)

func main() {
	fmt.Println("Testing B+ Tree...")

	// Initialize the tree with order 3 (small order for easy splitting)
	tr, err := tree.NewTree[tree.IntKey, string](3)
	if err != nil {
		panic(err)
	}

	// Insert fake key-value pairs
	_ = tr.Insert(tree.IntKey(10), "val10")
	_ = tr.Insert(tree.IntKey(20), "val20")
	_ = tr.Insert(tree.IntKey(5), "val5")
	_ = tr.Insert(tree.IntKey(15), "val15")
	_ = tr.Insert(tree.IntKey(25), "val25")
	_ = tr.Insert(tree.IntKey(1), "val1")

	fmt.Println("Tree after insertion:")
	tr.Print()

	// Test search
	val, err := tr.Search(tree.IntKey(15))
	if err != nil {
		fmt.Println("Search 15 → not found")
	} else {
		fmt.Println("Search 15 →", val)
	}
	val, err = tr.Search(tree.IntKey(100))
	if err != nil {
		fmt.Println("Search 100 → not found")
	} else {
		fmt.Println("Search 100 →", val)
	}

	// Test deletion
	fmt.Println("\nTesting deletion...")

	// Delete a key that exists
	err = tr.Delete(tree.IntKey(15))
	if err != nil {
		fmt.Println("Delete 15 failed:", err)
	} else {
		fmt.Println("Successfully deleted key 15")
	}

	// Try to search for deleted key
	val, err = tr.Search(tree.IntKey(15))
	if err != nil {
		fmt.Println("Search 15 after deletion → not found")
	} else {
		fmt.Println("Search 15 after deletion →", val)
	}

	// Delete another key
	err = tr.Delete(tree.IntKey(5))
	if err != nil {
		fmt.Println("Delete 5 failed:", err)
	} else {
		fmt.Println("Successfully deleted key 5")
	}

	fmt.Println("\nTree after deletions:")
	tr.Print()

	// Try to delete a non-existent key
	err = tr.Delete(tree.IntKey(100))
	if err != nil {
		fmt.Println("Delete 100 failed (expected):", err)
	} else {
		fmt.Println("Unexpectedly deleted key 100")
	}
}
