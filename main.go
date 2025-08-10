package main

import (
	"fmt"
	"log"
	"os"
	"pranavdb/index"
	"pranavdb/tree"
)

func main() {
	// Test file path
	testFile := "test_index.idx"

	// Clean up any existing test file
	os.Remove(testFile)

	fmt.Println("=== Testing Disk-Based B+ Tree ===")
	fmt.Println("Creating new disk-based B+ tree...")

	// Create a new disk-based B+ tree with order 3
	diskTree, err := index.NewDiskTree[tree.IntKey, string](testFile, 3)
	if err != nil {
		log.Fatalf("Failed to create disk tree: %v", err)
	}
	defer diskTree.Close()

	fmt.Printf("Tree created with order: %d\n", diskTree.GetOrder())
	fmt.Printf("Initial root page ID: %d\n", diskTree.GetRoot())

	fmt.Println("\n=== Testing Insert Operations ===")

	// Insert test data
	testData := []struct {
		key   tree.IntKey
		value string
	}{
		{10, "ten"},
		{5, "five"},
		{15, "fifteen"},
		{3, "three"},
		{7, "seven"},
		{12, "twelve"},
		{18, "eighteen"},
		{1, "one"},
		{25, "twenty-five"},
		{30, "thirty"},
	}

	for _, data := range testData {
		fmt.Printf("Inserting key %d with value '%s'\n", data.key, data.value)
		if err := diskTree.Insert(data.key, data.value); err != nil {
			log.Fatalf("Failed to insert %d: %v", data.key, err)
		}
	}

	fmt.Printf("\nTree after all insertions:")
	fmt.Printf("Root page ID: %d\n", diskTree.GetRoot())
	fmt.Printf("File size: %d bytes\n", getFileSize(testFile))

	fmt.Println("\n=== Testing Print Function ===")
	if err := diskTree.Print(); err != nil {
		log.Printf("Failed to print tree: %v", err)
	}

	fmt.Println("\n=== Testing Search Operations ===")

	// Test individual searches
	searchTests := []tree.IntKey{1, 5, 10, 15, 25, 100}
	for _, searchKey := range searchTests {
		val, err := diskTree.Search(searchKey)
		if err != nil {
			fmt.Printf("Search %d → not found: %v\n", searchKey, err)
		} else {
			fmt.Printf("Search %d → found: %s\n", searchKey, val)
		}
	}

	fmt.Println("\n=== Testing Range Search ===")

	// Test range searches
	rangeTests := []struct {
		start, end tree.IntKey
		desc       string
	}{
		{5, 15, "keys in range [5, 15)"},
		{1, 20, "keys in range [1, 20)"},
		{25, 35, "keys in range [25, 35)"},
		{0, 100, "all keys in range [0, 100)"},
	}

	for _, test := range rangeTests {
		results, err := diskTree.RangeSearch(test.start, test.end)
		if err != nil {
			fmt.Printf("Range search %s failed: %v\n", test.desc, err)
		} else {
			fmt.Printf("Range search %s: found %d results\n", test.desc, len(results))
			for _, pair := range results {
				fmt.Printf("  (%d: %s) ", pair.K, pair.Value)
			}
			fmt.Println()
		}
	}

	fmt.Println("\n=== Testing Min/Max Operations ===")

	// Test minimum key
	minPair, err := diskTree.Min()
	if err != nil {
		fmt.Printf("Min operation failed: %v\n", err)
	} else {
		fmt.Printf("Minimum key: %d with value: %s\n", minPair.K, minPair.Value)
	}

	// Test maximum key
	maxPair, err := diskTree.Max()
	if err != nil {
		fmt.Printf("Max operation failed: %v\n", err)
	} else {
		fmt.Printf("Maximum key: %d with value: %s\n", maxPair.K, maxPair.Value)
	}

	fmt.Println("\n=== Testing Tree Persistence ===")

	// Close the current tree
	diskTree.Close()

	// Try to open the existing tree
	fmt.Println("Opening existing tree...")
	existingTree, err := index.OpenDiskTree[tree.IntKey, string](testFile)
	if err != nil {
		log.Fatalf("Failed to open existing tree: %v", err)
	}
	defer existingTree.Close()

	fmt.Printf("Tree order: %d\n", existingTree.GetOrder())
	fmt.Printf("Root page ID: %d\n", existingTree.GetRoot())

	// Verify data persistence by searching for a key
	val, err := existingTree.Search(tree.IntKey(10))
	if err != nil {
		fmt.Printf("Search 10 after reopening → not found: %v\n", err)
	} else {
		fmt.Printf("Search 10 after reopening → found: %s\n", val)
	}

	// Print the tree again to verify structure
	fmt.Println("\nTree structure after reopening:")
	if err := existingTree.Print(); err != nil {
		log.Printf("Failed to print reopened tree: %v", err)
	}

	fmt.Println("\n=== All Tests Completed Successfully! ===")
}

func getFileSize(filename string) int64 {
	info, err := os.Stat(filename)
	if err != nil {
		return 0
	}
	return info.Size()
}
