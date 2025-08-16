package main

import (
	"fmt"
	"log"
	"os"
	"pranavdb/index"
	"pranavdb/tree"
	"pranavdb/data"
)

func main() {
	// Test file path
	testFile := "test_index.idx"

	// Clean up any existing test file
	os.Remove(testFile)

	fmt.Println("=== Testing Disk-Based B+ Tree ===")
	fmt.Println("Creating new disk-based B+ tree...")

	// Create a new disk-based B+ tree with order 3
	diskTree, err := index.NewDiskTree[tree.IntKey, string](testFile, 5)
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



	// Test deletion of existing keys
	deleteTests := []tree.IntKey{10,5}
	for _, deleteKey := range deleteTests {
		fmt.Printf("Deleting key %d...\n", deleteKey)
		if err := diskTree.Delete(deleteKey); err != nil {
			fmt.Printf("Delete %d failed: %v\n", deleteKey, err)
		} else {
			fmt.Printf("Successfully deleted key %d\n", deleteKey)
		}
	}

	// Try to delete a non-existent key
	if err := diskTree.Delete(tree.IntKey(100)); err != nil {
		fmt.Printf("Delete 100 failed (expected): %v\n", err)
	} else {
		fmt.Println("Unexpectedly deleted key 100")
	}

	// Print tree after deletions
	fmt.Println("\nTree after deletions:")
	if err := diskTree.Print(); err != nil {
		log.Printf("Failed to print tree after deletions: %v", err)
	}

	// Test search after deletion
	fmt.Println("\n=== Testing Search After Deletion ===")
	searchAfterDeleteTests := []tree.IntKey{15, 5, 25, 10, 1}
	for _, searchKey := range searchAfterDeleteTests {
		val, err := diskTree.Search(searchKey)
		if err != nil {
			fmt.Printf("Search %d after deletion → not found: %v\n", searchKey, err)
		} else {
			fmt.Printf("Search %d after deletion → found: %s\n", searchKey, val)
		}
	}

	// Test Min/Max after deletion
	
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


	// now we test the working of our free list try inserting 31 and if the pageid 2 is use we kknow our free list is working

	_ = existingTree.Insert(tree.IntKey(31), "thirtyone")
	_ = existingTree.Insert(tree.IntKey(32), "thirtytwo")
	_ = existingTree.Insert(tree.IntKey(33), "thirtythree")
	err = existingTree.Insert(tree.IntKey(34), "thirtyfour")

	if err != nil{
		fmt.Println(err)
	}
	if err := existingTree.Print(); err != nil {
		log.Printf("Failed to print reopened tree: %v", err)
	}
	fmt.Println("\n=== All Tests Completed Successfully! ===")


	//////////////////////////////////////////////////////////////////////////////////////////////////row

	const fn = "test_rows.dat"

	// clean up old test file if present
	_ = os.Remove(fn)

	// create rowfile with schema: int, string, float
	rf, err := data.NewRowfile(fn, "int,string,float")
	if err != nil {
		log.Fatalf("NewRowfile failed: %v", err)
	}
	defer rf.Close()
	fmt.Printf("Created rowfile %q with schema: %s (columns: %d)\n",
		fn, data.SchemaStringFromCodes(rf.GetSchemaCodes()), rf.GetColumnCount())

	// prepare rows (values must match schema exactly: int, string, float64)
	rows := [][]any{
		{42, "hello", 3.14159},
		{7, "world", 2.71828},
		{1000, "this is a longer string", 1.41421},
	}

	var offsets []int64
	for i, r := range rows {
		off, err := rf.WriteRow(r)
		if err != nil {
			log.Fatalf("WriteRow #%d failed: %v", i, err)
		}
		fmt.Printf("Wrote row #%d at offset %d\n", i, off)
		offsets = append(offsets, off)
	}

	// Read back rows immediately
	fmt.Println("\nReading back rows (immediate):")
	for i, off := range offsets {
		vals, err := rf.ReadRowAt(off)
		if err != nil {
			log.Fatalf("ReadRowAt #%d failed: %v", i, err)
		}
		// decodeRow returns int32 for INT, float64 for FLOAT, string for STRING
		ival := vals[0].(int32)
		sval := vals[1].(string)
		fval := vals[2].(float64)
		fmt.Printf("Row #%d @%d -> int=%d, string=%q, float=%f\n", i, off, int(ival), sval, fval)
	}
	// ✅ DELETE one row
	fmt.Printf("\nDeleting row #1 at offset %d...\n", offsets[1])
	if err := rf.FreeRowAt(offsets[1]); err != nil {
		log.Fatalf("FreeRowAt failed: %v", err)
	}

//fmt.Printf("Before insertion, firstFreePage = %d\n", rf.GetFirstFreePage())
	// ✅ INSERT a new row (should reuse the freed slot)
	//// keep in mind the row to be inserted should be shorter than the row deleted  ///////////////////////////////////////////////////////
	newRow := []any{99, "r", 1.0}
	newOff, err := rf.WriteRow(newRow)
	if err != nil {
		log.Fatalf("WriteRow (after delete) failed: %v", err)
	}
	fmt.Printf("Inserted new row into offset %d (should match deleted slot %d)\n", newOff, offsets[1])

	// ✅ READ BACK the reused slot
	vals, err := rf.ReadRowAt(newOff)
	if err != nil {
		log.Fatalf("ReadRowAt (reused slot) failed: %v", err)
	}
	fmt.Printf("Reused slot row @%d -> int=%d, string=%q, float=%f\n",
		newOff, vals[0].(int32), vals[1].(string), vals[2].(float64))

	fmt.Println("\nAll tests completed successfully.")


	// Close and reopen to test persistence + header reading
	if err := rf.Close(); err != nil {
		log.Fatalf("close failed: %v", err)
	}

	rf2, err := data.OpenRowfile(fn)
	if err != nil {
		log.Fatalf("OpenRowfile failed: %v", err)
	}
	defer rf2.Close()
	fmt.Printf("\nReopened rowfile %q with schema: %s (columns: %d)\n",
		fn, data.SchemaStringFromCodes(rf2.GetSchemaCodes()), rf2.GetColumnCount())

	// Read rows again after reopen
	fmt.Println("\nReading back rows (after reopen):")
	for i, off := range offsets {
		vals, err := rf2.ReadRowAt(off)
		if err != nil {
			log.Fatalf("ReadRowAt after reopen #%d failed: %v", i, err)
		}
		ival := vals[0].(int32)
		sval := vals[1].(string)
		fval := vals[2].(float64)
		fmt.Printf("Row #%d @%d -> int=%d, string=%q, float=%f\n", i, off, int(ival), sval, fval)
	}

	fmt.Println("\nAll tests completed successfully.")
}

func getFileSize(filename string) int64 {
	info, err := os.Stat(filename)
	if err != nil {
		return 0
	}
	return info.Size()
}
