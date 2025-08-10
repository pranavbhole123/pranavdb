package index

import (
	"encoding/binary"
	"fmt"
	"os"
	"pranavdb/page"
	"pranavdb/tree"
)

const (
	// File header constants
	MagicNumber = 0x42504C55 // "B+LU" in hex
	Version     = 1
	HeaderSize  = 512 // First block size

	// Page types for index pages
	PageTypeHeader = 0
	PageTypeNode   = 1
)

// IndexFile manages a disk-based B+ tree index file
type IndexFile[K tree.Key, V any] struct {
	file       *os.File
	rootPageID uint32
	order      int
	codec      *page.IndexPageCodec[K, V]
}

// FileHeader represents the header stored in the first block
type FileHeader struct {
	MagicNumber uint32
	Version     uint32
	RootPageID  uint32
	TreeOrder   uint32
}

// NewIndexFile creates a new index file with the given order
func NewIndexFile[K tree.Key, V any](filepath string, order int) (*IndexFile[K, V], error) {
	// Create the file
	file, err := os.Create(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to create index file: %w", err)
	}

	// Initialize the index file
	indexFile := &IndexFile[K, V]{
		file:       file,
		rootPageID: 0, // No root initially
		order:      order,
		codec:      page.NewIndexPageCodec[K, V](),
	}

	// Write the initial header
	if err := indexFile.writeHeader(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	return indexFile, nil
}

// OpenIndexFile opens an existing index file
func OpenIndexFile[K tree.Key, V any](filepath string) (*IndexFile[K, V], error) {
	file, err := os.OpenFile(filepath, os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	indexFile := &IndexFile[K, V]{
		file:  file,
		codec: page.NewIndexPageCodec[K, V](),
	}

	// Read the header
	if err := indexFile.readHeader(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	return indexFile, nil
}

// Close closes the index file and writes the final header
func (idx *IndexFile[K, V]) Close() error {
	if err := idx.writeHeader(); err != nil {
		return fmt.Errorf("failed to write final header: %w", err)
	}
	return idx.file.Close()
}

// writeHeader writes the file header to the first block
func (idx *IndexFile[K, V]) writeHeader() error {
	header := FileHeader{
		MagicNumber: MagicNumber,
		Version:     Version,
		RootPageID:  idx.rootPageID,
		TreeOrder:   uint32(idx.order),
	}

	// Create header block
	headerBlock := make([]byte, HeaderSize)

	// Write header fields
	binary.LittleEndian.PutUint32(headerBlock[0:4], header.MagicNumber)
	binary.LittleEndian.PutUint32(headerBlock[4:8], header.Version)
	binary.LittleEndian.PutUint32(headerBlock[8:12], header.RootPageID)
	binary.LittleEndian.PutUint32(headerBlock[12:16], header.TreeOrder)

	// Rest is reserved (zeroed)

	// Write to file
	_, err := idx.file.WriteAt(headerBlock, 0)
	return err
}

// readHeader reads the file header from the first block
func (idx *IndexFile[K, V]) readHeader() error {
	headerBlock := make([]byte, HeaderSize)

	_, err := idx.file.ReadAt(headerBlock, 0)
	if err != nil {
		return err
	}

	// Read header fields
	magic := binary.LittleEndian.Uint32(headerBlock[0:4])
	version := binary.LittleEndian.Uint32(headerBlock[4:8])
	rootPageID := binary.LittleEndian.Uint32(headerBlock[8:12])
	treeOrder := binary.LittleEndian.Uint32(headerBlock[12:16])

	// Validate header
	if magic != MagicNumber {
		return fmt.Errorf("invalid magic number: expected %x, got %x", MagicNumber, magic)
	}
	if version != Version {
		return fmt.Errorf("unsupported version: %d", version)
	}

	idx.rootPageID = rootPageID
	idx.order = int(treeOrder)

	return nil
}

// allocatePage allocates a new page and returns its page ID
func (idx *IndexFile[K, V]) allocatePage() (uint32, error) {
	// Get file size to determine next page ID
	info, err := idx.file.Stat()
	if err != nil {
		return 0, err
	}

	// Calculate next page ID (after header block)
	nextPageID := uint32((info.Size()-HeaderSize)/page.PageSize) + 1

	// Extend file if needed
	requiredSize := int64(HeaderSize + (nextPageID+1)*page.PageSize)
	if info.Size() < requiredSize {
		// Write a zero page to extend the file
		zeroPage := make([]byte, page.PageSize)
		_, err = idx.file.WriteAt(zeroPage, int64(HeaderSize+nextPageID*page.PageSize))
		if err != nil {
			return 0, err
		}
	}

	return nextPageID, nil
}

// writeNode writes a node to a specific page
func (idx *IndexFile[K, V]) writeNode(node tree.Node[V], pageID uint32) error {
	// Encode the node
	data, err := idx.codec.Encode(node)
	if err != nil {
		return fmt.Errorf("failed to encode node: %w", err)
	}

	// Create index page
	indexPage := page.NewIndexPage()
	indexPage.SetData(data)

	// Write to disk
	offset := int64(HeaderSize + pageID*page.PageSize)
	_, err = idx.file.WriteAt(indexPage.GetData(), offset)
	return err
}

// readNode reads a node from a specific page
func (idx *IndexFile[K, V]) readNode(pageID uint32) (tree.Node[V], error) {
	// Read from disk
	offset := int64(HeaderSize + pageID*page.PageSize)
	data := make([]byte, page.PageSize)

	_, err := idx.file.ReadAt(data, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}

	// Decode the node
	decoded, err := idx.codec.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode node from page %d: %w", pageID, err)
	}

	// Cast to tree.Node[V]
	node, ok := decoded.(tree.Node[V])
	if !ok {
		return nil, fmt.Errorf("decoded object is not a tree node")
	}

	return node, nil
}

// SetRoot updates the root page ID and writes it to disk
func (idx *IndexFile[K, V]) SetRoot(pageID uint32) error {
	idx.rootPageID = pageID
	return idx.writeHeader()
}

// GetRoot returns the current root page ID
func (idx *IndexFile[K, V]) GetRoot() uint32 {
	return idx.rootPageID
}

// GetOrder returns the tree order
func (idx *IndexFile[K, V]) GetOrder() int {
	return idx.order
}
