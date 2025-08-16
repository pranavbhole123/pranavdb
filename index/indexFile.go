package index

import (
	"encoding/binary"
	"fmt"
	"os"
	"pranavdb/page"
	"pranavdb/tree"
)

const (
	MagicNumber = 0x42504C55 // "B+LU"
	Version     = 1
	HeaderSize  = 512

	PageTypeHeader = 0
	PageTypeNode   = 1
)

type IndexFile[K tree.Key, V any] struct {
	file          *os.File
	rootPageID    uint32
	order         int
	firstFreePage uint32 // ✅ Keep in-memory free list head
	codec         *page.IndexPageCodec[K, V]
}

type FileHeader struct {
	MagicNumber    uint32
	Version        uint32
	RootPageID     uint32
	TreeOrder      uint32
	FirstFreeListID uint32
}

func NewIndexFile[K tree.Key, V any](filepath string, order int) (*IndexFile[K, V], error) {
	file, err := os.Create(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to create index file: %w", err)
	}

	indexFile := &IndexFile[K, V]{
		file:          file,
		rootPageID:    0,
		order:         order,
		firstFreePage: 0, // no free pages yet
		codec:         page.NewIndexPageCodec[K, V](),
	}

	if err := indexFile.writeHeader(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	return indexFile, nil
}

func OpenIndexFile[K tree.Key, V any](filepath string) (*IndexFile[K, V], error) {
	file, err := os.OpenFile(filepath, os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	indexFile := &IndexFile[K, V]{
		file:  file,
		codec: page.NewIndexPageCodec[K, V](),
	}

	if err := indexFile.readHeader(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	return indexFile, nil
}

func (idx *IndexFile[K, V]) Close() error {
	if err := idx.writeHeader(); err != nil {
		return fmt.Errorf("failed to write final header: %w", err)
	}
	return idx.file.Close()
}

func (idx *IndexFile[K, V]) writeHeader() error {
	header := FileHeader{
		MagicNumber:    MagicNumber,
		Version:        Version,
		RootPageID:     idx.rootPageID,
		TreeOrder:      uint32(idx.order),
		FirstFreeListID: idx.firstFreePage,
	}

	headerBlock := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(headerBlock[0:4], header.MagicNumber)
	binary.LittleEndian.PutUint32(headerBlock[4:8], header.Version)
	binary.LittleEndian.PutUint32(headerBlock[8:12], header.RootPageID)
	binary.LittleEndian.PutUint32(headerBlock[12:16], header.TreeOrder)
	binary.LittleEndian.PutUint32(headerBlock[16:20], header.FirstFreeListID)

	_, err := idx.file.WriteAt(headerBlock, 0)
	return err
}

func (idx *IndexFile[K, V]) readHeader() error {
	headerBlock := make([]byte, HeaderSize)
	_, err := idx.file.ReadAt(headerBlock, 0)
	if err != nil {
		return err
	}

	magic := binary.LittleEndian.Uint32(headerBlock[0:4])
	version := binary.LittleEndian.Uint32(headerBlock[4:8])
	idx.rootPageID = binary.LittleEndian.Uint32(headerBlock[8:12])
	idx.order = int(binary.LittleEndian.Uint32(headerBlock[12:16]))
	idx.firstFreePage = binary.LittleEndian.Uint32(headerBlock[16:20])

	if magic != MagicNumber {
		return fmt.Errorf("invalid magic number: expected %x, got %x", MagicNumber, magic)
	}
	if version != Version {
		return fmt.Errorf("unsupported version: %d", version)
	}

	return nil
}


// ✅ Allocate page (reuse free list if possible)
func (idx *IndexFile[K, V]) allocatePage() (uint32, error) {
	// 1. Read the free list head from header
	freeHead := idx.firstFreePage

	//fmt.Print("freehead ******************************************************")
	//fmt.Println(freeHead)
	// 2. If there is a free page, reuse it
	if freeHead != 0 { 
		// Read next free page pointer from that page
		nextFree, err := idx.readFreeListPointer(freeHead)
		if err != nil {
			return 0, err
		}
		// the logic for making the bool 0 is already written in the write node if that is called the delete gets written to 0
		// Update the free list head to point to the next free page
		idx.firstFreePage = nextFree
		err = idx.writeHeader()
		if err != nil{
			return 0, err
		}

		// Return the reused page
		return freeHead, nil
	}

	// 3. Otherwise, append a new page at the end
	info, err := idx.file.Stat()
	if err != nil {
		return 0, err
	}
	nextPageID := max(uint32((info.Size() - HeaderSize) / page.PageSize),1)

	zeroPage := make([]byte, page.PageSize)
	_, err = idx.file.WriteAt(zeroPage, int64(HeaderSize+int64(nextPageID)*page.PageSize))
	if err != nil {
		return 0, err
	}
	return nextPageID, nil
}



func (idx *IndexFile[K, V]) freePage(pageID uint32) error {
	// build page buffer
	//fmt.Print("pageid ******************************************************")
	//fmt.Println(pageID)
	buf := make([]byte, page.PageSize)

	// mark as deleted
	buf[0] = 1

	// write next pointer at buf[1:5]
	binary.LittleEndian.PutUint32(buf[1:5], idx.firstFreePage)

	// write the page buffer to disk at the correct offset
	offset := int64(HeaderSize) + int64(pageID)*int64(page.PageSize)
	if _, err := idx.file.WriteAt(buf, offset); err != nil {
		return fmt.Errorf("freePage: write failed for page %d: %w", pageID, err)
	}

	// update in-memory head and persist header
	idx.firstFreePage = pageID
	if err := idx.writeHeader(); err != nil {
		return fmt.Errorf("freePage: writeHeader failed: %w", err)
	}

	return nil
}


// Helper to read next free list pointer from a free page
func (idx *IndexFile[K, V]) readFreeListPointer(pageID uint32) (uint32, error) {
	// Buffer for flag + next free page ID
	buf := make([]byte, 5) // 1 byte for bool + 4 bytes for uint32
	offset := int64(HeaderSize) + int64(pageID)*page.PageSize

	_, err := idx.file.ReadAt(buf, offset)
	if err != nil {
		return 0, err
	}

	// First byte is the deleted flag
	deleted := buf[0] != 0
	if !deleted {
		return 0, fmt.Errorf("page %d is not marked as free", pageID)
	}

	// Next 4 bytes are the next free page pointer
	nextFree := binary.LittleEndian.Uint32(buf[1:5])
	return nextFree, nil
}


// writeNode writes a node to a specific page
func (idx *IndexFile[K, V]) writeNode(node tree.Node[V], pageID uint32) error {
	// Encode the node
	data, err := idx.codec.Encode(node)
	if err != nil {
		return fmt.Errorf("failed to encode node: %w", err)
	}

	// Sanity check: encoded payload must fit in page minus 1 byte for Deleted flag
	if len(data) > page.PageSize-1 {
		return fmt.Errorf("encoded node size %d exceeds page payload capacity %d", len(data), page.PageSize-1)
	}

	// Build full physical page buffer: first byte = deleted flag (0), then payload
	buf := make([]byte, page.PageSize)
	buf[0] = 0 // not deleted
	if len(data) > 0 {
		copy(buf[1:], data)
	}

	// Write the full page to disk
	offset := int64(HeaderSize+ int64(pageID*page.PageSize))
	if _, err := idx.file.WriteAt(buf, offset); err != nil {
		return fmt.Errorf("failed to write node to page %d: %w", pageID, err)
	}
	return nil
}

func (idx *IndexFile[K, V]) readNode(pageID uint32) (tree.Node[V], error) {
	// Read the full page into buffer
	buf := make([]byte, page.PageSize)
	offset := int64(HeaderSize + int64(pageID*page.PageSize))

	_, err := idx.file.ReadAt(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}

	// Check deleted flag (first byte)
	if buf[0] != 0 {
		return nil, fmt.Errorf("page %d is marked deleted", pageID)
	}

	// Pass payload (skipping deleted flag) to codec for decoding
	payload := buf[1:]

	decoded, err := idx.codec.Decode(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to decode node from page %d: %w", pageID, err)
	}
	node, ok := decoded.(tree.Node[V])
	if !ok {
		return nil, fmt.Errorf("decoded object is not a tree node (page %d)", pageID)
	}
	return node, nil
}

func (idx *IndexFile[K, V]) SetRoot(pageID uint32) error {
	idx.rootPageID = pageID
	return idx.writeHeader()
}

func (idx *IndexFile[K, V]) GetRoot() uint32 {
	return idx.rootPageID
}

func (idx *IndexFile[K, V]) GetOrder() int {
	return idx.order
}
