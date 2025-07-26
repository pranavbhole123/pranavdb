package memory

import (
	"encoding/binary"
	"fmt"
	"os"
)

const (
	PageSize = 4096
)

// PageType constants
const (
	PageTypeData  = 1
	PageTypeIndex = 2
	PageTypeFree  = 3
)

// PageHeader is the metadata at the start of each page
// For simplicity: [type:uint8][numRecords:uint16][freeSpaceOffset:uint16]
type PageHeader struct {
	PageType        uint8
	NumRecords      uint16
	FreeSpaceOffset uint16
}

// Page represents a fixed-size page in the database file
// Header is at the start, Data is the rest
// Data is always PageSize - header size
type Page struct {
	Header PageHeader
	Data   [PageSize - 5]byte // 5 bytes for header
}

// NewPage creates a new empty page of the given type
func NewPage(pageType uint8) *Page {
	return &Page{
		Header: PageHeader{
			PageType:        pageType,
			NumRecords:      0,
			FreeSpaceOffset: 0,
		},
	}
}

// WritePage writes a page to the file at the given offset (pageID * PageSize)
func WritePage(f *os.File, pageID int64, page *Page) error {
	offset := pageID * PageSize
	buf := make([]byte, PageSize)
	// Encode header
	buf[0] = page.Header.PageType
	binary.LittleEndian.PutUint16(buf[1:3], page.Header.NumRecords)
	binary.LittleEndian.PutUint16(buf[3:5], page.Header.FreeSpaceOffset)
	// Copy data
	copy(buf[5:], page.Data[:])
	// Write to file
	_, err := f.WriteAt(buf, offset)
	return err
}

// ReadPage reads a page from the file at the given offset (pageID * PageSize)
func ReadPage(f *os.File, pageID int64) (*Page, error) {
	offset := pageID * PageSize
	buf := make([]byte, PageSize)
	_, err := f.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	page := &Page{}
	page.Header.PageType = buf[0]
	page.Header.NumRecords = binary.LittleEndian.Uint16(buf[1:3])
	page.Header.FreeSpaceOffset = binary.LittleEndian.Uint16(buf[3:5])
	copy(page.Data[:], buf[5:])
	return page, nil
}

// Debug print for a page
func (p *Page) Print() {
	fmt.Printf("PageType: %d, NumRecords: %d, FreeSpaceOffset: %d\n", p.Header.PageType, p.Header.NumRecords, p.Header.FreeSpaceOffset)
}
