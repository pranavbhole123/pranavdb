// page/page.go
package page

import (
	"encoding/binary"
	"errors"
)

const (
	// PageSize is the fixed size of every on-disk page.
	PageSize = 4096

	// HeaderSize is the number of bytes occupied by PageHeader.
	HeaderSize = 1 + 2 + 2 + 2 + 4 // PageType + SchemaID + NumRecords + FreeSpaceOffset + NextPageID
)

// PageHeader is the fixed, common header for every on-disk page.
type PageHeader struct {
	PageType        uint8  // e.g. 0=Free, 1=Meta, 2=Data, 3=Index
	SchemaID        uint16 // which table or index this page belongs to
	NumRecords      uint16 // number of entries (rows or keys) stored
	FreeSpaceOffset uint16 // offset into Data[] where the free space begins
	NextPageID      uint32 // e.g. right-sibling for index pages or next-free in free-list
}

// Page represents one fixed-size page: the header plus raw payload.
type Page struct {
	Header PageHeader
	Data   [PageSize - HeaderSize]byte
}

// NewPage returns a zeroed page with the given type and schema.
// It initializes FreeSpaceOffset to the start of Data (HeaderSize).
func NewPage(pageType uint8, schemaID uint16) *Page {
	p := &Page{
		Header: PageHeader{
			PageType:        pageType,
			SchemaID:        schemaID,
			NumRecords:      0,
			FreeSpaceOffset: HeaderSize,
			NextPageID:      0,
		},
	}
	// Data is zero-initialized by default
	return p
}

// Serialize packs the header and data into a single PageSize-length buffer.
func (p *Page) Serialize() []byte {
	buf := make([]byte, PageSize)
	// header
	buf[0] = p.Header.PageType
	binary.LittleEndian.PutUint16(buf[1:3], p.Header.SchemaID)
	binary.LittleEndian.PutUint16(buf[3:5], p.Header.NumRecords)
	binary.LittleEndian.PutUint16(buf[5:7], p.Header.FreeSpaceOffset)
	binary.LittleEndian.PutUint32(buf[7:11], p.Header.NextPageID)
	// payload
	copy(buf[HeaderSize:], p.Data[:])
	return buf
}

// DeserializePage reads a Page from a PageSize-length buffer.
func DeserializePage(buf []byte) (*Page, error) {
	if len(buf) != PageSize {
		return nil, errors.New("DeserializePage: buffer must be exactly PageSize bytes")
	}
	p := &Page{}
	p.Header.PageType = buf[0]
	p.Header.SchemaID = binary.LittleEndian.Uint16(buf[1:3])
	p.Header.NumRecords = binary.LittleEndian.Uint16(buf[3:5])
	p.Header.FreeSpaceOffset = binary.LittleEndian.Uint16(buf[5:7])
	p.Header.NextPageID = binary.LittleEndian.Uint32(buf[7:11])
	copy(p.Data[:], buf[HeaderSize:])
	return p, nil
}
