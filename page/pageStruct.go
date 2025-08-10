// page/page.go
package page

const (
	// PageSize is the fixed size of every on-disk index page.
	PageSize = 4096
)

// IndexPage represents one fixed-size index page containing raw B+ tree node data.
// Each index page stores exactly one complete node (internal or leaf).
type IndexPage struct {
	Data [PageSize]byte
}

// NewIndexPage returns a zeroed index page.
func NewIndexPage() *IndexPage {
	return &IndexPage{}
}

// GetData returns the raw data slice for encoding/decoding.
func (p *IndexPage) GetData() []byte {
	return p.Data[:]
}

// SetData sets the raw data from encoded node bytes.
func (p *IndexPage) SetData(data []byte) {
	copy(p.Data[:], data)
}
