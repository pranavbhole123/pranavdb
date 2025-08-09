package memory

import (
	"encoding/binary"
	"fmt"
	"io"     // <- import io for EOF
	"os"
)

const (
	// PageSize is the fixed size of every page on disk.
	PageSize = 4096

	// Superblock page ID
	SuperblockPageID = 0

	// PageType values stored in the first byte of each page.
	PageTypeFree = 0
	PageTypeMeta = 1
	PageTypeData = 2 // you can use 2 for index or data pages later
)

// Pager manages page allocation, free‐list, and raw I/O.
type Pager struct {
	file     *os.File
	freeHead uint32 // pageID of first free page (0 = none)
}

// OpenPager opens or creates the database file at path,
// initializes the free‐list head from the superblock (page 0).
func OpenPager(path string) (*Pager, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	p := &Pager{file: f}
	if err := p.loadSuperblock(); err != nil {
		f.Close()
		return nil, err
	}
	return p, nil
}

// Close writes back the superblock and closes the file.
func (p *Pager) Close() error {
	if err := p.writeSuperblock(); err != nil {
		return err
	}
	return p.file.Close()
}

// AllocatePage returns a free pageID: either by popping from the
// free‐list, or by extending the file (appending a new page).
func (p *Pager) AllocatePage() (uint32, error) {
	// If we have pages on the free‐list, pop one
	if p.freeHead != 0 {
		id := p.freeHead
		// read its header to find next free
		buf := make([]byte, PageSize)
		if _, err := p.file.ReadAt(buf, int64(id)*PageSize); err != nil {
			return 0, err
		}
		if buf[0] != PageTypeFree {
			return 0, fmt.Errorf("pager: expected free page at %d, got type %d", id, buf[0])
		}
		p.freeHead = binary.LittleEndian.Uint32(buf[1:5])
		return id, nil
	}

	// else extend the file
	info, err := p.file.Stat()
	if err != nil {
		return 0, err
	}
	newID := uint32(info.Size() / PageSize)
	// ensure file is large enough by writing zero‐page
	zero := make([]byte, PageSize)
	if _, err := p.file.WriteAt(zero, int64(newID)*PageSize); err != nil {
		return 0, err
	}
	return newID, nil
}

// FreePage adds pageID back to the free‐list.
func (p *Pager) FreePage(pageID uint32) error {
	// write a free‐page header: [type=0][nextFree:uint32]
	buf := make([]byte, PageSize)
	buf[0] = PageTypeFree
	binary.LittleEndian.PutUint32(buf[1:5], p.freeHead)
	if _, err := p.file.WriteAt(buf, int64(pageID)*PageSize); err != nil {
		return err
	}
	p.freeHead = pageID
	return nil
}

// ReadPage reads the raw bytes of pageID into dst (must be len ≥ PageSize).
func (p *Pager) ReadPage(pageID uint32, dst []byte) error {
	if len(dst) < PageSize {
		return fmt.Errorf("ReadPage: dst buffer too small")
	}
	_, err := p.file.ReadAt(dst[:PageSize], int64(pageID)*PageSize)
	return err
}

// WritePage writes src (len ≥ PageSize) to pageID.
func (p *Pager) WritePage(pageID uint32, src []byte) error {
	if len(src) < PageSize {
		return fmt.Errorf("WritePage: src buffer too small")
	}
	_, err := p.file.WriteAt(src[:PageSize], int64(pageID)*PageSize)
	return err
}

// --- superblock handling (page 0) ---

// superblock layout: [0] = PageTypeMeta, [1–4] = freeHead (uint32), rest unused.
func (p *Pager) loadSuperblock() error {
	buf := make([]byte, PageSize)
	n, err := p.file.ReadAt(buf, 0)
	if err != nil && err != io.EOF { // use io.EOF, not os.EOF
		return err
	}
	// If file too small or not meta, initialize fresh
	if n < 5 || buf[0] != PageTypeMeta {
		p.freeHead = 0
		return p.writeSuperblock()
	}
	p.freeHead = binary.LittleEndian.Uint32(buf[1:5])
	return nil
}

func (p *Pager) writeSuperblock() error {
	buf := make([]byte, PageSize)
	buf[0] = PageTypeMeta
	binary.LittleEndian.PutUint32(buf[1:5], p.freeHead)
	_, err := p.file.WriteAt(buf, 0)
	return err
}
