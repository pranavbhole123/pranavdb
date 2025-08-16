package data

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strings"
)

const (
	DataHeaderSize = 4096
	SchemaReserve  = 1000 // bytes reserved for 1-byte type codes (max columns)
)



// rowFile manages the table file header and schema codes.
type rowFile struct {
	file          *os.File
	firstFreePage uint64 // head of free list (byte offset), 0 means none
	schemaCodes   []byte // len(schemaCodes) == columnCount
	columnCount   uint16
}
func (rf *rowFile) GetFirstFreePage() uint64 {
    return rf.firstFreePage
}

// NewRowfile creates a new/truncated row file and writes the header.
// schemaStr is comma-separated type names, e.g. "int,string,float".
func NewRowfile(filepath string, schemaStr string) (*rowFile, error) {
	codes, count, err := parseSchemaString(schemaStr)
	if err != nil {
		return nil, err
	}
	if int(count) > SchemaReserve {
		return nil, fmt.Errorf("too many columns: %d (max %d)", count, SchemaReserve)
	}

	f, err := os.Create(filepath) // creates/truncates
	if err != nil {
		return nil, fmt.Errorf("create rowfile: %w", err)
	}

	rf := &rowFile{
		file:          f,
		firstFreePage: 0,
		schemaCodes:   append([]byte(nil), codes...),
		columnCount:   count,
	}

	if err := rf.writeHeader(); err != nil {
		f.Close()
		return nil, fmt.Errorf("write header: %w", err)
	}
	return rf, nil
}

// OpenRowfile opens an existing row file and reads header into memory.
func OpenRowfile(filepath string) (*rowFile, error) {
	f, err := os.OpenFile(filepath, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open rowfile: %w", err)
	}

	header := make([]byte, DataHeaderSize)
	n, err := f.ReadAt(header, 0)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}
	// need at least 10 bytes of metadata (2 + 8)
	if n < 10 {
		f.Close()
		return nil, fmt.Errorf("header too small: read %d bytes", n)
	}

	// Read column count (first)
	colCount := binary.LittleEndian.Uint16(header[0:2])
	if int(colCount) > SchemaReserve {
		f.Close()
		return nil, fmt.Errorf("invalid columnCount in header: %d", colCount)
	}

	// Read firstFreePage (next 8 bytes)
	firstFree := binary.LittleEndian.Uint64(header[2:10])

	// Ensure we have enough bytes to slice schema area
	if n < 10+int(colCount) {
		f.Close()
		return nil, fmt.Errorf("header truncated: expected at least %d bytes, got %d", 10+int(colCount), n)
	}

	// copy only the meaningful schema bytes (first colCount bytes from schema area)
	schemaBuf := make([]byte, colCount)
	copy(schemaBuf, header[10:10+int(colCount)])

	return &rowFile{
		file:          f,
		firstFreePage: firstFree,
		schemaCodes:   schemaBuf,
		columnCount:   colCount,
	}, nil
}

// writeHeader persists header (columnCount, firstFreePage, schema codes).
// bytes 0..1   -> columnCount (uint16)
// bytes 2..9   -> firstFreePage (uint64)
// bytes 10..(10+SchemaReserve-1) -> schema fixed area (we copy schemaCodes into start of it)
func (rw *rowFile) writeHeader() error {
	header := make([]byte, DataHeaderSize)

	// columnCount at bytes 0..1
	binary.LittleEndian.PutUint16(header[0:2], rw.columnCount)

	// firstFreePage at bytes 2..9
	binary.LittleEndian.PutUint64(header[2:10], rw.firstFreePage)

	// copy schema codes into fixed schema area starting at offset 10
	copy(header[10:10+SchemaReserve], rw.schemaCodes)

	if _, err := rw.file.WriteAt(header, 0); err != nil {
		return fmt.Errorf("writeHeader: %w", err)
	}
	return nil
}

func (rw *rowFile) readHeader() error {
	if rw.file == nil {
		return fmt.Errorf("readHeader: file is not open")
	}

	header := make([]byte, DataHeaderSize)
	n, err := rw.file.ReadAt(header, 0)
	if err != nil {
		return fmt.Errorf("readHeader: failed to read header: %w", err)
	}
	if n < 10 {
		return fmt.Errorf("readHeader: header too small: read %d bytes", n)
	}

	// columnCount stored at bytes 0..1
	colCount := binary.LittleEndian.Uint16(header[0:2])
	if int(colCount) > SchemaReserve {
		return fmt.Errorf("readHeader: invalid columnCount in header: %d (max %d)", colCount, SchemaReserve)
	}

	// firstFreePage stored at bytes 2..9
	firstFree := binary.LittleEndian.Uint64(header[2:10])

	// ensure we have enough bytes to read the meaningful schema bytes
	if n < 10+int(colCount) {
		return fmt.Errorf("readHeader: header truncated: expected at least %d bytes, got %d", 10+int(colCount), n)
	}

	// copy only the meaningful schema bytes (first colCount bytes from schema area)
	schemaBuf := make([]byte, colCount)
	copy(schemaBuf, header[10:10+int(colCount)])

	// populate struct
	rw.columnCount = colCount
	rw.firstFreePage = firstFree
	rw.schemaCodes = schemaBuf

	return nil
}


// allocatePage finds a free slot large enough to fit 'size' bytes (length-prefix + payload),
// or appends at EOF. Free-node layout on disk:
// [0:2]   uint16 marker = 0xFFFF
// [2:10]  uint64 nextFreeOffset
// [10:12] uint16 originalPayloadLen
func (rw *rowFile) allocatePage(size int) (int64, error) {
	var prevOffset uint64 = 0
	currOffset := rw.firstFreePage
	// Traverse free list (first-fit)
	for currOffset != 0 {
		header := make([]byte, 12)
		if _, err := rw.file.ReadAt(header, int64(currOffset)); err != nil {
			return 0, err
		}

		marker := binary.LittleEndian.Uint16(header[0:2])
		if marker != 0xFFFF {
			return 0, fmt.Errorf("corrupted free page at offset %d", currOffset)
		}

		nextFree := binary.LittleEndian.Uint64(header[2:10])
		payloadLen := int(binary.LittleEndian.Uint16(header[10:12]))

		// Total size available = 2 (header len field) + payload
		if 2+payloadLen >= size {
			if prevOffset == 0 {
				// First node in list
				rw.firstFreePage = nextFree
			} else {
				// Patch "next" pointer of previous node to skip current
				tmp := make([]byte, 8)
				binary.LittleEndian.PutUint64(tmp, nextFree)
				if _, err := rw.file.WriteAt(tmp, int64(prevOffset)+2); err != nil {
					return 0, err
				}
			}
			return int64(currOffset), nil
		}

		// Advance to next node
		prevOffset = currOffset
		currOffset = nextFree
	}

	// No free slot fits → append at EOF
	info, err := rw.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}



func (rw *rowFile) WriteRow(values []any) (int64, error) {
	// encode payload according to current schema codes
	payload, err := encodeRow(rw.schemaCodes, values)
	if err != nil {
		return 0, err
	}

	// payload must fit in uint16
	if len(payload) > math.MaxUint16 {
		return 0, fmt.Errorf("WriteRow: payload too large (%d bytes, max %d)", len(payload), math.MaxUint16)
	}
	payloadLen := uint16(len(payload))

	// prepare buffer: 2 bytes length + payload
	buf := make([]byte, 2+len(payload))
	binary.LittleEndian.PutUint16(buf[0:2], payloadLen)
	copy(buf[2:], payload)

	// allocate append offset or reuse free
	offset, err := rw.allocatePage(2 + len(payload))
	if err != nil {
		return 0, fmt.Errorf("WriteRow: allocatePage: %w", err)
	}

	// write to file
	n, err := rw.file.WriteAt(buf, offset)
	if err != nil {
		return 0, fmt.Errorf("WriteRow: write failed at offset %d: %w", offset, err)
	}
	if n != len(buf) {
		return 0, fmt.Errorf("WriteRow: short write at offset %d: wrote %d of %d", offset, n, len(buf))
	}

	return offset, nil
}

// ReadRowAt reads a row starting at the given file offset (offset points to the 2-byte length),
// decodes it according to the in-memory schema, and returns the values slice.
func (rw *rowFile) ReadRowAt(offset int64) ([]any, error) {
	if rw.file == nil {
		return nil, fmt.Errorf("ReadRowAt: file not open")
	}

	// read 2-byte payload length
	lenBuf := make([]byte, 2)
	if _, err := rw.file.ReadAt(lenBuf, offset); err != nil {
		return nil, fmt.Errorf("ReadRowAt: read length failed at offset %d: %w", offset, err)
	}
	payloadLen := binary.LittleEndian.Uint16(lenBuf)


	//fmt.Println("******************************************* ",payloadLen)
	// detect free marker
	if payloadLen == 0xFFFF {
		return nil, fmt.Errorf("ReadRowAt: row at %d is free", offset)
	}

	// read payload
	if payloadLen == 0 {
		return []any{}, nil
	}
	payload := make([]byte, payloadLen)
	if _, err := rw.file.ReadAt(payload, offset+2); err != nil {
		return nil, fmt.Errorf("ReadRowAt: read payload failed at offset %d: %w", offset+2, err)
	}

	// decode according to current schema
	values, err := decodeRow(payload, rw.schemaCodes)
	if err != nil {
		return nil, fmt.Errorf("ReadRowAt: decode failed at offset %d: %w", offset, err)
	}
	return values, nil
}

/*
Free row management

On free, row layout becomes:
[0:2]   uint16 marker = 0xFFFF
[2:10]  uint64 nextFreeHead (previous free-list head)
[10:12] uint16 originalPayloadLen
[12:..] (unused)

firstFreePage in header points to the most-recently freed row.
*/

// FreeRowAt marks a row free and pushes it to the free list.
func (rw *rowFile) FreeRowAt(offset int64) error {
	if rw.file == nil {
		return fmt.Errorf("FreeRowAt: file not open")
	}

	// Read the existing payload length so we know how much space this row occupied.
	lenBuf := make([]byte, 2)
	if _, err := rw.file.ReadAt(lenBuf, offset); err != nil {
		return fmt.Errorf("FreeRowAt: failed to read existing length at %d: %w", offset, err)
	}
	oldLen := binary.LittleEndian.Uint16(lenBuf)

	// If it's already marked free (sentinel 0xFFFF), return early.
	if oldLen == 0xFFFF {
		return fmt.Errorf("FreeRowAt: row at offset %d already freed", offset)
	}

	// Build free-node metadata: nextFreeHead then original length.
	meta := make([]byte, 8+2)
	binary.LittleEndian.PutUint64(meta[0:8], rw.firstFreePage)
	binary.LittleEndian.PutUint16(meta[8:10], oldLen)

	// 1) write free marker (0xFFFF) into the 2-byte length field
	marker := make([]byte, 2)
	binary.LittleEndian.PutUint16(marker, 0xFFFF)
	if _, err := rw.file.WriteAt(marker, offset); err != nil {
		return fmt.Errorf("FreeRowAt: failed to write free marker at %d: %w", offset, err)
	}

	// 2) write metadata (next pointer + original length) at offset+2
	if _, err := rw.file.WriteAt(meta, offset+2); err != nil {
		return fmt.Errorf("FreeRowAt: failed to write free metadata at %d: %w", offset+2, err)
	}

	// 3) update in-memory free head and persist header
	rw.firstFreePage = uint64(offset)
	if err := rw.writeHeader(); err != nil {
		return fmt.Errorf("FreeRowAt: failed to persist header after freeing: %w", err)
	}

	return nil
}

// ReadFreeRowAt reads metadata for a *known-free* row at offset.
func (rw *rowFile) ReadFreeRowAt(offset int64) (nextFreeHead uint64, origPayloadLen uint16, err error) {
	header := make([]byte, 12) // marker(2) + next(8) + len(2)
	_, err = rw.file.ReadAt(header, offset)
	if err != nil {
		return 0, 0, fmt.Errorf("ReadFreeRowAt: %w", err)
	}

	// decode
	marker := binary.LittleEndian.Uint16(header[0:2])
	if marker != 0xFFFF {
		return 0, 0, fmt.Errorf("ReadFreeRowAt: expected free marker 0xFFFF, found 0x%X", marker)
	}

	nextFreeHead = binary.LittleEndian.Uint64(header[2:10])
	origPayloadLen = binary.LittleEndian.Uint16(header[10:12])

	return nextFreeHead, origPayloadLen, nil
}

// --- Schema helpers ---

func parseSchemaString(schema string) ([]byte, uint16, error) {
	trim := strings.TrimSpace(schema)
	if trim == "" {
		return []byte{}, 0, nil
	}
	parts := strings.Split(trim, ",")
	if len(parts) > SchemaReserve {
		return nil, 0, fmt.Errorf("too many columns: %d (max %d)", len(parts), SchemaReserve)
	}
	out := make([]byte, 0, len(parts))
	for i, p := range parts {
		name := strings.ToUpper(strings.TrimSpace(p))
		if name == "" {
			return nil, 0, fmt.Errorf("empty type at position %d", i)
		}
		code, ok := typeNameToCode[name]
		if !ok {
			return nil, 0, fmt.Errorf("unsupported type %q at position %d (supported: int,string,float)", p, i)
		}
		out = append(out, code)
	}
	return out, uint16(len(out)), nil
}

func SchemaStringFromCodes(codes []byte) string {
	if len(codes) == 0 {
		return ""
	}
	parts := make([]string, 0, len(codes))
	for _, c := range codes {
		if nm, ok := codeToTypeName[c]; ok {
			parts = append(parts, nm)
		} else {
			parts = append(parts, fmt.Sprintf("unknown(%d)", c))
		}
	}
	return strings.Join(parts, ",")
}

func (rw *rowFile) GetSchemaCodes() []byte {
	out := make([]byte, len(rw.schemaCodes))
	copy(out, rw.schemaCodes)
	return out
}

func (rw *rowFile) GetColumnCount() uint16 { return rw.columnCount }

func (rw *rowFile) Close() error {
	if rw.file == nil {
		return nil
	}
	return rw.file.Close()
}

