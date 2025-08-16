package data

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)


const (
	TypeCodeInt    byte = 1
	TypeCodeFloat  byte = 2
	TypeCodeString byte = 3
)

var typeNameToCode = map[string]byte{
	"INT":    TypeCodeInt,
	"FLOAT":  TypeCodeFloat,
	"STRING": TypeCodeString,
}

var codeToTypeName = map[byte]string{
	TypeCodeInt:    "int",
	TypeCodeFloat:  "float",
	TypeCodeString: "string",
}

// this file contains the code to encode and decode

func encodeRow(schemaCodes []byte, values []any) ([]byte, error) {
	if len(schemaCodes) != len(values) {
		return nil, fmt.Errorf("encodeRow: schema len %d != values len %d", len(schemaCodes), len(values))
	}

	out := make([]byte, 0, 128)

	for i, code := range schemaCodes {
		val := values[i]
		switch code {
		case TypeCodeInt:
			vi, ok := val.(int)
			if !ok {
				return nil, fmt.Errorf("encodeRow: field %d expected int, got %T", i, val)
			}
			if vi < math.MinInt32 || vi > math.MaxInt32 {
				return nil, fmt.Errorf("encodeRow: field %d int out of int32 range", i)
			}
			b := make([]byte, 4)
			binary.LittleEndian.PutUint32(b, uint32(int32(vi)))
			out = append(out, b...)

		case TypeCodeFloat:
			fv, ok := val.(float64)
			if !ok {
				return nil, fmt.Errorf("encodeRow: field %d expected float64, got %T", i, val)
			}
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, math.Float64bits(fv))
			out = append(out, b...)

		case TypeCodeString:
			s, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("encodeRow: field %d expected string, got %T", i, val)
			}
			sb := []byte(s)
			if len(sb) > math.MaxUint16 {
				return nil, fmt.Errorf("encodeRow: field %d string too large (%d > %d)", i, len(sb), math.MaxUint16)
			}
			lenb := make([]byte, 2)
			binary.LittleEndian.PutUint16(lenb, uint16(len(sb)))
			out = append(out, lenb...)
			out = append(out, sb...)

		default:
			return nil, fmt.Errorf("encodeRow: unknown type code %d at pos %d", code, i)
		}
	}

	return out, nil
}

func decodeRow(payload []byte, schemaCodes []byte) ([]any, error) {
	out := make([]any, 0, len(schemaCodes))
	offset := 0
	for i, code := range schemaCodes {
		switch code {
		case TypeCodeInt:
			// 4 bytes -> int32
			if offset+4 > len(payload) {
				return nil, fmt.Errorf("decodeRow: field %d int out of bounds", i)
			}
			u := binary.LittleEndian.Uint32(payload[offset : offset+4])
			v := int32(u)
			out = append(out, v)
			offset += 4

		case TypeCodeFloat:
			// 8 bytes -> float64
			if offset+8 > len(payload) {
				return nil, fmt.Errorf("decodeRow: field %d float out of bounds", i)
			}
			u := binary.LittleEndian.Uint64(payload[offset : offset+8])
			f := math.Float64frombits(u)
			out = append(out, f)
			offset += 8

		case TypeCodeString:
			// 2-byte length (uint16) followed by bytes
			if offset+2 > len(payload) {
				return nil, fmt.Errorf("decodeRow: field %d string length out of bounds", i)
			}
			strLen := binary.LittleEndian.Uint16(payload[offset : offset+2])
			offset += 2
			if offset+int(strLen) > len(payload) {
				return nil, fmt.Errorf("decodeRow: field %d string bytes out of bounds", i)
			}
			s := string(payload[offset : offset+int(strLen)])
			out = append(out, s)
			offset += int(strLen)

		default:
			return nil, fmt.Errorf("decodeRow: unknown type code %d at pos %d", code, i)
		}
	}

	if offset != len(payload) {
		// return decoded values and error describing mismatch
		return out, errors.New("decodeRow: payload length mismatch (possible schema mismatch)")
	}
	return out, nil
}
