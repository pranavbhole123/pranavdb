package page

import (
	"encoding/binary"
	"errors"
	"math"
	"pranavdb/tree"
)

// Key type constants for encoding/decoding
const (
	KeyTypeInt    = 1
	KeyTypeFloat  = 2
	KeyTypeString = 3
)

// Codec encodes/decodes objects into/from a raw page *payload* (no header).
// Not all codecs have to implement this; it's here if you need polymorphism.
type Codec interface {
	Encode(obj interface{}) ([]byte, error)
	Decode(data []byte) (interface{}, error)
}

type IndexPageCodec[K tree.Key, V any] struct {
}

// NewIndexPageCodec creates a new IndexPageCodec instance
func NewIndexPageCodec[K tree.Key, V any]() *IndexPageCodec[K, V] {
	return &IndexPageCodec[K, V]{}
}

// Encode implements the Codec interface for IndexPageCodec
func (p *IndexPageCodec[K, V]) Encode(obj interface{}) ([]byte, error) {
	// Try to cast the interface to tree.Node[V]
	if node, ok := obj.(tree.Node[V]); ok {
		return p.encodeNode(node)
	}
	return nil, errors.New("object is not a tree node")
}

// encodeNode encodes a specific tree node (internal method)
func (p *IndexPageCodec[K, V]) encodeNode(n tree.Node[V]) ([]byte, error) {
	// check whether its interim node or leaf node then accordingly encode the data keep in mind that we also
	// need to decode it so and return the slice of byte

	if n == nil {
		return nil, nil
	}

	// First byte indicates node type: 0 for internal node, 1 for leaf node
	var buf []byte

	// Try to cast to leaf node first
	if leaf, ok := n.(*tree.LeafNode[K, V]); ok {
		// Encode leaf node
		// Node type (1 byte)
		buf = append(buf, 1)

		// Number of pairs (2 bytes)
		numPairs := uint16(len(leaf.Pairs))
		pairCountBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(pairCountBytes, numPairs)
		buf = append(buf, pairCountBytes...)

		// Encode each key-value pair
		for _, pair := range leaf.Pairs {
			// Encode key with type identification
			keyBytes, err := p.encodeKey(pair.K)
			if err != nil {
				return nil, err
			}
			buf = append(buf, keyBytes...)

			// Encode value - we need to handle the generic V type
			// For now, assuming string values
			if strValue, ok := any(pair.Value).(string); ok {
				valueLen := uint16(len(strValue))
				valueLenBytes := make([]byte, 2)
				binary.LittleEndian.PutUint16(valueLenBytes, valueLen)
				buf = append(buf, valueLenBytes...)
				buf = append(buf, []byte(strValue)...)
			} else {
				// For other value types, we'll need to implement specific encoding
				return nil, errors.New("unsupported value type for encoding")
			}
		}

		// Next and prev pointers (8 bytes each, but for now just store as 0)
		nextPtr := make([]byte, 8)
		prevPtr := make([]byte, 8)
		buf = append(buf, nextPtr...)
		buf = append(buf, prevPtr...)

	} else if interm, ok := n.(*tree.IntermNode[K, V]); ok {
		// Encode internal node
		// Node type (1 byte)
		buf = append(buf, 0)

		// Number of keys (2 bytes)
		numKeys := uint16(len(interm.Keys))
		keyCountBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(keyCountBytes, numKeys)
		buf = append(buf, keyCountBytes...)

		// Encode each key
		for _, key := range interm.Keys {
			keyBytes, err := p.encodeKey(key)
			if err != nil {
				return nil, err
			}
			buf = append(buf, keyBytes...)
		}

		// Number of pointers (2 bytes)
		numPointers := uint16(len(interm.Pointers))
		ptrCountBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(ptrCountBytes, numPointers)
		buf = append(buf, ptrCountBytes...)

		// For pointers, we'll store placeholder values (8 bytes each)
		// In a real implementation, these would be page IDs or offsets
		for range interm.Pointers {
			ptrPlaceholder := make([]byte, 8)
			buf = append(buf, ptrPlaceholder...)
		}
	} else {
		return nil, errors.New("unknown node type")
	}

	return buf, nil
}

// encodeKey encodes a key with type identification
func (p *IndexPageCodec[K, V]) encodeKey(key K) ([]byte, error) {
	var buf []byte

	// Try to identify the key type and encode accordingly
	if intKey, ok := any(key).(tree.IntKey); ok {
		// Key type: 1 for IntKey (1 byte)
		buf = append(buf, KeyTypeInt)
		// Key value (4 bytes)
		keyBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(keyBytes, uint32(intKey))
		buf = append(buf, keyBytes...)
	} else if floatKey, ok := any(key).(tree.FloatKey); ok {
		// Key type: 2 for FloatKey (1 byte)
		buf = append(buf, KeyTypeFloat)
		// Key value (8 bytes for float64)
		keyBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(keyBytes, math.Float64bits(float64(floatKey)))
		buf = append(buf, keyBytes...)
	} else if stringKey, ok := any(key).(tree.StringKey); ok {
		// Key type: 3 for StringKey (1 byte)
		buf = append(buf, KeyTypeString)
		// String length (2 bytes)
		strLen := uint16(len(string(stringKey)))
		lenBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(lenBytes, strLen)
		buf = append(buf, lenBytes...)
		// String bytes
		buf = append(buf, []byte(string(stringKey))...)
	} else {
		return nil, errors.New("unsupported key type for encoding")
	}

	return buf, nil
}

// getEncodedKeySize returns the size in bytes of an encoded key
func (p *IndexPageCodec[K, V]) getEncodedKeySize(key K) (int, error) {
	if _, ok := any(key).(tree.IntKey); ok {
		return 1 + 4, nil // 1 byte type + 4 bytes value
	} else if _, ok := any(key).(tree.FloatKey); ok {
		return 1 + 8, nil // 1 byte type + 8 bytes value
	} else if stringKey, ok := any(key).(tree.StringKey); ok {
		return 1 + 2 + len(string(stringKey)), nil // 1 byte type + 2 bytes length + string bytes
	}
	return 0, errors.New("unsupported key type")
}

// Decode implements the Codec interface for IndexPageCodec
func (p *IndexPageCodec[K, V]) Decode(data []byte) (interface{}, error) {
	if len(data) == 0 {
		return nil, errors.New("empty data")
	}

	// First byte indicates node type
	nodeType := data[0]
	offset := 1

	switch nodeType {
	case 1: // Leaf node
		return p.decodeLeafNode(data[offset:])
	case 0: // Internal node
		return p.decodeInternalNode(data[offset:])
	default:
		return nil, errors.New("unknown node type")
	}
}

// decodeLeafNode decodes a leaf node from byte data
func (p *IndexPageCodec[K, V]) decodeLeafNode(data []byte) (*tree.LeafNode[K, V], error) {
	if len(data) < 2 {
		return nil, errors.New("insufficient data for leaf node")
	}

	offset := 0

	// Read number of pairs (2 bytes)
	numPairs := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	leaf := &tree.LeafNode[K, V]{
		Pairs: make([]tree.LeafPair[K, V], 0, numPairs),
	}

	// Decode each key-value pair
	for i := uint16(0); i < numPairs; i++ {
		if offset >= len(data) {
			return nil, errors.New("insufficient data for key-value pair")
		}

		// Decode key
		key, keySize, err := p.decodeKey(data[offset:])
		if err != nil {
			return nil, err
		}
		offset += keySize

		// Decode value (assuming string for now)
		if offset+2 > len(data) {
			return nil, errors.New("insufficient data for value length")
		}
		valueLen := binary.LittleEndian.Uint16(data[offset : offset+2])
		offset += 2

		if offset+int(valueLen) > len(data) {
			return nil, errors.New("insufficient data for value")
		}
		value := string(data[offset : offset+int(valueLen)])
		offset += int(valueLen)

		// Create the pair
		pair := tree.LeafPair[K, V]{
			K:     key,
			Value: any(value).(V),
		}
		leaf.Pairs = append(leaf.Pairs, pair)
	}

	// Skip next/prev pointers (16 bytes total)
	offset += 16

	return leaf, nil
}

// decodeInternalNode decodes an internal node from byte data
func (p *IndexPageCodec[K, V]) decodeInternalNode(data []byte) (*tree.IntermNode[K, V], error) {
	if len(data) < 2 {
		return nil, errors.New("insufficient data for internal node")
	}

	offset := 0

	// Read number of keys (2 bytes)
	numKeys := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	interm := &tree.IntermNode[K, V]{
		Keys:     make([]K, 0, numKeys),
		Pointers: make([]tree.Node[V], 0, numKeys+1),
	}

	// Decode each key
	for i := uint16(0); i < numKeys; i++ {
		if offset >= len(data) {
			return nil, errors.New("insufficient data for key")
		}

		key, keySize, err := p.decodeKey(data[offset:])
		if err != nil {
			return nil, err
		}
		offset += keySize

		interm.Keys = append(interm.Keys, key)
	}

	// Read number of pointers (2 bytes)
	if offset+2 > len(data) {
		return nil, errors.New("insufficient data for pointer count")
	}
	numPointers := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Skip pointer placeholders (8 bytes each)
	offset += int(numPointers) * 8

	return interm, nil
}

// decodeKey decodes a key from byte data and returns the key, size consumed, and any error
func (p *IndexPageCodec[K, V]) decodeKey(data []byte) (K, int, error) {
	if len(data) == 0 {
		var zero K
		return zero, 0, errors.New("empty data for key")
	}

	keyType := data[0]
	offset := 1

	switch keyType {
	case KeyTypeInt:
		if offset+4 > len(data) {
			var zero K
			return zero, 0, errors.New("insufficient data for int key")
		}
		intValue := int32(binary.LittleEndian.Uint32(data[offset : offset+4]))
		key := tree.IntKey(intValue)
		return any(key).(K), 5, nil // 1 byte type + 4 bytes value

	case KeyTypeFloat:
		if offset+8 > len(data) {
			var zero K
			return zero, 0, errors.New("insufficient data for float key")
		}
		uintValue := binary.LittleEndian.Uint64(data[offset : offset+8])
		floatValue := math.Float64frombits(uintValue)
		key := tree.FloatKey(floatValue)
		return any(key).(K), 9, nil // 1 byte type + 8 bytes value

	case KeyTypeString:
		if offset+2 > len(data) {
			var zero K
			return zero, 0, errors.New("insufficient data for string key length")
		}
		strLen := binary.LittleEndian.Uint16(data[offset : offset+2])
		offset += 2

		if offset+int(strLen) > len(data) {
			var zero K
			return zero, 0, errors.New("insufficient data for string key")
		}
		strValue := string(data[offset : offset+int(strLen)])
		key := tree.StringKey(strValue)
		return any(key).(K), 3 + int(strLen), nil // 1 byte type + 2 bytes length + string bytes

	default:
		var zero K
		return zero, 0, errors.New("unknown key type")
	}
}
