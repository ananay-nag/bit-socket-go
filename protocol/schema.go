package protocol

import (
	"encoding/binary"
	"fmt"
	"math"
	"regexp"

	"github.com/vmihailenco/msgpack/v5"
)

var schemaNameRe = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// Schema describes a fixed binary layout for an event's payload, allowing
// payloads to skip the generic msgpack+deflate envelope entirely. The wire
// format is purely positional and matches bit-socket-node's schema.js byte
// for byte:
//
//	uint8/boolean -> 1 byte
//	uint16        -> 2 bytes (big-endian)
//	uint32/int32  -> 4 bytes (big-endian)
//	float64       -> 8 bytes (big-endian)
//	string/bytes  -> 4-byte big-endian length prefix + raw bytes
//	object/array/any -> 4-byte big-endian length prefix + msgpack bytes
//	ArrayDef      -> 4-byte big-endian element count + each encoded element
//	ObjectDef     -> each field encoded in declaration order, back to back
type Schema struct {
	Name       string
	Definition SchemaDef
}

// NewSchema constructs a Schema. The name must be a single word containing
// only letters, numbers, and underscores (matching the JS implementation's
// validation), or empty, in which case it defaults to "unknown".
func NewSchema(name string, definition SchemaDef) (*Schema, error) {
	if name == "" {
		name = "unknown"
	} else if !schemaNameRe.MatchString(name) {
		return nil, fmt.Errorf("invalid schema name: '%s'. schema names must be a single word containing only letters, numbers, and underscores (no spaces or special characters)", name)
	}
	return &Schema{Name: name, Definition: definition}, nil
}

// MustNewSchema is like NewSchema but panics on error. Convenient for
// package-level schema declarations.
func MustNewSchema(name string, definition SchemaDef) *Schema {
	s, err := NewSchema(name, definition)
	if err != nil {
		panic(err)
	}
	return s
}

// EncodePayload encodes payload according to the schema's definition.
func (s *Schema) EncodePayload(payload interface{}) ([]byte, error) {
	var queue [][]byte
	size, err := computeSize(s.Definition, payload, &queue)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, size)
	off := &offsetRef{0}
	qi := &queueIdx{0}
	if err := encodeValue(s.Definition, payload, buf, off, queue, qi); err != nil {
		return nil, err
	}
	return buf, nil
}

// DecodePayload decodes buf according to the schema's definition.
func (s *Schema) DecodePayload(buf []byte) (interface{}, error) {
	off := &offsetRef{0}
	return decodeValue(s.Definition, buf, off)
}

type offsetRef struct{ offset int }
type queueIdx struct{ idx int }

func computeSize(def SchemaDef, val interface{}, queue *[][]byte) (int, error) {
	switch t := def.(type) {
	case string:
		switch t {
		case Uint8, Boolean:
			return 1, nil
		case Uint16:
			return 2, nil
		case Uint32, Int32:
			return 4, nil
		case Float64:
			return 8, nil
		case String:
			b := []byte(toStringVal(val))
			*queue = append(*queue, b)
			return 4 + len(b), nil
		case Bytes:
			b := toBytesVal(val)
			return 4 + len(b), nil
		case ObjectAny, ArrayAny, Any:
			var v interface{} = val
			packed, err := msgpack.Marshal(v)
			if err != nil {
				return 0, err
			}
			*queue = append(*queue, packed)
			return 4 + len(packed), nil
		default:
			return 0, fmt.Errorf("bitsocket schema error: unsupported type '%s'", t)
		}
	case *ArrayDef:
		arr := toSlice(val)
		size := 4
		for _, item := range arr {
			s, err := computeSize(t.Element, item, queue)
			if err != nil {
				return 0, err
			}
			size += s
		}
		return size, nil
	case *ObjectDef:
		size := 0
		obj := toMap(val)
		for _, f := range t.Fields {
			var fv interface{}
			if obj != nil {
				fv = obj[f.Key]
			}
			s, err := computeSize(f.Type, fv, queue)
			if err != nil {
				return 0, err
			}
			size += s
		}
		return size, nil
	default:
		return 0, fmt.Errorf("bitsocket schema error: invalid schema type definition")
	}
}

func encodeValue(def SchemaDef, val interface{}, buf []byte, off *offsetRef, queue [][]byte, qi *queueIdx) error {
	switch t := def.(type) {
	case string:
		switch t {
		case Uint8:
			buf[off.offset] = byte(toUint64(val))
			off.offset++
		case Boolean:
			if toBool(val) {
				buf[off.offset] = 1
			} else {
				buf[off.offset] = 0
			}
			off.offset++
		case Uint16:
			binary.BigEndian.PutUint16(buf[off.offset:], uint16(toUint64(val)))
			off.offset += 2
		case Uint32:
			binary.BigEndian.PutUint32(buf[off.offset:], uint32(toUint64(val)))
			off.offset += 4
		case Int32:
			binary.BigEndian.PutUint32(buf[off.offset:], uint32(int32(toInt64(val))))
			off.offset += 4
		case Float64:
			binary.BigEndian.PutUint64(buf[off.offset:], math.Float64bits(toFloat64(val)))
			off.offset += 8
		case String, ObjectAny, ArrayAny, Any:
			enc := queue[qi.idx]
			qi.idx++
			binary.BigEndian.PutUint32(buf[off.offset:], uint32(len(enc)))
			off.offset += 4
			copy(buf[off.offset:], enc)
			off.offset += len(enc)
		case Bytes:
			b := toBytesVal(val)
			binary.BigEndian.PutUint32(buf[off.offset:], uint32(len(b)))
			off.offset += 4
			copy(buf[off.offset:], b)
			off.offset += len(b)
		}
		return nil
	case *ArrayDef:
		arr := toSlice(val)
		binary.BigEndian.PutUint32(buf[off.offset:], uint32(len(arr)))
		off.offset += 4
		for _, item := range arr {
			if err := encodeValue(t.Element, item, buf, off, queue, qi); err != nil {
				return err
			}
		}
		return nil
	case *ObjectDef:
		obj := toMap(val)
		for _, f := range t.Fields {
			var fv interface{}
			if obj != nil {
				fv = obj[f.Key]
			}
			if err := encodeValue(f.Type, fv, buf, off, queue, qi); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

func decodeValue(def SchemaDef, buf []byte, off *offsetRef) (interface{}, error) {
	switch t := def.(type) {
	case string:
		switch t {
		case Uint8:
			v := buf[off.offset]
			off.offset++
			return uint8(v), nil
		case Boolean:
			v := buf[off.offset] != 0
			off.offset++
			return v, nil
		case Uint16:
			v := binary.BigEndian.Uint16(buf[off.offset:])
			off.offset += 2
			return v, nil
		case Uint32:
			v := binary.BigEndian.Uint32(buf[off.offset:])
			off.offset += 4
			return v, nil
		case Int32:
			v := int32(binary.BigEndian.Uint32(buf[off.offset:]))
			off.offset += 4
			return v, nil
		case Float64:
			v := math.Float64frombits(binary.BigEndian.Uint64(buf[off.offset:]))
			off.offset += 8
			return v, nil
		case String:
			n := int(binary.BigEndian.Uint32(buf[off.offset:]))
			off.offset += 4
			v := string(buf[off.offset : off.offset+n])
			off.offset += n
			return v, nil
		case Bytes:
			n := int(binary.BigEndian.Uint32(buf[off.offset:]))
			off.offset += 4
			v := make([]byte, n)
			copy(v, buf[off.offset:off.offset+n])
			off.offset += n
			return v, nil
		case ObjectAny, ArrayAny, Any:
			n := int(binary.BigEndian.Uint32(buf[off.offset:]))
			off.offset += 4
			sub := buf[off.offset : off.offset+n]
			off.offset += n
			var v interface{}
			if err := msgpack.Unmarshal(sub, &v); err != nil {
				return nil, err
			}
			return v, nil
		default:
			return nil, fmt.Errorf("bitsocket schema error: unsupported type '%s'", t)
		}
	case *ArrayDef:
		n := int(binary.BigEndian.Uint32(buf[off.offset:]))
		off.offset += 4
		arr := make([]interface{}, n)
		for i := 0; i < n; i++ {
			v, err := decodeValue(t.Element, buf, off)
			if err != nil {
				return nil, err
			}
			arr[i] = v
		}
		return arr, nil
	case *ObjectDef:
		obj := map[string]interface{}{}
		for _, f := range t.Fields {
			v, err := decodeValue(f.Type, buf, off)
			if err != nil {
				return nil, err
			}
			obj[f.Key] = v
		}
		return obj, nil
	default:
		return nil, fmt.Errorf("bitsocket schema error: invalid schema type definition")
	}
}
