package protocol

import (
	"bytes"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// wireSchemaDef adapts a SchemaDef tree to msgpack.CustomEncoder so it can be
// embedded inside a larger interface{} payload (e.g. a CONNECT frame's
// namespace->event->definition export) while still being encoded using an
// explicit field order. A plain map[string]interface{} can't be used for
// this purpose because Go map iteration order is randomized, which would
// silently corrupt the schema (and therefore every payload encoded against
// it) for any client that has to reconstruct field order from the wire.
type wireSchemaDef struct{ Def SchemaDef }

var _ msgpack.CustomEncoder = wireSchemaDef{}

// WireSchemaDef wraps a SchemaDef so it can be placed inside a larger
// interface{} value (e.g. a map[string]interface{} payload) and still
// msgpack-encode with its field order intact. Use this whenever a SchemaDef
// needs to travel over the wire, such as when exporting a namespace's
// schemas for client auto-sync.
func WireSchemaDef(def SchemaDef) interface{} {
	return wireSchemaDef{Def: def}
}

func (w wireSchemaDef) EncodeMsgpack(enc *msgpack.Encoder) error {
	return writeSchemaDef(enc, w.Def)
}

func writeSchemaDef(enc *msgpack.Encoder, def SchemaDef) error {
	switch t := def.(type) {
	case string:
		return enc.EncodeString(t)
	case *ArrayDef:
		if err := enc.EncodeArrayLen(1); err != nil {
			return err
		}
		return writeSchemaDef(enc, t.Element)
	case *ObjectDef:
		if err := enc.EncodeMapLen(len(t.Fields)); err != nil {
			return err
		}
		for _, f := range t.Fields {
			if err := enc.EncodeString(f.Key); err != nil {
				return err
			}
			if err := writeSchemaDef(enc, f.Type); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("bitsocket schema error: invalid schema type definition")
	}
}

// decodeSchemaDef reads one order-preserved SchemaDef value from dec.
func decodeSchemaDef(dec *msgpack.Decoder) (SchemaDef, error) {
	code, err := dec.PeekCode()
	if err != nil {
		return nil, err
	}
	switch {
	case msgpcode.IsString(code) || msgpcode.IsBin(code):
		return dec.DecodeString()
	case msgpcode.IsFixedArray(code) || code == msgpcode.Array16 || code == msgpcode.Array32:
		n, err := dec.DecodeArrayLen()
		if err != nil {
			return nil, err
		}
		if n != 1 {
			return nil, fmt.Errorf("bitsocket schema error: invalid array type definition on wire")
		}
		elem, err := decodeSchemaDef(dec)
		if err != nil {
			return nil, err
		}
		return &ArrayDef{Element: elem}, nil
	case msgpcode.IsFixedMap(code) || code == msgpcode.Map16 || code == msgpcode.Map32:
		n, err := dec.DecodeMapLen()
		if err != nil {
			return nil, err
		}
		fields := make([]FieldDef, 0, n)
		for i := 0; i < n; i++ {
			key, err := dec.DecodeString()
			if err != nil {
				return nil, err
			}
			val, err := decodeSchemaDef(dec)
			if err != nil {
				return nil, err
			}
			fields = append(fields, FieldDef{Key: key, Type: val})
		}
		return &ObjectDef{Fields: fields}, nil
	default:
		return nil, fmt.Errorf("bitsocket schema error: unsupported value on wire (code=%v)", code)
	}
}

// decodeSchemaDefMap reads an order-preserved map[string]SchemaDef, i.e. a
// single namespace's exported {eventName: definition} table.
func decodeSchemaDefMap(dec *msgpack.Decoder) (map[string]SchemaDef, error) {
	n, err := dec.DecodeMapLen()
	if err != nil {
		return nil, err
	}
	out := make(map[string]SchemaDef, n)
	for i := 0; i < n; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return nil, err
		}
		val, err := decodeSchemaDef(dec)
		if err != nil {
			return nil, err
		}
		out[key] = val
	}
	return out, nil
}

// DecodeNamespaceSchemaPayload decodes a deflate+msgpack-compressed
// sub-namespace CONNECT payload (an {eventName: definition} table) into a
// field-order-preserving map[string]SchemaDef.
func DecodeNamespaceSchemaPayload(raw []byte) (map[string]SchemaDef, error) {
	inflated, err := inflateRaw(raw)
	if err != nil {
		return nil, err
	}
	dec := msgpack.NewDecoder(bytes.NewReader(inflated))
	return decodeSchemaDefMap(dec)
}

// DecodeRootSchemaPayload decodes a deflate+msgpack-compressed root CONNECT
// payload (a {namespaceKey: {eventName: definition}} table) into a
// field-order-preserving map[string]map[string]SchemaDef.
func DecodeRootSchemaPayload(raw []byte) (map[string]map[string]SchemaDef, error) {
	inflated, err := inflateRaw(raw)
	if err != nil {
		return nil, err
	}
	dec := msgpack.NewDecoder(bytes.NewReader(inflated))
	n, err := dec.DecodeMapLen()
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[string]SchemaDef, n)
	for i := 0; i < n; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return nil, err
		}
		inner, err := decodeSchemaDefMap(dec)
		if err != nil {
			return nil, err
		}
		out[key] = inner
	}
	return out, nil
}
