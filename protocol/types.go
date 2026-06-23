package protocol

// SchemaDef describes a single node in a Schema's type tree. Concretely it is
// one of:
//   - a primitive type tag string (Uint8, Uint16, Uint32, Int32, Float64,
//     Boolean, String, Bytes, ObjectAny, ArrayAny, Any)
//   - *ArrayDef, produced by Array(elementType) - mirrors JS `[elementType]`
//   - *ObjectDef, produced by Object(fields...) - mirrors a JS object literal
//
// Field order inside ObjectDef is significant: the binary layout is purely
// positional, so the same order must be used to encode and decode a given
// payload. Unlike a plain Go map (whose iteration order is randomized),
// ObjectDef preserves the order fields were declared in.
type SchemaDef interface{}

// Primitive type tags. These match the strings used by bit-socket-node.
const (
	Uint8     = "uint8"
	Uint16    = "uint16"
	Uint32    = "uint32"
	Int32     = "int32"
	Float64   = "float64"
	Boolean   = "boolean"
	String    = "string"
	Bytes     = "bytes"
	ObjectAny = "object" // dynamic msgpack-encoded fallback (map[string]interface{})
	ArrayAny  = "array"  // dynamic msgpack-encoded fallback ([]interface{})
	Any       = "any"    // dynamic msgpack-encoded fallback (anything)
)

// FieldDef is a single named, ordered field within an ObjectDef.
type FieldDef struct {
	Key  string
	Type SchemaDef
}

// F builds a FieldDef. Use it together with Object() to declare schemas:
//
//	protocol.Object(
//	    protocol.F("id", protocol.Uint32),
//	    protocol.F("name", protocol.String),
//	)
func F(key string, typ SchemaDef) FieldDef {
	return FieldDef{Key: key, Type: typ}
}

// ObjectDef is an ordered set of fields, mirroring a JS object literal used
// as a schema type definition.
type ObjectDef struct {
	Fields []FieldDef
}

// Object builds an ObjectDef from an ordered list of fields.
func Object(fields ...FieldDef) *ObjectDef {
	return &ObjectDef{Fields: fields}
}

// ArrayDef wraps an element type, mirroring JS's `[elementType]` schema
// array syntax (e.g. Array(String) ~= ['string'], Array(Array(Uint8)) ~=
// [['uint8']]).
type ArrayDef struct {
	Element SchemaDef
}

// Array builds an ArrayDef.
func Array(element SchemaDef) *ArrayDef {
	return &ArrayDef{Element: element}
}
