package protocol

import (
	"reflect"
	"testing"
)

func TestSchema_BasicTypes(t *testing.T) {
	userSchema := MustNewSchema("USER_TEST", Object(
		F("id", Uint32),
		F("name", String),
		F("isActive", Boolean),
	))

	payload := map[string]interface{}{"id": 1045, "name": "Ana", "isActive": true}

	buf, err := userSchema.EncodePayload(payload)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// 4 (uint32) + 4 (string length) + 3 ("Ana") + 1 (boolean) = 12 bytes
	if len(buf) != 12 {
		t.Fatalf("expected 12 bytes, got %d", len(buf))
	}

	decoded, err := userSchema.DecodePayload(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	dm := decoded.(map[string]interface{})
	if toUint64(dm["id"]) != 1045 || dm["name"] != "Ana" || dm["isActive"] != true {
		t.Fatalf("roundtrip mismatch: %+v", dm)
	}
}

func TestSchema_EmptyStringsAndBytes(t *testing.T) {
	edgeSchema := MustNewSchema("EDGE_TEST", Object(
		F("buf", Bytes),
		F("text", String),
	))

	payload := map[string]interface{}{"buf": []byte{}, "text": ""}
	buf, err := edgeSchema.EncodePayload(payload)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(buf) != 8 {
		t.Fatalf("expected 8 bytes, got %d", len(buf))
	}

	decoded, err := edgeSchema.DecodePayload(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	dm := decoded.(map[string]interface{})
	if dm["text"] != "" {
		t.Fatalf("expected empty text, got %v", dm["text"])
	}
	if len(dm["buf"].([]byte)) != 0 {
		t.Fatalf("expected empty buf")
	}
}

func TestSchema_AllNumericTypes(t *testing.T) {
	numSchema := MustNewSchema("NUM_TEST", Object(
		F("u8", Uint8),
		F("u16", Uint16),
		F("u32", Uint32),
		F("i32", Int32),
		F("f64", Float64),
	))

	payload := map[string]interface{}{
		"u8":  255,
		"u16": 65535,
		"u32": 4294967295,
		"i32": -2147483648,
		"f64": 3.14159265359,
	}

	buf, err := numSchema.EncodePayload(payload)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(buf) != 19 {
		t.Fatalf("expected 19 bytes, got %d", len(buf))
	}

	decoded, err := numSchema.DecodePayload(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	dm := decoded.(map[string]interface{})
	if toUint64(dm["u8"]) != 255 || toUint64(dm["u16"]) != 65535 || toUint64(dm["u32"]) != 4294967295 {
		t.Fatalf("unsigned mismatch: %+v", dm)
	}
	if toInt64(dm["i32"]) != -2147483648 {
		t.Fatalf("i32 mismatch: %v", dm["i32"])
	}
	if toFloat64(dm["f64"]) != 3.14159265359 {
		t.Fatalf("f64 mismatch: %v", dm["f64"])
	}
}

func TestSchema_NestedObjects(t *testing.T) {
	nestedSchema := MustNewSchema("NESTED_TEST", Object(
		F("id", Uint32),
		F("profile", Object(
			F("age", Uint8),
			F("isActive", Boolean),
		)),
	))

	payload := map[string]interface{}{
		"id": 999,
		"profile": map[string]interface{}{
			"age":      25,
			"isActive": true,
		},
	}

	buf, err := nestedSchema.EncodePayload(payload)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(buf) != 6 {
		t.Fatalf("expected 6 bytes, got %d", len(buf))
	}

	decoded, err := nestedSchema.DecodePayload(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	dm := decoded.(map[string]interface{})
	profile := dm["profile"].(map[string]interface{})
	if toUint64(dm["id"]) != 999 || toUint64(profile["age"]) != 25 || profile["isActive"] != true {
		t.Fatalf("nested mismatch: %+v", dm)
	}
}

func TestSchema_Arrays(t *testing.T) {
	arrSchema := MustNewSchema("ARR_TEST", Object(
		F("tags", Array(String)),
		F("matrix", Array(Array(Uint8))),
	))

	payload := map[string]interface{}{
		"tags":   []interface{}{"alpha", "beta"},
		"matrix": []interface{}{[]interface{}{1, 2}, []interface{}{3, 4}},
	}

	buf, err := arrSchema.EncodePayload(payload)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := arrSchema.DecodePayload(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	dm := decoded.(map[string]interface{})
	tags := dm["tags"].([]interface{})
	if tags[0] != "alpha" || tags[1] != "beta" {
		t.Fatalf("tags mismatch: %+v", tags)
	}
}

func TestSchema_DynamicFallbacks(t *testing.T) {
	dynSchema := MustNewSchema("DYN_TEST", Object(
		F("metadata", ObjectAny),
		F("list", ArrayAny),
	))

	payload := map[string]interface{}{
		"metadata": map[string]interface{}{"arbitrary": "data", "val": 42},
		"list":     []interface{}{1, "two", map[string]interface{}{"three": 3}},
	}

	buf, err := dynSchema.EncodePayload(payload)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := dynSchema.DecodePayload(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	dm := decoded.(map[string]interface{})
	meta := dm["metadata"].(map[string]interface{})
	if meta["arbitrary"] != "data" {
		t.Fatalf("metadata mismatch: %+v", meta)
	}
}

func TestSchema_InvalidName(t *testing.T) {
	_, err := NewSchema("bad name!", Object(F("x", Uint8)))
	if err == nil {
		t.Fatalf("expected error for invalid schema name")
	}
}

func TestWireSchemaDef_OrderPreserved(t *testing.T) {
	def := Object(
		F("z_first", Uint8),
		F("a_second", String),
		F("m_third", Object(F("nested", Float64))),
	)
	encoded, err := DefaultEncodePayload(map[string]interface{}{"EVT": wireSchemaDef{Def: def}})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	defs, err := DecodeNamespaceSchemaPayload(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := defs["EVT"].(*ObjectDef)
	if !ok {
		t.Fatalf("expected *ObjectDef, got %T", defs["EVT"])
	}
	var gotKeys []string
	for _, f := range got.Fields {
		gotKeys = append(gotKeys, f.Key)
	}
	want := []string{"z_first", "a_second", "m_third"}
	if !reflect.DeepEqual(gotKeys, want) {
		t.Fatalf("field order not preserved: got %v want %v", gotKeys, want)
	}
}
