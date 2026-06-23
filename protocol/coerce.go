package protocol

// These helpers mirror the permissive `val || default` coercion that the
// original JavaScript implementation relies on when encoding schema values:
// missing/nil/wrong-typed values fall back to a sane zero value rather than
// panicking, so partially-populated payloads still encode deterministically.

func toUint64(v interface{}) uint64 {
	switch t := v.(type) {
	case nil:
		return 0
	case int:
		return uint64(t)
	case int8:
		return uint64(t)
	case int16:
		return uint64(t)
	case int32:
		return uint64(t)
	case int64:
		return uint64(t)
	case uint:
		return uint64(t)
	case uint8:
		return uint64(t)
	case uint16:
		return uint64(t)
	case uint32:
		return uint64(t)
	case uint64:
		return t
	case float32:
		return uint64(t)
	case float64:
		return uint64(t)
	default:
		return 0
	}
}

func toInt64(v interface{}) int64 {
	switch t := v.(type) {
	case nil:
		return 0
	case int:
		return int64(t)
	case int8:
		return int64(t)
	case int16:
		return int64(t)
	case int32:
		return int64(t)
	case int64:
		return t
	case uint:
		return int64(t)
	case uint8:
		return int64(t)
	case uint16:
		return int64(t)
	case uint32:
		return int64(t)
	case uint64:
		return int64(t)
	case float32:
		return int64(t)
	case float64:
		return int64(t)
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	switch t := v.(type) {
	case nil:
		return 0
	case float32:
		return float64(t)
	case float64:
		return t
	case int:
		return float64(t)
	case int8:
		return float64(t)
	case int16:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case uint:
		return float64(t)
	case uint8:
		return float64(t)
	case uint16:
		return float64(t)
	case uint32:
		return float64(t)
	case uint64:
		return float64(t)
	default:
		return 0
	}
}

func toBool(v interface{}) bool {
	b, _ := v.(bool)
	return b
}

func toStringVal(v interface{}) string {
	s, _ := v.(string)
	return s
}

func toBytesVal(v interface{}) []byte {
	switch t := v.(type) {
	case []byte:
		return t
	case nil:
		return []byte{}
	default:
		return []byte{}
	}
}

func toSlice(v interface{}) []interface{} {
	s, _ := v.([]interface{})
	return s
}

func toMap(v interface{}) map[string]interface{} {
	m, _ := v.(map[string]interface{})
	return m
}
