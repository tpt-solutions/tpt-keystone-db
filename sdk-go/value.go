package keystone

import (
	"fmt"
	"strconv"
)

// encodeParam converts a Go value into the extended query protocol's text
// format for a bound parameter. Returns (nil, true) for SQL NULL.
func encodeParam(v any) (data []byte, isNull bool) {
	switch t := v.(type) {
	case nil:
		return nil, true
	case string:
		return []byte(t), false
	case []byte:
		return t, false
	case bool:
		if t {
			return []byte("t"), false
		}
		return []byte("f"), false
	case int:
		return []byte(strconv.FormatInt(int64(t), 10)), false
	case int32:
		return []byte(strconv.FormatInt(int64(t), 10)), false
	case int64:
		return []byte(strconv.FormatInt(t, 10)), false
	case float32:
		return []byte(strconv.FormatFloat(float64(t), 'g', -1, 32)), false
	case float64:
		return []byte(strconv.FormatFloat(t, 'g', -1, 64)), false
	case fmt.Stringer:
		return []byte(t.String()), false
	default:
		return []byte(fmt.Sprint(v)), false
	}
}
