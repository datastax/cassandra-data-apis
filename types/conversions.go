package types

import (
	"encoding"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
	"math/big"
	"strings"
	"time"
)

type toJsonFn func(value interface{}) interface{}
type fromJsonFn func(value interface{}) (interface{}, error)

func ToJsonValues(rows []map[string]interface{}, table *gocql.TableMetadata) []map[string]interface{} {
	rowsLength := len(rows)
	if rowsLength == 0 {
		return rows
	}

	firstRow := rows[0]
	columnsLength := len(firstRow)
	converters := make(map[string]toJsonFn, columnsLength)
	result := make([]map[string]interface{}, rowsLength)

	for columnName := range firstRow {
		column := table.Columns[columnName]
		converters[columnName] = jsonConverterPerType(column.Type)
	}

	for i := 0; i < rowsLength; i++ {
		row := rows[i]
		item := make(map[string]interface{}, columnsLength)

		for columnName, value := range row {
			converter := converters[columnName]
			if value != nil {
				item[columnName] = converter(value)
			} else {
				item[columnName] = nil
			}
		}

		result[i] = item
	}

	return result
}

func FromJsonValue(value interface{}, typeInfo gocql.TypeInfo) (interface{}, error) {
	switch typeInfo.Type() {
	case gocql.TypeTimestamp:
		return StringToTime(value)
	case gocql.TypeDecimal:
		return StringToDecimal(value)
	case gocql.TypeVarint:
		return StringToBigInt(value)
	case gocql.TypeInt, gocql.TypeTinyInt, gocql.TypeSmallInt:
		return FloatToInt(value)
	case gocql.TypeBlob:
		return Base64StringToByteArray(value)
	case gocql.TypeFloat:
		return Float64ToFloat32(value)
	case gocql.TypeTime:
		return CqlFormattedStringToDuration(value)
	}
	return value, nil
}

func jsonConverterPerType(typeInfo gocql.TypeInfo) toJsonFn {
	switch typeInfo.Type() {
	case gocql.TypeVarint, gocql.TypeDecimal:
		return StringerToString
	case gocql.TypeBlob:
		return ByteArrayToBase64String
	case gocql.TypeTimestamp:
		return TimeAsString
	case gocql.TypeTime:
		return DurationToCqlFormattedString
	}

	return identityFn
}

func identityFn(value interface{}) interface{} {
	return value
}

func StringerToString(value interface{}) interface{} {
	switch value := value.(type) {
	case fmt.Stringer:
		if value == nil {
			return value
		}
		return value.String()
	default:
		return value
	}
}

func ByteArrayToBase64String(value interface{}) interface{} {
	switch value := value.(type) {
	case *[]byte:
		if value == nil {
			return value
		}
		return base64.StdEncoding.EncodeToString(*value)
	default:
		return value
	}
}

func TimeAsString(value interface{}) interface{} {
	switch value := value.(type) {
	case *time.Time:
		if value == nil {
			return value
		}
		return marshalText(value)
	default:
		return value
	}
}

func marshalText(value encoding.TextMarshaler) *string {
	buff, err := value.MarshalText()
	if err != nil {
		return nil
	}

	var s = string(buff)
	return &s
}

func DurationToCqlFormattedString(value interface{}) interface{} {
	switch value := value.(type) {
	case *time.Duration:
		if value == nil {
			return value
		}
		d := *value
		totalSeconds := d.Truncate(time.Second)
		remainingNanos := d - totalSeconds

		var (
			hours   = 0
			minutes = 0
		)
		secs := int(totalSeconds.Seconds())

		if secs >= 60 {
			minutes = secs / 60
			secs = secs % 60
		}
		if minutes >= 60 {
			hours = minutes / 60
			minutes = minutes % 60
		}

		nanosStr := ""
		if remainingNanos > 0 {
			nanosStr = fmt.Sprintf(".%09d", remainingNanos.Nanoseconds())
		}
		return fmt.Sprintf("%02d:%02d:%02d%s", hours, minutes, secs, nanosStr)
	default:
		return value
	}
}

func CqlFormattedStringToDuration(value interface{}) (interface{}, error) {
	switch value := value.(type) {
	case string:
		parts := strings.Split(value, ":")
		if len(parts) != 3 {
			return nil, errors.New("time has wrong format")
		}

		secs := parts[2]
		nanos := "0"
		if strings.Contains(parts[2], ".") {
			secParts := strings.Split(parts[2], ".")
			secs = secParts[0]
			nanos = secParts[1]
			// Pad right zeros
			if len(nanos) < 9 {
				nanos = nanos + strings.Repeat("0", 9-len(nanos))
			}
		}

		duration, err := time.ParseDuration(fmt.Sprintf("%sh%sm%ss%sns", parts[0], parts[1], secs, nanos))
		if err != nil {
			return nil, errors.New("time has wrong format")
		}
		return duration, nil
	default:
		return value, nil
	}
}

func Base64StringToByteArray(value interface{}) (interface{}, error) {
	switch value := value.(type) {
	case string:
		return base64.StdEncoding.DecodeString(value)
	default:
		return value, nil
	}
}

func FloatToInt(value interface{}) (interface{}, error) {
	if f, ok := value.(float64); ok {
		return int(f), nil
	}

	return nil, errors.New("wrong value provided for int type")
}

func Float64ToFloat32(value interface{}) (interface{}, error) {
	if f, ok := value.(float64); ok {
		return float32(f), nil
	}

	return nil, errors.New("wrong value provided for float (32) type")
}

func unmarshallerToText(factory func() encoding.TextUnmarshaler) fromJsonFn {
	return func(value interface{}) (interface{}, error) {
		switch value := value.(type) {
		case string:
			t := factory()
			err := t.UnmarshalText([]byte(value))
			if err != nil {
				return nil, err
			}

			return t, nil
		default:
			return value, nil
		}
	}
}

var StringToTime fromJsonFn = unmarshallerToText(func() encoding.TextUnmarshaler {
	return &time.Time{}
})

var StringToDecimal = unmarshallerToText(func() encoding.TextUnmarshaler {
	return &inf.Dec{}
})

var StringToBigInt = unmarshallerToText(func() encoding.TextUnmarshaler {
	return &big.Int{}
})
