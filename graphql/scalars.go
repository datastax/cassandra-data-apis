package graphql

import (
	"encoding"
	"encoding/base64"
	"fmt"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"gopkg.in/inf.v0"
	"math/big"
	"strconv"
	"strings"
	"time"
)

var uuid = newStringNativeScalar("Uuid", "The `Uuid` scalar type represents a CQL uuid as a string.")

var timeuuid = newStringNativeScalar("TimeUuid", "The `TimeUuid` scalar type represents a CQL timeuuid as a string.")

var ip = newStringNativeScalar("Inet", "The `Inet` scalar type represents a CQL inet as a string.")

var bigint = newStringNativeScalar(
	"BigInt", "The `BigInt` scalar type represents a CQL bigint (64-bit signed integer) as a string.")

var counter = newStringNativeScalar(
	"Counter", "The `Counter` scalar type represents a CQL counter (64-bit signed integer) as a string.")

var ascii = newStringNativeScalar(
	"Ascii", "The `Ascii` scalar type represents CQL ascii character values as a string.")

var decimal = newStringScalar(
	"Decimal", "The `Decimal` scalar type represents a CQL decimal as a string.",
	serializeStringer, deserializeDecimal)

var varint = newStringScalar(
	"Varint", "The `Varint` scalar type represents a CQL varint as a string.",
	serializeStringer, deserializeVarint)

var float32Scalar = graphql.NewScalar(graphql.ScalarConfig{
	Name:        "Float32",
	Description: "The `Float32` scalar type represents a CQL float (single-precision fractional values)",
	Serialize:   identityFn,
	ParseValue:  deserializeFloat32,
	ParseLiteral: func(valueAST ast.Value) interface{} {
		switch valueAST := valueAST.(type) {
		case *ast.FloatValue:
			return deserializeFloat32(valueAST.Value)
		}
		return nil
	},
})

var blob = newStringScalar(
	"Blob", "The `Blob` scalar type represents a CQL blob as a base64 encoded byte array.",
	serializeBlob, deserializeBlob)

var timestamp = newStringScalar(
	"Timestamp", "The `Timestamp` scalar type represents a DateTime."+
		" The Timestamp is serialized as an RFC 3339 quoted string",
	serializeTimestamp, deserializeTimestamp)

var localTime = newStringScalar(
	"Time", "The `Time` scalar type represents a local time."+
		" Values are represented as strings, such as 13:30:54.234..",
	serializeLocalTime, deserializeLocalTime)

// newStringNativeScalar Creates an string-based scalar with custom serialization functions
func newStringScalar(
	name string, description string, serializeFn graphql.SerializeFn, deserializeFn graphql.ParseValueFn,
) *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:         name,
		Description:  description,
		Serialize:    serializeFn,
		ParseValue:   deserializeFn,
		ParseLiteral: parseLiteralFromStringHandler(deserializeFn),
	})
}

// newStringNativeScalar Creates an string-based scalar that has native representation in gocql (no parsing or needed)
func newStringNativeScalar(name string, description string) *graphql.Scalar {
	return newStringScalar(name, description, identityFn, identityFn)
}

func identityFn(value interface{}) interface{} {
	return value
}

func parseLiteralFromStringHandler(parser graphql.ParseValueFn) graphql.ParseLiteralFn {
	return func(valueAST ast.Value) interface{} {
		switch valueAST := valueAST.(type) {
		case *ast.StringValue:
			return parser(valueAST.Value)
		}
		return nil
	}
}

var deserializeTimestamp = deserializeFromUnmarshaler(func() encoding.TextUnmarshaler {
	return &time.Time{}
})

var deserializeDecimal = deserializeFromUnmarshaler(func() encoding.TextUnmarshaler {
	return &inf.Dec{}
})

var deserializeVarint = deserializeFromUnmarshaler(func() encoding.TextUnmarshaler {
	return &big.Int{}
})

func deserializeFromUnmarshaler(factory func() encoding.TextUnmarshaler) graphql.ParseValueFn {
	var fn func(value interface{}) interface{}

	fn = func(value interface{}) interface{} {
		switch value := value.(type) {
		case []byte:
			t := factory()
			err := t.UnmarshalText(value)
			if err != nil {
				return nil
			}

			return t
		case string:
			return fn([]byte(value))
		case *string:
			if value == nil {
				return nil
			}
			return fn([]byte(*value))
		default:
			return value
		}
	}

	return fn
}

func serializeTimestamp(value interface{}) interface{} {
	switch value := value.(type) {
	case *time.Time:
		return marshalText(value)
	default:
		return value
	}
}

func serializeLocalTime(value interface{}) interface{} {
	switch value := value.(type) {
	case *time.Duration:
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

func deserializeLocalTime(value interface{}) interface{} {
	switch value := value.(type) {
	case string:
		parts := strings.Split(value, ":")
		if len(parts) != 3 {
			return nil
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
			return nil
		}
		return duration
	default:
		return value
	}
}

func deserializeFloat32(value interface{}) interface{} {
	switch value := value.(type) {
	case float64:
		return float32(value)
	case string:
		float64Value, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return nil
		}
		return float32(float64Value)
	default:
		return value
	}
}

func serializeStringer(value interface{}) interface{} {
	switch value := value.(type) {
	case fmt.Stringer:
		return value.String()
	default:
		return value
	}
}

func serializeBlob(value interface{}) interface{} {
	switch value := value.(type) {
	case *[]byte:
		return base64.StdEncoding.EncodeToString(*value)
	default:
		return value
	}
}

func deserializeBlob(value interface{}) interface{} {
	switch value := value.(type) {
	case string:
		blob, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil
		}
		return blob
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
