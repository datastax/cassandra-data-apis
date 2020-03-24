package graphql

import (
	"encoding"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"gopkg.in/inf.v0"
	"net"
	"strconv"
	"time"
)

var timestamp = newStringScalar(
	"Timestamp", "The `Timestamp` scalar type represents a DateTime."+
		" The Timestamp is serialized as an RFC 3339 quoted string",
	serializeTimestamp,
	deserializeTimestamp)

var uuid = newStringScalar(
	"Uuid", "The `Uuid` scalar type represents a CQL uuid as a string.", serializeUuid, deserializeUuid)

var timeuuid = newStringScalar(
	"TimeUuid", "The `TimeUuid` scalar type represents a CQL timeuuid as a string.", serializeUuid, deserializeUuid)

var ip = newStringScalar(
	"Inet", "The `Inet` scalar type represents a CQL inet as a string.", serializeIp, deserializeIp)

var bigint = newStringScalar(
	"BigInt", "The `BigInt` scalar type represents a CQL bigint (64-bit signed integer) as a string.",
	serializeBigInt, deserializeBigInt)

var decimal = newStringScalar(
	"Decimal", "The `Decimal` scalar type represents a CQL decimal as a string.",
	serializeDecimal, deserializeDecimal)

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

var deserializeUuid = deserializeFromUnmarshaler(func() encoding.TextUnmarshaler {
	return &gocql.UUID{}
})

var deserializeTimestamp = deserializeFromUnmarshaler(func() encoding.TextUnmarshaler {
	return &time.Time{}
})

var deserializeIp = deserializeFromUnmarshaler(func() encoding.TextUnmarshaler {
	return &net.IP{}
})

var deserializeDecimal = deserializeFromUnmarshaler(func() encoding.TextUnmarshaler {
	return &inf.Dec{}
})

func parseLiteralFromStringHandler(parser graphql.ParseValueFn) graphql.ParseLiteralFn {
	return func(valueAST ast.Value) interface{} {
		switch valueAST := valueAST.(type) {
		case *ast.StringValue:
			return parser(valueAST.Value)
		}
		return nil
	}
}

func deserializeBigInt(value interface{}) interface{} {
	switch value := value.(type) {
	case []byte:
		return deserializeBigInt(string(value))
	case string:
		intValue, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil
		}
		return intValue
	case *string:
		if value == nil {
			return nil
		}
		return deserializeBigInt(*value)
	default:
		return nil
	}
}

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
	case time.Time:
		return marshalText(value)
	case *time.Time:
		return marshalText(value)
	default:
		return value
	}
}

func serializeUuid(value interface{}) interface{} {
	switch value := value.(type) {
	case gocql.UUID:
		return marshalText(value)
	case *gocql.UUID:
		return marshalText(value)
	default:
		return value
	}
}

func serializeIp(value interface{}) interface{} {
	switch value := value.(type) {
	case net.IP:
		return marshalText(value)
	case *net.IP:
		return marshalText(value)
	default:
		return value
	}
}

func serializeBigInt(value interface{}) interface{} {
	switch value := value.(type) {
	case int64:
		return strconv.FormatInt(value, 10)
	case *int64:
		return strconv.FormatInt(*value, 10)
	default:
		return value
	}
}

func serializeDecimal(value interface{}) interface{} {
	switch value := value.(type) {
	case inf.Dec:
		return value.String()
	case *inf.Dec:
		return value.String()
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
