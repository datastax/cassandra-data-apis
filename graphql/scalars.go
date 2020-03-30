package graphql

import (
	"encoding"
	"fmt"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"gopkg.in/inf.v0"
	"time"
)

var uuid = newStringNativeScalar("Uuid", "The `Uuid` scalar type represents a CQL uuid as a string.")

var timeuuid = newStringNativeScalar("TimeUuid", "The `TimeUuid` scalar type represents a CQL timeuuid as a string.")

var ip = newStringNativeScalar("Inet", "The `Inet` scalar type represents a CQL inet as a string.")

var bigint = newStringNativeScalar(
	"BigInt", "The `BigInt` scalar type represents a CQL bigint (64-bit signed integer) as a string.")

var decimal = newStringScalar(
	"Decimal", "The `Decimal` scalar type represents a CQL decimal as a string.",
	serializeStringer, deserializeDecimal)

var timestamp = newStringScalar(
	"Timestamp", "The `Timestamp` scalar type represents a DateTime."+
		" The Timestamp is serialized as an RFC 3339 quoted string",
	serializeTimestamp, deserializeTimestamp)

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

func serializeStringer(value interface{}) interface{} {
	switch value := value.(type) {
	case fmt.Stringer:
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
