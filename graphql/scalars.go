package graphql

import (
	"github.com/datastax/cassandra-data-apis/types"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"strconv"
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
	types.StringerToString, errToNilDeserializer(deserializerWithErrorFn(types.StringToDecimal)))

var varint = newStringScalar(
	"Varint", "The `Varint` scalar type represents a CQL varint as a string.",
	types.StringerToString, errToNilDeserializer(deserializerWithErrorFn(types.StringToBigInt)))

var float32Scalar = graphql.NewScalar(graphql.ScalarConfig{
	Name:        "Float32",
	Description: "The `Float32` scalar type represents a CQL float (single-precision floating point values).",
	Serialize:   identityFn,
	ParseValue:  deserializeFloat32,
	ParseLiteral: func(valueAST ast.Value) interface{} {
		switch valueAST := valueAST.(type) {
		case *ast.FloatValue:
			return deserializeFloat32(valueAST.Value)
		case *ast.IntValue:
			return deserializeFloat32(valueAST.Value)
		}
		return nil
	},
})

var blob = newStringScalar(
	"Blob", "The `Blob` scalar type represents a CQL blob as a base64 encoded byte array.",
	types.ByteArrayToBase64String, errToNilDeserializer(types.Base64StringToByteArray))

var timestamp = newStringScalar(
	"Timestamp", "The `Timestamp` scalar type represents a DateTime."+
		" The Timestamp is serialized as a RFC 3339 quoted string",
	types.TimeAsString, errToNilDeserializer(deserializerWithErrorFn(types.StringToTime)))

var localTime = newStringScalar(
	"Time", "The `Time` scalar type represents a local time."+
		" Values are represented as strings, such as 13:30:54.234..",
	types.DurationToCqlFormattedString, errToNilDeserializer(types.CqlFormattedStringToDuration))

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

type deserializerWithErrorFn func(interface{}) (interface{}, error)

func errToNilDeserializer(fn deserializerWithErrorFn) graphql.ParseValueFn {
	return func(value interface{}) interface{} {
		result, err := fn(value)
		if err != nil {
			return nil
		}
		return result
	}
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
