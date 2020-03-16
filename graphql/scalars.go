package graphql

import (
	"encoding"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"time"
)

var Scalars = []*graphql.Scalar{timestamp, uuid}

var timestamp = graphql.NewScalar(graphql.ScalarConfig{
	Name: "Timestamp",
	Description: "The `Timestamp` scalar type represents a DateTime." +
		" The Timestamp is serialized as an RFC 3339 quoted string",
	Serialize:    serializeTimestamp,
	ParseValue:   deserializeTimestamp,
	ParseLiteral: parseLiteralFromStringHandler(deserializeTimestamp),
})

var uuid = graphql.NewScalar(graphql.ScalarConfig{
	Name:         "Uuid",
	Description:  "The `Uuid` scalar type represents a CQL uuid as a string.",
	Serialize:    serializeUuid,
	ParseValue:   deserializeUuid,
	ParseLiteral: parseLiteralFromStringHandler(deserializeUuid),
})

var deserializeUuid = deserializeFromUnmarshaler(func() encoding.TextUnmarshaler {
	return &gocql.UUID{}
})

var deserializeTimestamp = deserializeFromUnmarshaler(func() encoding.TextUnmarshaler {
	return &time.Time{}
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
			return nil
		}
	}

	return fn
}

func serializeTimestamp(value interface{}) interface{} {
	switch value := value.(type) {
	case time.Time:
		buff, err := value.MarshalText()
		if err != nil {
			return nil
		}

		return string(buff)
	case *time.Time:
		if value == nil {
			return nil
		}
		return serializeTimestamp(*value)
	default:
		return nil
	}
}

func serializeUuid(value interface{}) interface{} {
	switch value := value.(type) {
	case gocql.UUID:
		buff, err := value.MarshalText()
		if err != nil {
			return nil
		}

		return string(buff)
	case *gocql.UUID:
		if value == nil {
			return nil
		}
		return serializeUuid(*value)
	default:
		return nil
	}
}
