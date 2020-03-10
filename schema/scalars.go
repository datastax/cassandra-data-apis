package schema

import (
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"time"
)

var timestamp = graphql.NewScalar(graphql.ScalarConfig{
	Name: "Timestamp",
	Description: "The `Timestamp` scalar type represents a DateTime." +
		" The Timestamp is serialized as an RFC 3339 quoted string",
	Serialize:  serializeTimestamp,
	ParseValue: deserializeTimestamp,
	ParseLiteral: func(valueAST ast.Value) interface{} {
		switch valueAST := valueAST.(type) {
		case *ast.StringValue:
			return deserializeTimestamp(valueAST.Value)
		}
		return nil
	},
})

var uuid = graphql.NewScalar(graphql.ScalarConfig{
	Name:        "Uuid",
	Description: "The `Uuid` scalar type represents a CQL uuid as a string.",
	Serialize:   serializeUuid,
	ParseValue:  deserializeUuid,
	ParseLiteral: func(valueAST ast.Value) interface{} {
		switch valueAST := valueAST.(type) {
		case *ast.StringValue:
			return deserializeUuid(valueAST.Value)
		}
		return nil
	},
})

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

func deserializeUuid(value interface{}) interface{} {
	switch value := value.(type) {
	case []byte:
		t := gocql.UUID{}
		err := t.UnmarshalText(value)
		if err != nil {
			return nil
		}

		return t
	case string:
		return deserializeUuid([]byte(value))
	case *string:
		if value == nil {
			return nil
		}
		return deserializeUuid([]byte(*value))
	default:
		return nil
	}
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

func deserializeTimestamp(value interface{}) interface{} {
	switch value := value.(type) {
	case []byte:
		t := time.Time{}
		err := t.UnmarshalText(value)
		if err != nil {
			return nil
		}

		return t
	case string:
		return deserializeTimestamp([]byte(value))
	case *string:
		if value == nil {
			return nil
		}
		return deserializeTimestamp([]byte(*value))
	case time.Time:
		return value
	default:
		return nil
	}
}
