package config

import "github.com/iancoleman/strcase"

type NamingConvention interface {
	// ToCQLColumn converts a GraphQL/REST name to a CQL column name.
	ToCQLColumn(name string) string

	// ToCQLColumn converts a GraphQL/REST name to a CQL table name.
	ToCQLTable(name string) string

	// ToGraphQLField converts a CQL name (typically a column name) to a GraphQL field name.
	ToGraphQLField(name string) string

	// ToGraphQLOperation converts a CQL name (typically a table name) to a GraphQL operation name.
	ToGraphQLOperation(prefix string, name string) string

	// ToGraphQLType converts a CQL name (typically a table name) to a GraphQL type name.
	ToGraphQLType(name string) string

	// ToGraphQLEnumValue converts a CQL name to a GraphQL enumeration value name.
	ToGraphQLEnumValue(name string) string
}

type defaultNaming struct{}

// Default naming implementation.
var DefaultNaming = &defaultNaming{}

func (n *defaultNaming) ToCQLColumn(name string) string {
	// TODO: Fix numbers: "Table2" or "table2" --> "table_2"
	return strcase.ToSnake(name)
}

func (n *defaultNaming) ToCQLTable(name string) string {
	// TODO: Fix numbers: "Table2" or "table2" --> "table_2"
	return strcase.ToSnake(name)
}

func (n *defaultNaming) ToGraphQLField(name string) string {
	return strcase.ToLowerCamel(name)
}

func (n *defaultNaming) ToGraphQLOperation(prefix string, name string) string {
	if prefix == "" {
		return strcase.ToLowerCamel(name)
	} else {
		return strcase.ToLowerCamel(prefix) + strcase.ToCamel(name)
	}
}

func (n *defaultNaming) ToGraphQLType(name string) string {
	return strcase.ToCamel(name)
}

func (n *defaultNaming) ToGraphQLEnumValue(name string) string {
	return strcase.ToLowerCamel(name)
}
