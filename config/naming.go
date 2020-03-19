package config

import "github.com/iancoleman/strcase"

type NamingConvention interface {
	ToCQLColumn(name string) string
	ToCQLTable(name string) string
	ToGraphQLField(name string) string
	ToGraphQLOperation(prefix string, name string) string
	ToGraphQLType(name string) string
	ToGraphQLEnum(name string) string
}

type defaultNaming struct{}

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
	return strcase.ToLowerCamel(prefix) + strcase.ToCamel(name)
}

func (n *defaultNaming) ToGraphQLType(name string) string {
	return strcase.ToCamel(name)
}

func (n *defaultNaming) ToGraphQLEnum(name string) string {
	return strcase.ToCamel(name)
}
