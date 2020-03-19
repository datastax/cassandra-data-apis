package config

import "github.com/iancoleman/strcase"

type NamingConvention interface {
	ToCQLColumn(name string) string
	ToCQLTable(name string) string

	ToGraphQLField(name string) string
	ToGraphQLFieldPrefix(prefix string, name string) string

	ToGraphQLType(name string) string
}

type defaultNaming struct {
}

func NewDefaultNaming() *defaultNaming {
	return &defaultNaming{}
}

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

func (n *defaultNaming) ToGraphQLFieldPrefix(prefix string, name string) string {
	return strcase.ToLowerCamel(prefix) + strcase.ToCamel(name)
}

func (n *defaultNaming) ToGraphQLType(name string) string {
	return strcase.ToCamel(name)
}
