package graphql

import (
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
)

var stringOperatorType = operatorType(graphql.String)
var intOperatorType = operatorType(graphql.Int)
var floatOperatorType = operatorType(graphql.Float)

// cqlOperators contains the CQL operator for a given "graphql" operator
var cqlOperators = map[string]string{
	"eq":    "=",
	"notEq": "!=",
	"gt":    ">",
	"gte":   ">=",
	"lt":    "<",
	"lte":   "<=",
	"in":    "IN",
}

var operatorsInputTypes = map[gocql.Type]*graphql.InputObject{
	gocql.TypeInt:       intOperatorType,
	gocql.TypeTinyInt:   intOperatorType,
	gocql.TypeSmallInt:  intOperatorType,
	gocql.TypeText:      stringOperatorType,
	gocql.TypeVarchar:   stringOperatorType,
	gocql.TypeFloat:     floatOperatorType,
	gocql.TypeDouble:    floatOperatorType,
	gocql.TypeUUID:      operatorType(uuid),
	gocql.TypeTimestamp: operatorType(timestamp),
	gocql.TypeTimeUUID:  operatorType(timeuuid),
	gocql.TypeInet:      operatorType(ip),
}

func operatorType(graphqlType graphql.Type) *graphql.InputObject {
	return graphql.NewInputObject(graphql.InputObjectConfig{
		Name: graphqlType.Name() + "FilterInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"eq":    {Type: graphqlType},
			"notEq": {Type: graphqlType},
			"gt":    {Type: graphqlType},
			"gte":   {Type: graphqlType},
			"lt":    {Type: graphqlType},
			"lte":   {Type: graphqlType},
			"in":    {Type: graphql.NewList(graphqlType)},
		},
	})
}
