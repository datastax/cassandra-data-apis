package graphql

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
)

var stringOperatorType = operatorType(graphql.String)
var intOperatorType = operatorType(graphql.Int)
var floatOperatorType = operatorType(graphql.Float)

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
	gocql.TypeTimeUUID:  operatorType(timeuuid), //TODO: Apply max/min to timeuuid
	gocql.TypeInet:      operatorType(ip),
	gocql.TypeBigInt:    operatorType(bigint),
	gocql.TypeDecimal:   operatorType(decimal),
	gocql.TypeVarint:    operatorType(varint),
	gocql.TypeBlob:      operatorType(blob),
}

func operatorType(graphqlType graphql.Type) *graphql.InputObject {
	return graphql.NewInputObject(graphql.InputObjectConfig{
		Description: fmt.Sprintf("Input type to be used in filter queries for the %s type.", graphqlType.Name()),
		Name:        graphqlType.Name() + "FilterInput",
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
