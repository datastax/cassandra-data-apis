package graphql

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/iancoleman/strcase"
)

type KeyspaceGraphQLSchema struct {
	// Table agnostic types
	commonInputTypes []*graphql.InputObject
	// A map containing the table type by table name, with each column as scalar value
	tableValueTypes map[string]*graphql.Object
	// A map containing the result type by table name for a select query
	resultSelectTypes map[string]*graphql.Object
	// A map containing the result type by table name for a update/insert/delete query
	resultUpdateTypes map[string]*graphql.Object
	// A map containing the order enum by table name
	orderEnums map[string]*graphql.Type
}

func (s *KeyspaceGraphQLSchema) BuildTypes(keyspace *gocql.KeyspaceMetadata) error {
	s.buildCommonInputTypes()
	s.buildOrderEnums(keyspace)
	s.buildTableValueTypes(keyspace)
	s.buildResultTypes(keyspace)
	return nil
}

func (s *KeyspaceGraphQLSchema) buildCommonInputTypes() {
	s.commonInputTypes = []*graphql.InputObject{
		graphql.NewInputObject(graphql.InputObjectConfig{
			Name: "QueryOptions",
			Fields: graphql.InputObjectConfigFieldMap{
				"limit":     {Type: graphql.Int},
				"pageSize":  {Type: graphql.Int},
				"pageState": {Type: graphql.String},
			},
		}),
		graphql.NewInputObject(graphql.InputObjectConfig{
			Name: "UpdateOptions",
			Fields: graphql.InputObjectConfigFieldMap{
				"ttl": {Type: graphql.Int},
			},
		}),
	}
}

func (s *KeyspaceGraphQLSchema) buildOrderEnums(keyspace *gocql.KeyspaceMetadata) {
	s.orderEnums = make(map[string]*graphql.Type, len(keyspace.Tables))
	for _, table := range keyspace.Tables {
		values := make(map[string]*graphql.EnumValueConfig, len(table.Columns))
		for _, column := range table.Columns {
			values[strcase.ToCamel(column.Name)+"_ASC"] = &graphql.EnumValueConfig{
				Value: column.Name + "_ASC",
				Description: fmt.Sprintf("Order %s by %s in a	scending order", table.Name, column.Name),
			}
			values[strcase.ToCamel(column.Name)+"_DESC"] = &graphql.EnumValueConfig{
				Value:       column.Name + "_DESC",
				Description: fmt.Sprintf("Order %s by %s in descending order", table.Name, column.Name),
			}
		}

		enumType := graphql.Type(graphql.NewEnum(graphql.EnumConfig{
			Name:   strcase.ToCamel(table.Name + "Order"),
			Values: values,
		}))

		s.orderEnums[table.Name] = &enumType
	}
}

func (s *KeyspaceGraphQLSchema) AllTypes() []graphql.Type {
	result := make([]graphql.Type, 0,
		len(s.commonInputTypes)+len(Scalars)+len(s.orderEnums)+len(s.tableValueTypes)*3)

	for _, t := range s.commonInputTypes {
		result = append(result, t)
	}

	for _, scalar := range Scalars {
		result = append(result, scalar)
	}

	for _, t := range s.orderEnums {
		result = append(result, *t)
	}

	for _, t := range s.tableValueTypes {
		result = append(result, t)
	}

	for _, t := range s.resultSelectTypes {
		result = append(result, t)
	}

	for _, t := range s.resultUpdateTypes {
		result = append(result, t)
	}

	return result
}

func (s *KeyspaceGraphQLSchema) buildTableValueTypes(keyspace *gocql.KeyspaceMetadata) {
	s.tableValueTypes = make(map[string]*graphql.Object, len(keyspace.Tables))

	for _, table := range keyspace.Tables {
		fields := graphql.Fields{}

		for name, column := range table.Columns {
			fields[strcase.ToLowerCamel(name)] = &graphql.Field{
				Type: buildType(column.Type),
			}
		}

		s.tableValueTypes[table.Name] = graphql.NewObject(graphql.ObjectConfig{
			Name:   strcase.ToCamel(table.Name),
			Fields: fields,
		})
	}
}

func (s *KeyspaceGraphQLSchema) buildResultTypes(keyspace *gocql.KeyspaceMetadata) {
	s.resultSelectTypes = make(map[string]*graphql.Object, len(keyspace.Tables))
	s.resultUpdateTypes = make(map[string]*graphql.Object, len(keyspace.Tables))

	for _, table := range keyspace.Tables {
		itemType, ok := s.tableValueTypes[table.Name]

		if !ok {
			panic(fmt.Sprintf("Table value type for table '%s' not found", table.Name))
		}

		s.resultSelectTypes[table.Name] = graphql.NewObject(graphql.ObjectConfig{
			Name: strcase.ToCamel(table.Name + "Result"),
			Fields: graphql.Fields{
				"pageState": {Type: graphql.String},
				"values":    {Type: graphql.NewList(graphql.NewNonNull(itemType))},
			},
		})

		s.resultUpdateTypes[table.Name] = graphql.NewObject(graphql.ObjectConfig{
			Name: strcase.ToCamel(table.Name + "MutationResult"),
			Fields: graphql.Fields{
				"applied": {Type: graphql.NewNonNull(graphql.Boolean)},
				"value":   {Type: itemType},
			},
		})
	}
}
