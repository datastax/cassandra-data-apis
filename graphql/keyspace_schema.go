package graphql

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/riptano/data-endpoints/config"
	"log"
)

type KeyspaceGraphQLSchema struct {
	// A set of ignored tables
	ignoredTables map[string]bool
	// A map containing the table type by table name, with each column as scalar value
	tableValueTypes map[string]*graphql.Object
	// A map containing the table input type by table name, with each column as scalar value
	tableScalarInputTypes map[string]*graphql.InputObject
	// A map containing the table type by table name, with each column as input filter
	tableOperatorInputTypes map[string]*graphql.InputObject
	// A map containing the result type by table name for a select query
	resultSelectTypes map[string]*graphql.Object
	// A map containing the result type by table name for a update/insert/delete query
	resultUpdateTypes map[string]*graphql.Object
	// A map containing the order enum by table name
	orderEnums map[string]*graphql.Enum
}

var inputQueryOptions = graphql.NewInputObject(graphql.InputObjectConfig{
	Name: "QueryOptions",
	Fields: graphql.InputObjectConfigFieldMap{
		"limit":     {Type: graphql.Int},
		"pageSize":  {Type: graphql.Int},
		"pageState": {Type: graphql.String},
	},
})

var inputMutationOptions = graphql.NewInputObject(graphql.InputObjectConfig{
	Name: "UpdateOptions",
	Fields: graphql.InputObjectConfigFieldMap{
		"ttl": {Type: graphql.Int},
	},
})

func (s *KeyspaceGraphQLSchema) BuildTypes(keyspace *gocql.KeyspaceMetadata, naming config.NamingConvention) error {
	s.buildOrderEnums(keyspace, naming)
	s.buildTableTypes(keyspace, naming)
	s.buildResultTypes(keyspace, naming)
	return nil
}

func (s *KeyspaceGraphQLSchema) buildOrderEnums(keyspace *gocql.KeyspaceMetadata, naming config.NamingConvention) {
	s.orderEnums = make(map[string]*graphql.Enum, len(keyspace.Tables))
	for _, table := range keyspace.Tables {
		values := make(map[string]*graphql.EnumValueConfig, len(table.Columns))
		for _, column := range table.Columns {
			values[naming.ToGraphQLEnumValue(column.Name)+"_ASC"] = &graphql.EnumValueConfig{
				Value: column.Name + "_ASC",
				Description: fmt.Sprintf("Order %s by %s in a	scending order", table.Name, column.Name),
			}
			values[naming.ToGraphQLEnumValue(column.Name)+"_DESC"] = &graphql.EnumValueConfig{
				Value:       column.Name + "_DESC",
				Description: fmt.Sprintf("Order %s by %s in descending order", table.Name, column.Name),
			}
		}

		s.orderEnums[table.Name] = graphql.NewEnum(graphql.EnumConfig{
			Name:   naming.ToGraphQLType(table.Name + "Order"),
			Values: values,
		})
	}
}

func (s *KeyspaceGraphQLSchema) buildTableTypes(keyspace *gocql.KeyspaceMetadata, naming config.NamingConvention) {
	s.tableValueTypes = make(map[string]*graphql.Object, len(keyspace.Tables))
	s.tableScalarInputTypes = make(map[string]*graphql.InputObject, len(keyspace.Tables))
	s.tableOperatorInputTypes = make(map[string]*graphql.InputObject, len(keyspace.Tables))

	for _, table := range keyspace.Tables {
		fields := graphql.Fields{}
		inputFields := graphql.InputObjectConfigFieldMap{}
		inputOperatorFields := graphql.InputObjectConfigFieldMap{}
		var err error

		for name, column := range table.Columns {
			var fieldType graphql.Output
			fieldName := naming.ToGraphQLField(name)
			fieldType, err = buildType(column.Type)
			if err != nil {
				log.Println(err)
				break
			}

			fields[fieldName] = &graphql.Field{Type: fieldType}
			inputFields[fieldName] = &graphql.InputObjectFieldConfig{Type: fieldType}

			t := operatorsInputTypes[column.Type.Type()]
			if t == nil {
				// Exit: this is a bug as no operator type was defined for a type
				log.Fatalf("No operator input type found for %s", column.Type.Type())
			}

			inputOperatorFields[fieldName] = &graphql.InputObjectFieldConfig{
				Type: t,
			}
		}

		if err != nil {
			log.Printf("Ignoring table %s", table.Name)
			s.ignoredTables[table.Name] = true
			err = nil
			continue
		}

		s.tableValueTypes[table.Name] = graphql.NewObject(graphql.ObjectConfig{
			Name:   naming.ToGraphQLType(table.Name),
			Fields: fields,
		})

		s.tableScalarInputTypes[table.Name] = graphql.NewInputObject(graphql.InputObjectConfig{
			Name:   naming.ToGraphQLType(table.Name) + "Input",
			Fields: inputFields,
		})

		s.tableOperatorInputTypes[table.Name] = graphql.NewInputObject(graphql.InputObjectConfig{
			Name:   naming.ToGraphQLType(table.Name) + "FilterInput",
			Fields: inputOperatorFields,
		})
	}
}

func (s *KeyspaceGraphQLSchema) buildResultTypes(keyspace *gocql.KeyspaceMetadata, naming config.NamingConvention) {
	s.resultSelectTypes = make(map[string]*graphql.Object, len(keyspace.Tables))
	s.resultUpdateTypes = make(map[string]*graphql.Object, len(keyspace.Tables))

	for _, table := range keyspace.Tables {
		if s.ignoredTables[table.Name] {
			continue
		}

		itemType, ok := s.tableValueTypes[table.Name]

		if !ok {
			panic(fmt.Sprintf("Table value type for table '%s' not found", table.Name))
		}

		s.resultSelectTypes[table.Name] = graphql.NewObject(graphql.ObjectConfig{
			Name: naming.ToGraphQLType(table.Name + "Result"),
			Fields: graphql.Fields{
				"pageState": {Type: graphql.String},
				"values":    {Type: graphql.NewList(graphql.NewNonNull(itemType))},
			},
		})

		s.resultUpdateTypes[table.Name] = graphql.NewObject(graphql.ObjectConfig{
			Name: naming.ToGraphQLType(table.Name + "MutationResult"),
			Fields: graphql.Fields{
				"applied": {Type: graphql.NewNonNull(graphql.Boolean)},
				"value":   {Type: itemType},
			},
		})
	}
}
