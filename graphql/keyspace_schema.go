package graphql

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/riptano/data-endpoints/config"
	"github.com/riptano/data-endpoints/types"
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
	// A map containing key/value types for maps
	keyValueTypes map[string]graphql.Output
}

var inputQueryOptions = graphql.NewInputObject(graphql.InputObjectConfig{
	Name: "QueryOptions",
	Fields: graphql.InputObjectConfigFieldMap{
		"limit":       {Type: graphql.Int},
		"pageSize":    {Type: graphql.Int},
		"pageState":   {Type: graphql.String},
		"consistency": {Type: consistencyEnum, DefaultValue: gocql.LocalQuorum},
	},
})

var inputMutationOptions = graphql.NewInputObject(graphql.InputObjectConfig{
	Name: "UpdateOptions",
	Fields: graphql.InputObjectConfigFieldMap{
		"ttl":         {Type: graphql.Int, DefaultValue: -1},
		"consistency": {Type: consistencyEnum, DefaultValue: gocql.LocalQuorum},
	},
})

var inputQueryOptionsDefault = types.QueryOptions{Consistency: int(gocql.LocalQuorum)}
var inputMutationOptionsDefault = types.MutationOptions{
	TTL:         -1,
	Consistency: int(gocql.LocalQuorum),
}

var consistencyEnum = graphql.NewEnum(graphql.EnumConfig{
	Name: "Consistency",
	Values: graphql.EnumValueConfigMap{
		"LOCAL_ONE":    {Value: gocql.LocalOne},
		"LOCAL_QUORUM": {Value: gocql.LocalQuorum},
		"ALL":          {Value: gocql.All},
		"SERIAL":       {Value: gocql.Serial},
		"LOCAL_SERIAL": {Value: gocql.LocalSerial},
	},
})

func (s *KeyspaceGraphQLSchema) buildType(typeInfo gocql.TypeInfo, isInput bool) (graphql.Output, error) {
	switch typeInfo.Type() {
	case gocql.TypeInt, gocql.TypeTinyInt, gocql.TypeSmallInt:
		return graphql.Int, nil
	case gocql.TypeFloat, gocql.TypeDouble:
		return graphql.Float, nil
	case gocql.TypeText, gocql.TypeVarchar:
		return graphql.String, nil
	case gocql.TypeBigInt:
		return bigint, nil
	case gocql.TypeDecimal:
		return decimal, nil
	case gocql.TypeVarint:
		return varint, nil
	case gocql.TypeBoolean:
		return graphql.Boolean, nil
	case gocql.TypeUUID:
		return uuid, nil
	case gocql.TypeTimeUUID:
		return timeuuid, nil
	case gocql.TypeTimestamp:
		return timestamp, nil
	case gocql.TypeInet:
		return ip, nil
	case gocql.TypeBlob:
		return blob, nil
	case gocql.TypeTime:
		return localTime, nil
	case gocql.TypeList, gocql.TypeSet:
		elem, err := s.buildType(typeInfo.(gocql.CollectionType).Elem, isInput)
		if err != nil {
			return nil, err
		}
		return graphql.NewList(elem), nil
	case gocql.TypeMap:
		key, err := s.buildType(typeInfo.(gocql.CollectionType).Key, isInput)
		if err != nil {
			return nil, err
		}
		value, err := s.buildType(typeInfo.(gocql.CollectionType).Elem, isInput)
		if err != nil {
			return nil, err
		}
		kvType := s.buildKeyValueType(key, value, isInput)
		if kvType == nil {
			return nil, fmt.Errorf("Type for %s could not be created", typeInfo.Type().String())
		}
		return graphql.NewList(kvType), nil
	default:
		return nil, fmt.Errorf("Unsupported type %s", typeInfo.Type().String())
	}
}

func (s *KeyspaceGraphQLSchema) buildKeyValueType(key graphql.Output, value graphql.Output, isInput bool) graphql.Output {
	keyName := getTypeName(key)
	valueName := getTypeName(value)
	if keyName == "" || valueName == "" {
		return nil
	}

	typeName := fmt.Sprintf("Key%sValue%s", keyName, valueName)

	if isInput {
		typeName = "Input" + typeName
	}

	t := s.keyValueTypes[typeName]

	if t == nil {
		if isInput {
			t = graphql.NewInputObject(graphql.InputObjectConfig{
				Name: typeName,
				Fields: graphql.InputObjectConfigFieldMap{
					"key": &graphql.InputObjectFieldConfig{
						Type: graphql.NewNonNull(key),
					},
					"value": &graphql.InputObjectFieldConfig{
						Type: value,
					},
				},
			})
		} else {
			t = graphql.NewObject(graphql.ObjectConfig{
				Name: typeName,
				Fields: graphql.Fields{
					"key": &graphql.Field{
						Type: graphql.NewNonNull(key),
					},
					"value": &graphql.Field{
						Type: value,
					},
				},
			})
		}

		s.keyValueTypes[typeName] = t
	}

	return t
}

func getTypeName(t graphql.Output) string {
	switch specType := t.(type) {
	case *graphql.Scalar:
		return t.Name()
	case *graphql.Object, *graphql.InputObject:
		// Its a map key value: use the existing composite name
		return t.Name()
	case *graphql.List:
		elemName := getTypeName(specType.OfType)
		if elemName == "" {
			return ""
		}
		return fmt.Sprintf("List%s", elemName)
	}

	return ""
}

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
	s.keyValueTypes = make(map[string]graphql.Output)
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
			var inputFieldType graphql.Output
			fieldName := naming.ToGraphQLField(name)
			fieldType, err = s.buildType(column.Type, false)
			if err != nil {
				log.Println(err)
				break
			}
			inputFieldType, err = s.buildType(column.Type, true)
			if err != nil {
				log.Println(err)
				break
			}

			fields[fieldName] = &graphql.Field{Type: fieldType}
			inputFields[fieldName] = &graphql.InputObjectFieldConfig{Type: inputFieldType}

			t := operatorsInputTypes[column.Type.Type()]
			if t != nil {
				// Only allow filtering for types that are supported (i.e. lists are not included)
				inputOperatorFields[fieldName] = &graphql.InputObjectFieldConfig{
					Type: t,
				}
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
