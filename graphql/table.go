package graphql

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/mitchellh/mapstructure"
	"github.com/riptano/data-endpoints/db"
)

const (
	typeInt = iota
	typeVarchar
	typeText
	typeUUID
	// ...
)

const (
	kindUnknown = iota
	kindPartition
	kindClustering
	kindRegular
	kindStatic
	kindCompact
)

type dataTypeValue struct {
	Basic    int              `json:"basic"`
	SubTypes []*dataTypeValue `json:"subTypes"`
}

type columnValue struct {
	Name string         `json:"name"`
	Kind int            `json:"kind"`
	Type *dataTypeValue `json:"type"`
}

type clusteringInfo struct {
	// mapstructure.Decode() calls don't work when embedding values
	//columnValue  //embedded
	Name  string         `json:"name"`
	Kind  int            `json:"kind"`
	Type  *dataTypeValue `json:"type"`
	Order string         `json:"order"`
}

type tableValue struct {
	Name    string         `json:"name"`
	Columns []*columnValue `json:"columns"`
}

var basicTypeEnum = graphql.NewEnum(graphql.EnumConfig{
	Name: "BasicType",
	Values: graphql.EnumValueConfigMap{
		"INT": &graphql.EnumValueConfig{
			Value: typeInt,
		},
		"VARCHAR": &graphql.EnumValueConfig{
			Value: typeVarchar,
		},
		"TEXT": &graphql.EnumValueConfig{
			Value: typeText,
		},
		"UUID": &graphql.EnumValueConfig{
			Value: typeUUID,
		},
		// ...
	},
})

var dataType = buildDataType()

func buildDataType() *graphql.Object {
	dataType := graphql.NewObject(graphql.ObjectConfig{
		Name: "DataType",
		Fields: graphql.Fields{
			"basic": &graphql.Field{
				Type: graphql.NewNonNull(basicTypeEnum),
			},
		},
	})
	dataType.AddFieldConfig("subTypes", &graphql.Field{
		Type: graphql.NewList(dataType),
	})
	return dataType
}

var columnKindEnum = graphql.NewEnum(graphql.EnumConfig{
	Name: "ColumnKind",
	Values: graphql.EnumValueConfigMap{
		"UNKNOWN": &graphql.EnumValueConfig{
			Value: kindUnknown,
		},
		"PARTITION": &graphql.EnumValueConfig{
			Value: kindPartition,
		},
		"CLUSTERING": &graphql.EnumValueConfig{
			Value: kindClustering,
		},
		"REGULAR": &graphql.EnumValueConfig{
			Value: kindRegular,
		},
		"STATIC": &graphql.EnumValueConfig{
			Value: kindStatic,
		},
		"COMPACT": &graphql.EnumValueConfig{
			Value: kindCompact,
		},
	},
})

var columnType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Column",
	Fields: graphql.Fields{
		"name": &graphql.Field{
			Type: graphql.NewNonNull(graphql.String),
		},
		"kind": &graphql.Field{
			Type: graphql.NewNonNull(columnKindEnum),
		},
		"type": &graphql.Field{
			Type: graphql.NewNonNull(dataType),
		},
	},
})

var dataTypeInput = buildDataTypeInput()

func buildDataTypeInput() *graphql.InputObject {
	dataType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "DataTypeInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"basic": &graphql.InputObjectFieldConfig{
				Type: graphql.NewNonNull(basicTypeEnum),
			},
		},
	})
	dataType.AddFieldConfig("subTypes", &graphql.InputObjectFieldConfig{
		Type: graphql.NewList(dataType),
	})
	return dataType
}

var columnInput = graphql.NewInputObject(graphql.InputObjectConfig{
	Name: "ColumnInput",
	Fields: graphql.InputObjectConfigFieldMap{
		"name": &graphql.InputObjectFieldConfig{
			Type: graphql.NewNonNull(graphql.String),
		},
		"type": &graphql.InputObjectFieldConfig{
			Type: graphql.NewNonNull(dataTypeInput),
		},
	},
})

var clusteringKeyInput = graphql.NewInputObject(graphql.InputObjectConfig{
	Name: "ClusteringKeyInput",
	Fields: graphql.InputObjectConfigFieldMap{
		"name": &graphql.InputObjectFieldConfig{
			Type: graphql.NewNonNull(graphql.String),
		},
		"type": &graphql.InputObjectFieldConfig{
			Type: graphql.NewNonNull(dataTypeInput),
		},
		"order": &graphql.InputObjectFieldConfig{
			Type: graphql.String,
		},
	},
})

var tableType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Table",
	Fields: graphql.Fields{
		"name": &graphql.Field{
			Type: graphql.NewNonNull(graphql.String),
		},
		"columns": &graphql.Field{
			Type: graphql.NewList(columnType),
		},
	},
})

func (s *KeyspaceGraphQLSchema) getTable(keyspace *gocql.KeyspaceMetadata, args map[string]interface{}) (interface{}, error) {
	name := args["name"].(string)
	table := keyspace.Tables[s.naming.ToCQLTable(name)]
	if table == nil {
		return nil, fmt.Errorf("unable to find table '%s'", name)
	}
	return &tableValue{
		Name:    s.naming.ToGraphQLType(name),
		Columns: s.toColumnValues(table.Name, table.Columns),
	}, nil
}

func (s *KeyspaceGraphQLSchema) getTables(keyspace *gocql.KeyspaceMetadata) (interface{}, error) {
	tableValues := make([]*tableValue, 0)
	for _, table := range keyspace.Tables {
		tableValues = append(tableValues, &tableValue{
			Name:    s.naming.ToGraphQLType(table.Name),
			Columns: s.toColumnValues(table.Name, table.Columns),
		})
	}
	return tableValues, nil
}

func (s *KeyspaceGraphQLSchema) decodeColumns(tableName string, columns []interface{}) ([]*gocql.ColumnMetadata, error) {
	columnValues := make([]*gocql.ColumnMetadata, 0)
	for _, column := range columns {
		var value columnValue
		if err := mapstructure.Decode(column, &value); err != nil {
			return nil, err
		}

		// Adapt from GraphQL column to gocql column
		cqlColumn := &gocql.ColumnMetadata{
			Name: s.naming.ToCQLColumn(tableName, value.Name),
			Kind: toDbColumnKind(value.Kind),
			Type: toDbColumnType(value.Type),
		}

		columnValues = append(columnValues, cqlColumn)
	}
	return columnValues, nil
}

func (s *KeyspaceGraphQLSchema) decodeClusteringInfo(tableName string, columns []interface{}) ([]*gocql.ColumnMetadata, error) {
	columnValues := make([]*gocql.ColumnMetadata, 0)
	for _, column := range columns {
		var value clusteringInfo
		if err := mapstructure.Decode(column, &value); err != nil {
			return nil, err
		}

		// Adapt from GraphQL column to gocql column
		cqlColumn := &gocql.ColumnMetadata{
			Name: s.naming.ToCQLColumn(tableName, value.Name),
			Kind: toDbColumnKind(value.Kind),
			Type: toDbColumnType(value.Type),
			//TODO: Use enums
			ClusteringOrder: value.Order,
		}

		columnValues = append(columnValues, cqlColumn)
	}
	return columnValues, nil
}

func (sg *SchemaGenerator) createTable(
	ksName string, ksSchema *KeyspaceGraphQLSchema, params graphql.ResolveParams) (interface{}, error) {
	var values []*gocql.ColumnMetadata = nil
	var clusteringKeys []*gocql.ColumnMetadata = nil
	args := params.Args
	tableName := ksSchema.naming.ToCQLTable(args["tableName"].(string))

	partitionKeys, err := ksSchema.decodeColumns(tableName, args["partitionKeys"].([]interface{}))

	if err != nil {
		return false, err
	}

	if args["values"] != nil {
		if values, err = ksSchema.decodeColumns(tableName, args["values"].([]interface{})); err != nil {
			return nil, err
		}
	}

	if args["clusteringKeys"] != nil {
		if clusteringKeys, err = ksSchema.decodeClusteringInfo(tableName, args["clusteringKeys"].([]interface{})); err != nil {
			return nil, err
		}
	}

	userOrRole, err := sg.checkUserOrRoleAuth(params)
	if err != nil {
		return nil, err
	}
	return sg.dbClient.CreateTable(&db.CreateTableInfo{
		Keyspace:       ksName,
		Table:          tableName,
		PartitionKeys:  partitionKeys,
		ClusteringKeys: clusteringKeys,
		Values:         values}, db.NewQueryOptions().WithUserOrRole(userOrRole))
}

func (sg *SchemaGenerator) alterTableAdd(ksName string, ksSchema *KeyspaceGraphQLSchema, params graphql.ResolveParams) (interface{}, error) {
	var err error
	var toAdd []*gocql.ColumnMetadata

	args := params.Args
	tableName := ksSchema.naming.ToCQLTable(args["tableName"].(string))

	if toAdd, err = ksSchema.decodeColumns(tableName, args["toAdd"].([]interface{})); err != nil {
		return nil, err
	}

	if len(toAdd) == 0 {
		return nil, fmt.Errorf("at least one column required")
	}

	userOrRole, err := sg.checkUserOrRoleAuth(params)
	if err != nil {
		return nil, err
	}
	return sg.dbClient.AlterTableAdd(&db.AlterTableAddInfo{
		Keyspace: ksName,
		Table:    tableName,
		ToAdd:    toAdd,
	}, db.NewQueryOptions().WithUserOrRole(userOrRole))
}

func (sg *SchemaGenerator) alterTableDrop(
	ksName string, ksSchema *KeyspaceGraphQLSchema, params graphql.ResolveParams) (interface{}, error) {
	args := params.Args
	tableName := ksSchema.naming.ToCQLTable(args["tableName"].(string))

	toDropArg := args["toDrop"].([]interface{})
	toDrop := make([]string, 0, len(toDropArg))

	for _, column := range toDropArg {
		toDrop = append(toDrop, ksSchema.naming.ToCQLColumn(tableName, column.(string)))
	}

	if len(toDrop) == 0 {
		return nil, fmt.Errorf("at least one column required")
	}

	userOrRole, err := sg.checkUserOrRoleAuth(params)
	if err != nil {
		return nil, err
	}
	return sg.dbClient.AlterTableDrop(&db.AlterTableDropInfo{
		Keyspace: ksName,
		Table:    tableName,
		ToDrop:   toDrop,
	}, db.NewQueryOptions().WithUserOrRole(userOrRole))
}

func (sg *SchemaGenerator) dropTable(ksName string, ksSchema *KeyspaceGraphQLSchema, params graphql.ResolveParams) (interface{}, error) {
	name := ksSchema.naming.ToCQLTable(params.Args["name"].(string))
	userOrRole, err := sg.checkUserOrRoleAuth(params)
	if err != nil {
		return nil, err
	}
	return sg.dbClient.DropTable(&db.DropTableInfo{
		Keyspace: ksName,
		Table:    name}, db.NewQueryOptions().WithUserOrRole(userOrRole))
}

func toColumnKind(kind gocql.ColumnKind) int {
	switch kind {
	case gocql.ColumnPartitionKey:
		return kindPartition
	case gocql.ColumnClusteringKey:
		return kindClustering
	case gocql.ColumnRegular:
		return kindRegular
	case gocql.ColumnStatic:
		return kindStatic
	case gocql.ColumnCompact:
		return kindCompact
	default:
		return kindUnknown
	}
}

func toDbColumnKind(kind int) gocql.ColumnKind {
	switch kind {
	case kindPartition:
		return gocql.ColumnPartitionKey
	case kindClustering:
		return gocql.ColumnClusteringKey
	case kindRegular:
		return gocql.ColumnRegular
	case kindStatic:
		return gocql.ColumnStatic
	case kindCompact:
		return gocql.ColumnCompact
	default:
		return kindUnknown
	}
}

func toColumnType(info gocql.TypeInfo) *dataTypeValue {
	switch info.Type() {
	case gocql.TypeInt:
		return &dataTypeValue{
			Basic:    typeInt,
			SubTypes: nil,
		}
	case gocql.TypeVarchar:
		return &dataTypeValue{
			Basic:    typeVarchar,
			SubTypes: nil,
		}
	case gocql.TypeText:
		return &dataTypeValue{
			Basic:    typeText,
			SubTypes: nil,
		}
	case gocql.TypeUUID:
		return &dataTypeValue{
			Basic:    typeUUID,
			SubTypes: nil,
		}
		// ...
	}
	return nil
}

func toDbColumnType(info *dataTypeValue) gocql.TypeInfo {
	switch info.Basic {
	case typeInt:
		return gocql.NewNativeType(0, gocql.TypeInt, "")
	case typeVarchar:
		return gocql.NewNativeType(0, gocql.TypeVarchar, "")
	case typeText:
		return gocql.NewNativeType(0, gocql.TypeText, "")
	case typeUUID:
		return gocql.NewNativeType(0, gocql.TypeUUID, "")
	}

	return nil
}

func (s *KeyspaceGraphQLSchema) toColumnValues(tableName string, columns map[string]*gocql.ColumnMetadata) []*columnValue {
	columnValues := make([]*columnValue, 0)
	for _, column := range columns {
		columnValues = append(columnValues, &columnValue{
			Name: s.naming.ToGraphQLField(tableName, column.Name),
			Kind: toColumnKind(column.Kind),
			Type: toColumnType(column.Type),
		})
	}
	return columnValues
}
