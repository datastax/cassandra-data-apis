package schema

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/iancoleman/strcase"
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
	kindPrimary
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
		"PRIMARY": &graphql.EnumValueConfig{
			Value: kindPrimary,
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

func getTable(keyspace *gocql.KeyspaceMetadata, args map[string]interface{}) (interface{}, error) {
	name := args["name"].(string)
	table := keyspace.Tables[strcase.ToSnake(name)]
	if table == nil {
		return nil, fmt.Errorf("unable to find table '%s'", name)
	}
	return &tableValue{
		Name:    strcase.ToCamel(name),
		Columns: toColumnValues(table.Columns),
	}, nil
}

func getTables(keyspace *gocql.KeyspaceMetadata) (interface{}, error) {
	tableValues := make([]*tableValue, 0)
	for _, table := range keyspace.Tables {
		tableValues = append(tableValues, &tableValue{
			Name:    strcase.ToCamel(table.Name),
			Columns: toColumnValues(table.Columns),
		})
	}
	return tableValues, nil
}

func decodeColumns(columns []interface{}) []*columnValue {
	columnValues := make([]*columnValue, 0)
	for _, column := range columns {
		var value columnValue
		mapstructure.Decode(column, &value)
		columnValues = append(columnValues, &value)
	}
	return columnValues
}

func createTable(db *db.Db, ksName string, args map[string]interface{}) (interface{}, error) {
	//name := args["name"].(string)
	//primaryKey := decodeColumns(args["primaryKey"].([]interface{}))
	//clusteringKey := decodeColumns(args["clusteringKey"].([]interface{}))
	//values := decodeColumns(args["values"].([]interface{}))

	return nil, nil
}

func dropTable(db *db.Db, ksName string, args map[string]interface{}) (interface{}, error) {
	return db.DropTable(ksName, strcase.ToSnake(args["name"].(string)))
}

func toColumnKind(kind gocql.ColumnKind) int {
	switch kind {
	case gocql.ColumnPartitionKey:
		return kindPrimary
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

func toColumnValues(columns map[string]*gocql.ColumnMetadata) []*columnValue {
	columnValues := make([]*columnValue, 0)
	for _, column := range columns {
		columnValues = append(columnValues, &columnValue{
			Name: strcase.ToLowerCamel(column.Name),
			Kind: toColumnKind(column.Kind),
			Type: toColumnType(column.Type),
		})
	}
	return columnValues
}
