package graphql

import (
	"fmt"
	"github.com/datastax/cassandra-data-apis/db"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/mitchellh/mapstructure"
)

type dataTypeValue struct {
	Basic    gocql.Type    `json:"basic"`
	TypeInfo *dataTypeInfo `json:"info"`
}

type dataTypeInfo struct {
	Name     string          `json:"name"`
	SubTypes []dataTypeValue `json:"subTypes"`
}

type columnValue struct {
	Name string           `json:"name"`
	Kind gocql.ColumnKind `json:"kind"`
	Type *dataTypeValue   `json:"type"`
}

type clusteringInfo struct {
	// mapstructure.Decode() calls don't work when embedding values
	//columnValue  //embedded
	Name  string           `json:"name"`
	Kind  gocql.ColumnKind `json:"kind"`
	Type  *dataTypeValue   `json:"type"`
	Order string           `json:"order"`
}

type tableValue struct {
	Name    string         `json:"name"`
	Columns []*columnValue `json:"columns"`
}

var basicTypeEnum = graphql.NewEnum(graphql.EnumConfig{
	Name: "BasicType",
	Values: graphql.EnumValueConfigMap{
		"ASCII":     {Value: gocql.TypeAscii},
		"VARCHAR":   {Value: gocql.TypeVarchar},
		"TEXT":      {Value: gocql.TypeText},
		"BOOLEAN":   {Value: gocql.TypeBoolean},
		"FLOAT":     {Value: gocql.TypeFloat},
		"DOUBLE":    {Value: gocql.TypeDouble},
		"TINYINT":   {Value: gocql.TypeTinyInt},
		"SMALLINT":  {Value: gocql.TypeSmallInt},
		"INT":       {Value: gocql.TypeInt},
		"BIGINT":    {Value: gocql.TypeBigInt},
		"VARINT":    {Value: gocql.TypeVarint},
		"DECIMAL":   {Value: gocql.TypeDecimal},
		"COUNTER":   {Value: gocql.TypeCounter},
		"UUID":      {Value: gocql.TypeUUID},
		"TIMEUUID":  {Value: gocql.TypeTimeUUID},
		"TIME":      {Value: gocql.TypeTime},
		"DATE":      {Value: gocql.TypeDate},
		"DURATION":  {Value: gocql.TypeDuration},
		"TIMESTAMP": {Value: gocql.TypeTimestamp},
		"BLOB":      {Value: gocql.TypeBlob},
		"INET":      {Value: gocql.TypeInet},
		"LIST":      {Value: gocql.TypeList},
		"SET":       {Value: gocql.TypeSet},
		"MAP":       {Value: gocql.TypeMap},
		"TUPLE":     {Value: gocql.TypeTuple},
		"UDT":       {Value: gocql.TypeUDT},
		"CUSTOM":    {Value: gocql.TypeCustom},
	},
})

var dataType = buildDataType()

func buildDataType() *graphql.Object {
	dataType := graphql.NewObject(graphql.ObjectConfig{
		Name: "DataType",
		Fields: graphql.Fields{
			"basic": {Type: graphql.NewNonNull(basicTypeEnum)},
		},
	})

	info := graphql.NewObject(graphql.ObjectConfig{
		Name: "DataTypeInfo",
		Fields: graphql.Fields{
			"name":     {Type: graphql.String},
			"subTypes": {Type: graphql.NewList(dataType)},
		},
	})

	dataType.AddFieldConfig("info", &graphql.Field{
		Type: info,
	})
	return dataType
}

var columnKindEnum = graphql.NewEnum(graphql.EnumConfig{
	Name: "ColumnKind",
	Values: graphql.EnumValueConfigMap{
		"UNKNOWN":    {Value: gocql.ColumnUnkownKind},
		"PARTITION":  {Value: gocql.ColumnPartitionKey},
		"CLUSTERING": {Value: gocql.ColumnClusteringKey},
		"REGULAR":    {Value: gocql.ColumnRegular},
		"STATIC":     {Value: gocql.ColumnStatic},
		"COMPACT":    {Value: gocql.ColumnCompact},
	},
})

var columnType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Column",
	Fields: graphql.Fields{
		"name": {Type: graphql.NewNonNull(graphql.String)},
		"kind": {Type: graphql.NewNonNull(columnKindEnum)},
		"type": {Type: graphql.NewNonNull(dataType)},
	},
})

var dataTypeInput = buildDataTypeInput()

func buildDataTypeInput() *graphql.InputObject {
	dataType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "DataTypeInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"basic": {Type: graphql.NewNonNull(basicTypeEnum)},
		},
	})

	info := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "DataTypeInfoInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"name":     {Type: graphql.String},
			"subTypes": {Type: graphql.NewList(dataType)},
		},
	})

	dataType.AddFieldConfig("info", &graphql.InputObjectFieldConfig{
		Type: info,
	})
	return dataType
}

var columnInput = graphql.NewInputObject(graphql.InputObjectConfig{
	Name: "ColumnInput",
	Fields: graphql.InputObjectConfigFieldMap{
		"name": {Type: graphql.NewNonNull(graphql.String)},
		"type": {Type: graphql.NewNonNull(dataTypeInput)},
	},
})

var clusteringKeyInput = graphql.NewInputObject(graphql.InputObjectConfig{
	Name: "ClusteringKeyInput",
	Fields: graphql.InputObjectConfigFieldMap{
		"name":  {Type: graphql.NewNonNull(graphql.String)},
		"type":  {Type: graphql.NewNonNull(dataTypeInput)},
		"order": {Type: graphql.String},
	},
})

var tableType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Table",
	Fields: graphql.Fields{
		"name":    {Type: graphql.NewNonNull(graphql.String)},
		"columns": {Type: graphql.NewList(columnType)},
	},
})

func getTables(keyspace *gocql.KeyspaceMetadata, args map[string]interface{}) (interface{}, error) {
	if args["name"] != nil {
		// Filter by name
		name := args["name"].(string)
		table := keyspace.Tables[name]
		if table == nil {
			return nil, fmt.Errorf("unable to find table '%s'", name)
		}
		return []*tableValue{
			{
				Name:    table.Name,
				Columns: toColumnValues(table.Columns),
			},
		}, nil
	}

	tableValues := make([]*tableValue, 0)
	for _, table := range keyspace.Tables {
		tableValues = append(tableValues, &tableValue{
			Name:    table.Name,
			Columns: toColumnValues(table.Columns),
		})
	}
	return tableValues, nil
}

func decodeColumns(columns []interface{}) ([]*gocql.ColumnMetadata, error) {
	columnValues := make([]*gocql.ColumnMetadata, 0)
	for _, column := range columns {
		var value columnValue
		if err := mapstructure.Decode(column, &value); err != nil {
			return nil, err
		}

		// Adapt from GraphQL column to gocql column
		cqlColumn := &gocql.ColumnMetadata{
			Name: value.Name,
			Kind: value.Kind,
			Type: toDbColumnType(value.Type),
		}

		columnValues = append(columnValues, cqlColumn)
	}
	return columnValues, nil
}

func decodeClusteringInfo(columns []interface{}) ([]*gocql.ColumnMetadata, error) {
	columnValues := make([]*gocql.ColumnMetadata, 0)
	for _, column := range columns {
		var value clusteringInfo
		if err := mapstructure.Decode(column, &value); err != nil {
			return nil, err
		}

		// Adapt from GraphQL column to gocql column
		cqlColumn := &gocql.ColumnMetadata{
			Name: value.Name,
			Kind: value.Kind,
			Type: toDbColumnType(value.Type),
			//TODO: Use enums
			ClusteringOrder: value.Order,
		}

		columnValues = append(columnValues, cqlColumn)
	}
	return columnValues, nil
}

func (sg *SchemaGenerator) createTable(params graphql.ResolveParams) (interface{}, error) {
	var values []*gocql.ColumnMetadata = nil
	var clusteringKeys []*gocql.ColumnMetadata = nil
	args := params.Args
	ksName := args["keyspaceName"].(string)
	tableName := args["tableName"].(string)

	partitionKeys, err := decodeColumns(args["partitionKeys"].([]interface{}))

	if err != nil {
		return false, err
	}

	if args["values"] != nil {
		if values, err = decodeColumns(args["values"].([]interface{})); err != nil {
			return nil, err
		}
	}

	if args["clusteringKeys"] != nil {
		if clusteringKeys, err = decodeClusteringInfo(args["clusteringKeys"].([]interface{})); err != nil {
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

func (sg *SchemaGenerator) alterTableAdd(params graphql.ResolveParams) (interface{}, error) {
	var err error
	var toAdd []*gocql.ColumnMetadata

	args := params.Args
	ksName := args["keyspaceName"].(string)
	tableName := args["tableName"].(string)

	if toAdd, err = decodeColumns(args["toAdd"].([]interface{})); err != nil {
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

func (sg *SchemaGenerator) alterTableDrop(params graphql.ResolveParams) (interface{}, error) {
	args := params.Args
	ksName := args["keyspaceName"].(string)
	tableName := args["tableName"].(string)

	toDropArg := args["toDrop"].([]interface{})
	toDrop := make([]string, 0, len(toDropArg))

	for _, column := range toDropArg {
		toDrop = append(toDrop, column.(string))
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

func (sg *SchemaGenerator) dropTable(params graphql.ResolveParams) (interface{}, error) {
	args := params.Args
	ksName := args["keyspaceName"].(string)
	tableName := args["tableName"].(string)
	userOrRole, err := sg.checkUserOrRoleAuth(params)
	if err != nil {
		return nil, err
	}
	return sg.dbClient.DropTable(&db.DropTableInfo{
		Keyspace: ksName,
		Table:    tableName}, db.NewQueryOptions().WithUserOrRole(userOrRole))
}

func toColumnType(info gocql.TypeInfo) *dataTypeValue {
	var subTypeInfo *dataTypeInfo = nil
	switch info.Type() {
	case gocql.TypeList, gocql.TypeSet:
		collectionInfo := info.(gocql.CollectionType)
		subTypeInfo = &dataTypeInfo{
			SubTypes: []dataTypeValue{*toColumnType(collectionInfo.Elem)},
		}
	case gocql.TypeMap:
		collectionInfo := info.(gocql.CollectionType)
		subTypeInfo = &dataTypeInfo{
			SubTypes: []dataTypeValue{*toColumnType(collectionInfo.Key), *toColumnType(collectionInfo.Elem)},
		}
	case gocql.TypeCustom:
		subTypeInfo = &dataTypeInfo{
			Name: info.Custom(),
		}
	case gocql.TypeUDT, gocql.TypeTuple:
		panic("Not yet supported")
	}

	return &dataTypeValue{
		Basic:    info.Type(),
		TypeInfo: subTypeInfo,
	}
}

func toDbColumnType(info *dataTypeValue) gocql.TypeInfo {
	switch info.Basic {
	case gocql.TypeList, gocql.TypeSet:
		return gocql.CollectionType{
			NativeType: gocql.NewNativeType(0, info.Basic, ""),
			Key:        nil,
			Elem:       toDbColumnType(&info.TypeInfo.SubTypes[0]),
		}
	case gocql.TypeMap:
		return gocql.CollectionType{
			NativeType: gocql.NewNativeType(0, info.Basic, ""),
			Key:        toDbColumnType(&info.TypeInfo.SubTypes[0]),
			Elem:       toDbColumnType(&info.TypeInfo.SubTypes[1]),
		}
	case gocql.TypeCustom:
		return gocql.NewNativeType(0, info.Basic, info.TypeInfo.Name)
	case gocql.TypeUDT, gocql.TypeTuple:
		panic("Not yet supported")
	default:
		return gocql.NewNativeType(0, info.Basic, "")
	}
}

func toColumnValues(columns map[string]*gocql.ColumnMetadata) []*columnValue {
	columnValues := make([]*columnValue, 0)
	for _, column := range columns {
		columnValues = append(columnValues, &columnValue{
			Name: column.Name,
			Kind: column.Kind,
			Type: toColumnType(column.Type),
		})
	}
	return columnValues
}
