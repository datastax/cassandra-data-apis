package graphql

import (
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/riptano/data-endpoints/config"
	"github.com/riptano/data-endpoints/log"
	"github.com/riptano/data-endpoints/types"
	"gopkg.in/inf.v0"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/riptano/data-endpoints/db"
)

const (
	insertPrefix = "insert"
	deletePrefix = "delete"
	updatePrefix = "update"
)

const AuthUserOrRole = "userOrRole"

type SchemaGenerator struct {
	dbClient          *db.Db
	namingFn          config.NamingConventionFn
	supportedOps      config.Operations
	useUserOrRoleAuth bool
	logger            log.Logger
}

var appliedModificationResult = types.ModificationResult{Applied: true}

func NewSchemaGenerator(dbClient *db.Db, cfg config.Config) *SchemaGenerator {
	return &SchemaGenerator{
		dbClient:          dbClient,
		namingFn:          cfg.Naming(),
		supportedOps:      cfg.SupportedOperations(),
		useUserOrRoleAuth: cfg.UseUserOrRoleAuth(),
		logger:            cfg.Logger(),
	}
}

func (sg *SchemaGenerator) buildQueriesFields(ksSchema *KeyspaceGraphQLSchema, tables map[string]*gocql.TableMetadata, resolve graphql.FieldResolveFn) graphql.Fields {
	fields := graphql.Fields{}
	for name, table := range tables {
		if ksSchema.ignoredTables[table.Name] {
			continue
		}

		fields[ksSchema.naming.ToGraphQLOperation("", name)] = &graphql.Field{
			Type: ksSchema.resultSelectTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"data":    {Type: graphql.NewNonNull(ksSchema.tableScalarInputTypes[table.Name])},
				"orderBy": {Type: graphql.NewList(ksSchema.orderEnums[table.Name])},
				"options": {Type: inputQueryOptions, DefaultValue: inputQueryOptionsDefault},
			},
			Resolve: resolve,
		}

		fields[ksSchema.naming.ToGraphQLOperation("", name)+"Filter"] = &graphql.Field{
			Type: ksSchema.resultSelectTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"filter":  {Type: graphql.NewNonNull(ksSchema.tableOperatorInputTypes[table.Name])},
				"orderBy": {Type: graphql.NewList(ksSchema.orderEnums[table.Name])},
				"options": {Type: inputQueryOptions, DefaultValue: inputQueryOptionsDefault},
			},
			Resolve: resolve,
		}
	}
	fields["table"] = &graphql.Field{
		Type: tableType,
		Args: graphql.FieldConfigArgument{
			"name": &graphql.ArgumentConfig{
				Type: graphql.NewNonNull(graphql.String),
			},
		},
		Resolve: resolve,
	}
	fields["tables"] = &graphql.Field{
		Type:    graphql.NewList(tableType),
		Resolve: resolve,
	}
	return fields
}

func (sg *SchemaGenerator) buildQuery(schema *KeyspaceGraphQLSchema, tables map[string]*gocql.TableMetadata, resolve graphql.FieldResolveFn) *graphql.Object {
	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "TableQuery",
			Fields: sg.buildQueriesFields(schema, tables, resolve),
		})
}

func (sg *SchemaGenerator) buildMutationFields(ksSchema *KeyspaceGraphQLSchema, tables map[string]*gocql.TableMetadata, resolve graphql.FieldResolveFn) graphql.Fields {
	fields := graphql.Fields{}
	for name, table := range tables {
		if ksSchema.ignoredTables[table.Name] {
			continue
		}
		fields[ksSchema.naming.ToGraphQLOperation(insertPrefix, name)] = &graphql.Field{
			Type: ksSchema.resultUpdateTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"data":        {Type: graphql.NewNonNull(ksSchema.tableScalarInputTypes[table.Name])},
				"ifNotExists": {Type: graphql.Boolean},
				"options":     {Type: inputMutationOptions, DefaultValue: inputMutationOptionsDefault},
			},
			Resolve: resolve,
		}

		fields[ksSchema.naming.ToGraphQLOperation(deletePrefix, name)] = &graphql.Field{
			Type: ksSchema.resultUpdateTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"data":        {Type: graphql.NewNonNull(ksSchema.tableScalarInputTypes[table.Name])},
				"ifExists":    {Type: graphql.Boolean},
				"ifCondition": {Type: ksSchema.tableOperatorInputTypes[table.Name]},
				"options":     {Type: inputMutationOptions, DefaultValue: inputMutationOptionsDefault},
			},
			Resolve: resolve,
		}

		fields[ksSchema.naming.ToGraphQLOperation(updatePrefix, name)] = &graphql.Field{
			Type: ksSchema.resultUpdateTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"data":        {Type: graphql.NewNonNull(ksSchema.tableScalarInputTypes[table.Name])},
				"ifExists":    {Type: graphql.Boolean},
				"ifCondition": {Type: ksSchema.tableOperatorInputTypes[table.Name]},
				"options":     {Type: inputMutationOptions, DefaultValue: inputMutationOptionsDefault},
			},
			Resolve: resolve,
		}
	}
	if sg.supportedOps.IsSupported(config.TableCreate) {
		fields["createTable"] = &graphql.Field{
			Type: graphql.Boolean,
			Args: graphql.FieldConfigArgument{
				"name": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.String),
				},
				"partitionKeys": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.NewList(columnInput)),
				},
				"clusteringKeys": &graphql.ArgumentConfig{
					Type: graphql.NewList(clusteringKeyInput),
				},
				"values": &graphql.ArgumentConfig{
					Type: graphql.NewList(columnInput),
				},
			},
			Resolve: resolve,
		}
	}
	if sg.supportedOps.IsSupported(config.TableAlterAdd) {
		fields["alterTableAdd"] = &graphql.Field{
			Type: graphql.Boolean,
			Args: graphql.FieldConfigArgument{
				"name": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.String),
				},
				"toAdd": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.NewList(columnInput)),
				},
			},
			Resolve: resolve,
		}
	}
	if sg.supportedOps.IsSupported(config.TableAlterDrop) {
		fields["alterTableDrop"] = &graphql.Field{
			Type: graphql.Boolean,
			Args: graphql.FieldConfigArgument{
				"name": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.String),
				},
				"toDrop": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.NewList(graphql.String)),
				},
			},
			Resolve: resolve,
		}
	}
	if sg.supportedOps.IsSupported(config.TableDrop) {
		fields["dropTable"] = &graphql.Field{
			Type: graphql.Boolean,
			Args: graphql.FieldConfigArgument{
				"name": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.String),
				},
			},
			Resolve: resolve,
		}
	}
	return fields
}

func (sg *SchemaGenerator) buildMutation(schema *KeyspaceGraphQLSchema, tables map[string]*gocql.TableMetadata, resolveFn graphql.FieldResolveFn) *graphql.Object {
	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "TableMutation",
			Fields: sg.buildMutationFields(schema, tables, resolveFn),
		})
}

// Build GraphQL schema for tables in the provided keyspace metadata
func (sg *SchemaGenerator) BuildSchema(keyspaceName string) (graphql.Schema, error) {
	keyspace, err := sg.dbClient.Keyspace(keyspaceName)
	if err != nil {
		return graphql.Schema{}, err
	}

	sg.logger.Info("building schema", "keyspace", keyspace.Name)

	ksNaming := sg.dbClient.KeyspaceNamingInfo(keyspace)
	keyspaceSchema := &KeyspaceGraphQLSchema{
		ignoredTables: make(map[string]bool),
		schemaGen:     sg,
		naming:        sg.namingFn(ksNaming),
	}

	if err := keyspaceSchema.BuildTypes(keyspace); err != nil {
		return graphql.Schema{}, err
	}

	queryResolveFn := sg.queryFieldResolver(keyspace, keyspaceSchema)
	mutationResolveFn := sg.mutationFieldResolver(keyspace, keyspaceSchema)

	return graphql.NewSchema(
		graphql.SchemaConfig{
			Query:    sg.buildQuery(keyspaceSchema, keyspace.Tables, queryResolveFn),
			Mutation: sg.buildMutation(keyspaceSchema, keyspace.Tables, mutationResolveFn),
		},
	)
}

func (sg *SchemaGenerator) queryFieldResolver(
	keyspace *gocql.KeyspaceMetadata, ksSchema *KeyspaceGraphQLSchema) graphql.FieldResolveFn {
	return func(params graphql.ResolveParams) (interface{}, error) {
		fieldName := params.Info.FieldName
		switch fieldName {
		case "table":
			return ksSchema.getTable(keyspace, params.Args)
		case "tables":
			return ksSchema.getTables(keyspace)
		default:
			var table *gocql.TableMetadata
			table, tableFound := keyspace.Tables[ksSchema.naming.ToCQLTable(fieldName)]
			var data map[string]interface{}
			if params.Args["data"] != nil {
				data = params.Args["data"].(map[string]interface{})
			} else {
				data = params.Args["filter"].(map[string]interface{})
			}

			var whereClause []types.ConditionItem

			if tableFound {
				whereClause = make([]types.ConditionItem, 0, len(data))
				for key, value := range data {
					whereClause = append(whereClause, types.ConditionItem{
						Column:   ksSchema.naming.ToCQLColumn(key),
						Operator: "=",
						Value:    adaptParameterValue(value),
					})
				}
			} else if strings.HasSuffix(fieldName, "Filter") {
				table, tableFound = keyspace.Tables[ksSchema.naming.ToCQLTable(strings.TrimSuffix(fieldName, "Filter"))]
				if !tableFound {
					return nil, fmt.Errorf("unable to find table '%s'", params.Info.FieldName)
				}

				whereClause = ksSchema.adaptCondition(data)
			} else {
				return nil, fmt.Errorf("Unable to find table for '%s'", params.Info.FieldName)
			}

			var orderBy []interface{}
			var options types.QueryOptions
			if err := mapstructure.Decode(params.Args["options"], &options); err != nil {
				return nil, err
			}

			if params.Args["orderBy"] != nil {
				orderBy = params.Args["orderBy"].([]interface{})
			}

			userOrRole, err := sg.checkUserOrRoleAuth(params)
			if err != nil {
				return nil, err
			}

			result, err := sg.dbClient.Select(
				&db.SelectInfo{
					Keyspace: keyspace.Name,
					Table:    table.Name,
					Where:    whereClause,
					OrderBy:  parseColumnOrder(orderBy),
					Options:  &options,
				},
				db.NewQueryOptions().
					WithUserOrRole(userOrRole).
					WithConsistency(gocql.Consistency(options.Consistency)).
					WithSerialConsistency(gocql.SerialConsistency(options.SerialConsistency)))

			if err != nil {
				return nil, err
			}

			return &types.QueryResult{
				PageState: result.PageState(),
				Values:    ksSchema.adaptResult(result.Values()),
			}, nil
		}
	}
}

func (s *KeyspaceGraphQLSchema) adaptCondition(data map[string]interface{}) []types.ConditionItem {
	result := make([]types.ConditionItem, 0, len(data))
	for key, value := range data {
		if value == nil {
			continue
		}
		mapValue := value.(map[string]interface{})

		for operatorName, itemValue := range mapValue {
			result = append(result, types.ConditionItem{
				Column:   s.naming.ToCQLColumn(key),
				Operator: cqlOperators[operatorName],
				Value:    adaptParameterValue(itemValue),
			})
		}
	}
	return result
}

func (s *KeyspaceGraphQLSchema) adaptResult(values []map[string]interface{}) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(values))
	for _, item := range values {
		resultItem := make(map[string]interface{})
		for k, v := range item {
			resultItem[s.naming.ToGraphQLField(k)] = adaptResultValue(v)
		}
		result = append(result, resultItem)
	}

	return result
}

func adaptResultValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch value.(type) {
	case *int8, *int16, *int, *float32, *float64, *int32, *string, *bool,
		*time.Time, *inf.Dec, *big.Int, *gocql.UUID, *[]byte:
		// Avoid reflection whenever possible
		return value
	}

	rv := reflect.ValueOf(value)
	typeKind := rv.Type().Kind()

	if typeKind == reflect.Ptr && rv.IsNil() {
		return nil
	}

	if !(typeKind == reflect.Ptr && rv.Elem().Type().Kind() == reflect.Map) {
		return value
	}

	rv = rv.Elem()

	// Maps should be adapted to a slice of maps, each map containing 2 keys: 'key' and 'value'
	result := make([]map[string]interface{}, 0, rv.Len())
	iter := rv.MapRange()
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		result = append(result, map[string]interface{}{
			"key":   key.Interface(),
			"value": value.Interface(),
		})
	}

	return result
}

func (sg *SchemaGenerator) mutationFieldResolver(
	keyspace *gocql.KeyspaceMetadata, ksSchema *KeyspaceGraphQLSchema) graphql.FieldResolveFn {
	return func(params graphql.ResolveParams) (interface{}, error) {
		fieldName := params.Info.FieldName
		switch fieldName {
		case "createTable":
			return sg.createTable(keyspace.Name, ksSchema, params)
		case "alterTableAdd":
			return sg.alterTableAdd(keyspace.Name, ksSchema, params)
		case "alterTableDrop":
			return sg.alterTableDrop(keyspace.Name, ksSchema, params)
		case "dropTable":
			return sg.dropTable(keyspace.Name, ksSchema, params)
		default:
			operation, typeName := mutationPrefix(fieldName)
			if table, ok := keyspace.Tables[ksSchema.naming.ToCQLTable(typeName)]; ok {
				data := params.Args["data"].(map[string]interface{})
				columnNames := make([]string, 0, len(data))
				queryParams := make([]interface{}, 0, len(data))

				for key, value := range data {
					columnNames = append(columnNames, ksSchema.naming.ToCQLColumn(key))
					queryParams = append(queryParams, adaptParameterValue(value))
				}

				var options types.MutationOptions
				if err := mapstructure.Decode(params.Args["options"], &options); err != nil {
					return nil, err
				}

				userOrRole, err := sg.checkUserOrRoleAuth(params)
				if err != nil {
					return nil, err
				}

				queryOptions := db.NewQueryOptions().
					WithUserOrRole(userOrRole).
					WithConsistency(gocql.Consistency(options.Consistency)).
					WithSerialConsistency(gocql.SerialConsistency(options.SerialConsistency))

				var result db.ResultSet

				switch operation {
				case insertPrefix:
					ifNotExists := params.Args["ifNotExists"] == true
					result, err = sg.dbClient.Insert(&db.InsertInfo{
						Keyspace:    keyspace.Name,
						Table:       table.Name,
						Columns:     columnNames,
						QueryParams: queryParams,
						IfNotExists: ifNotExists,
						TTL:         options.TTL,
					}, queryOptions)
				case deletePrefix:
					var ifCondition []types.ConditionItem
					if params.Args["ifCondition"] != nil {
						ifCondition = ksSchema.adaptCondition(params.Args["ifCondition"].(map[string]interface{}))
					}
					result, err = sg.dbClient.Delete(&db.DeleteInfo{
						Keyspace:    keyspace.Name,
						Table:       table.Name,
						Columns:     columnNames,
						QueryParams: queryParams,
						IfCondition: ifCondition,
						IfExists:    params.Args["ifExists"] == true}, queryOptions)
				case updatePrefix:
					var ifCondition []types.ConditionItem
					if params.Args["ifCondition"] != nil {
						ifCondition = ksSchema.adaptCondition(params.Args["ifCondition"].(map[string]interface{}))
					}
					result, err = sg.dbClient.Update(&db.UpdateInfo{
						Keyspace:    keyspace.Name,
						Table:       table,
						Columns:     columnNames,
						QueryParams: queryParams,
						IfCondition: ifCondition,
						TTL:         options.TTL,
						IfExists:    params.Args["ifExists"] == true}, queryOptions)
				default:
					return false, fmt.Errorf("operation '%s' not supported", operation)
				}

				return sg.getModificationResult(result, err)
			} else {
				return nil, fmt.Errorf("unable to find table for type name '%s'", params.Info.FieldName)
			}

		}
	}
}

func (sg *SchemaGenerator) getModificationResult(rs db.ResultSet, err error) (*types.ModificationResult, error) {
	if err != nil {
		return nil, err
	}

	rows := rs.Values()

	if len(rows) == 0 {
		return &appliedModificationResult, nil
	}

	result := types.ModificationResult{}
	row := rows[0]
	applied := row["[applied]"].(*bool)
	result.Applied = applied != nil && *applied

	result.Value = make(map[string]interface{})
	for k, v := range row {
		if k == "[applied]" {
			continue
		}
		result.Value[sg.naming.ToGraphQLField(k)] = adaptResultValue(v)
	}

	return &result, nil
}

func adaptParameterValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch value.(type) {
	case int8, int16, int, float32, float64, string, bool:
		// Avoid using reflection for common scalars
		// Ideally, the algorithm should function without this optimization
		return value
	}

	return adaptCollectionParameter(value)
}

func adaptCollectionParameter(value interface{}) interface{} {
	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Slice:
		// Type element (rv.Type().Elem()) is an interface{}
		// We have to inspect the first value
		length := rv.Len()
		if length == 0 {
			return value
		}
		firstElement := rv.Index(0)
		if reflect.TypeOf(firstElement.Interface()).Kind() != reflect.Map {
			return value
		}

		result := make(map[interface{}]interface{})
		// It's a slice of maps that only contains two keys: 'key' and 'value'
		// It's the graphql representation of a map: [KeyValueType]
		for i := 0; i < length; i++ {
			element := rv.Index(i).Interface().(map[string]interface{})
			result[element["key"]] = adaptParameterValue(element["value"])
		}

		return result
	}

	return value
}

func mutationPrefix(value string) (string, string) {
	mutationPrefixes := []string{insertPrefix, deletePrefix, updatePrefix}

	for _, prefix := range mutationPrefixes {
		if strings.Index(value, prefix) == 0 {
			return prefix, value[len(prefix):]
		}
	}

	panic("Unsupported mutation")
}

func parseColumnOrder(values []interface{}) []db.ColumnOrder {
	result := make([]db.ColumnOrder, 0, len(values))

	for _, value := range values {
		strValue := value.(string)
		index := strings.LastIndex(strValue, "_")
		result = append(result, db.ColumnOrder{
			Column: strValue[0:index],
			Order:  strValue[index+1:],
		})
	}

	return result
}

func (sg *SchemaGenerator) checkUserOrRoleAuth(params graphql.ResolveParams) (string, error) {
	value := params.Context.Value(AuthUserOrRole)
	if value == nil {
		if sg.useUserOrRoleAuth {
			return "", fmt.Errorf("expected user or role for this operation")
		} else {
			return "", nil
		}
	}
	return value.(string), nil
}
