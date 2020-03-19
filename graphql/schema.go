package graphql

import (
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/riptano/data-endpoints/types"
	"strings"

	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/iancoleman/strcase"
	"github.com/riptano/data-endpoints/db"
)

// TODO: could be done with enums
const insertPrefix = "insert"
const deletePrefix = "delete"
const updatePrefix = "update"

const AuthUserOrRole = "userOrRole"

func buildType(typeInfo gocql.TypeInfo) graphql.Output {
	switch typeInfo.Type() {
	case gocql.TypeInt, gocql.TypeTinyInt, gocql.TypeSmallInt:
		return graphql.Int
	case gocql.TypeFloat, gocql.TypeDouble:
		return graphql.Float
	case gocql.TypeText, gocql.TypeVarchar, gocql.TypeBigInt, gocql.TypeDecimal:
		return graphql.String
	case gocql.TypeBoolean:
		return graphql.Boolean
	case gocql.TypeUUID:
		return uuid
	case gocql.TypeTimeUUID:
		return graphql.String
	case gocql.TypeTimestamp:
		return timestamp
	default:
		panic("Unsupported type " + typeInfo.Type().String())
	}
}

func buildQueriesFields(schema *KeyspaceGraphQLSchema, tables map[string]*gocql.TableMetadata, resolve graphql.FieldResolveFn) graphql.Fields {
	fields := graphql.Fields{}
	for name, table := range tables {
		fields[strcase.ToLowerCamel(name)] = &graphql.Field{
			Type: schema.resultSelectTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"data":    {Type: graphql.NewNonNull(schema.tableScalarInputTypes[table.Name])},
				"orderBy": {Type: graphql.NewList(schema.orderEnums[table.Name])},
				"options": {Type: inputQueryOptions},
			},
			Resolve: resolve,
		}

		fields[strcase.ToLowerCamel(name)+"Filter"] = &graphql.Field{
			Type: schema.resultSelectTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"filter":  {Type: graphql.NewNonNull(schema.tableOperatorInputTypes[table.Name])},
				"orderBy": {Type: graphql.NewList(schema.orderEnums[table.Name])},
				"options": {Type: inputQueryOptions},
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

func buildQuery(schema *KeyspaceGraphQLSchema, tables map[string]*gocql.TableMetadata, resolve graphql.FieldResolveFn) *graphql.Object {
	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "TableQuery",
			Fields: buildQueriesFields(schema, tables, resolve),
		})
}

func buildMutationFields(schema *KeyspaceGraphQLSchema, tables map[string]*gocql.TableMetadata, resolve graphql.FieldResolveFn) graphql.Fields {
	fields := graphql.Fields{}
	for name, table := range tables {
		fields[insertPrefix+strcase.ToCamel(name)] = &graphql.Field{
			Type: schema.resultUpdateTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"data":        {Type: graphql.NewNonNull(schema.tableScalarInputTypes[table.Name])},
				"ifNotExists": {Type: graphql.Boolean},
				"options":     {Type: inputMutationOptions},
			},
			Resolve: resolve,
		}

		fields[deletePrefix+strcase.ToCamel(name)] = &graphql.Field{
			Type: schema.resultUpdateTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"data":        {Type: graphql.NewNonNull(schema.tableScalarInputTypes[table.Name])},
				"ifExists":    {Type: graphql.Boolean},
				"ifCondition": {Type: schema.tableOperatorInputTypes[table.Name]},
				"options":     {Type: inputMutationOptions},
			},
			Resolve: resolve,
		}
	}
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
	fields["dropTable"] = &graphql.Field{
		Type: graphql.Boolean,
		Args: graphql.FieldConfigArgument{
			"name": &graphql.ArgumentConfig{
				Type: graphql.NewNonNull(graphql.String),
			},
		},
		Resolve: resolve,
	}
	return fields
}

func buildMutation(schema *KeyspaceGraphQLSchema, tables map[string]*gocql.TableMetadata, resolveFn graphql.FieldResolveFn) *graphql.Object {
	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "TableMutation",
			Fields: buildMutationFields(schema, tables, resolveFn),
		})
}

// Build GraphQL schema for tables in the provided keyspace metadata
func BuildSchema(keyspaceName string, dbClient *db.Db) (graphql.Schema, error) {
	keyspace, err := dbClient.Keyspace(keyspaceName)
	if err != nil {
		return graphql.Schema{}, err
	}

	keyspaceSchema := &KeyspaceGraphQLSchema{}
	if err := keyspaceSchema.BuildTypes(keyspace); err != nil {
		return graphql.Schema{}, err
	}

	return graphql.NewSchema(
		graphql.SchemaConfig{
			Query:    buildQuery(keyspaceSchema, keyspace.Tables, queryFieldResolver(keyspace, dbClient)),
			Mutation: buildMutation(keyspaceSchema, keyspace.Tables, mutationFieldResolver(keyspace, dbClient)),
		},
	)
}

func queryFieldResolver(keyspace *gocql.KeyspaceMetadata, dbClient *db.Db) graphql.FieldResolveFn {
	return func(params graphql.ResolveParams) (interface{}, error) {
		fieldName := params.Info.FieldName
		switch fieldName {
		case "table":
			return getTable(keyspace, params.Args)
		case "tables":
			return getTables(keyspace)
		default:
			var table *gocql.TableMetadata
			table, tableFound := keyspace.Tables[strcase.ToSnake(fieldName)]
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
						Column:   strcase.ToSnake(key),
						Operator: "=",
						Value:    value,
					})
				}
			} else {
				if strings.HasSuffix(fieldName, "Filter") {
					table, tableFound = keyspace.Tables[strcase.ToSnake(strings.TrimSuffix(fieldName, "Filter"))]
					if !tableFound {
						return nil, fmt.Errorf("unable to find table '%s'", params.Info.FieldName)
					}

					whereClause = adaptCondition(data)
				}
			}

			var orderBy []interface{}
			var options types.QueryOptions
			if err := mapstructure.Decode(params.Args["options"], &options); err != nil {
				return nil, err
			}

			if params.Args["orderBy"] != nil {
				orderBy = params.Args["orderBy"].([]interface{})
			}

			userOrRole, err := checkAuthUserOrRole(params)
			if err != nil {
				return nil, err
			}

			result, err := dbClient.Select(&db.SelectInfo{
				Keyspace: keyspace.Name,
				Table:    table.Name,
				Where:    whereClause,
				OrderBy:  parseColumnOrder(orderBy),
				Options:  &options,
			}, db.NewQueryOptions().WithUserOrRole(userOrRole))

			if err != nil {
				return nil, err
			}

			return &types.QueryResult{
				PageState: result.PageState(),
				Values:    adaptResultValues(result.Values()),
			}, nil
		}
	}
}

func adaptCondition(data map[string]interface{}) []types.ConditionItem {
	result := make([]types.ConditionItem, 0, len(data))
	for key, value := range data {
		if value == nil {
			continue
		}
		mapValue := value.(map[string]interface{})

		for operatorName, itemValue := range mapValue {
			result = append(result, types.ConditionItem{
				Column:   strcase.ToSnake(key),
				Operator: cqlOperators[operatorName],
				Value:    itemValue,
			})
		}
	}
	return result
}

func adaptResultValues(values []map[string]interface{}) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(values))
	// TODO: Use naming conventions
	for _, item := range values {
		resultItem := make(map[string]interface{})
		for k, v := range item {
			resultItem[strcase.ToLowerCamel(k)] = v
		}
		result = append(result, resultItem)
	}

	return result
}

func mutationFieldResolver(keyspace *gocql.KeyspaceMetadata, dbClient *db.Db) graphql.FieldResolveFn {
	return func(params graphql.ResolveParams) (interface{}, error) {
		fieldName := params.Info.FieldName
		switch fieldName {
		case "createTable":
			return createTable(dbClient, keyspace.Name, params)
		case "dropTable":
			return dropTable(dbClient, keyspace.Name, params)
		default:
			operation, typeName := mutationPrefix(fieldName)
			if table, ok := keyspace.Tables[strcase.ToSnake(typeName)]; ok {
				data := params.Args["data"].(map[string]interface{})
				columnNames := make([]string, 0, len(data))
				queryParams := make([]interface{}, 0, len(data))

				for key, value := range data {
					columnNames = append(columnNames, strcase.ToSnake(key))
					queryParams = append(queryParams, value)
				}

				var options map[string]interface{}

				if params.Args["options"] != nil {
					options = params.Args["options"].(map[string]interface{})
				}

				userOrRole, err := checkAuthUserOrRole(params)
				if err != nil {
					return nil, err
				}
				queryOptions := db.NewQueryOptions().WithUserOrRole(userOrRole)
				switch operation {
				case insertPrefix:
					ttl := -1
					if options != nil {
						ttl = options["ttl"].(int)
					}
					ifNotExists := params.Args["ifNotExists"] == true
					return dbClient.Insert(&db.InsertInfo{
						Keyspace:    keyspace.Name,
						Table:       table.Name,
						Columns:     columnNames,
						QueryParams: queryParams,
						IfNotExists: ifNotExists,
						TTL:         ttl,
					}, queryOptions)
				case deletePrefix:
					var ifCondition []types.ConditionItem
					if params.Args["ifCondition"] != nil {
						ifCondition = adaptCondition(params.Args["ifCondition"].(map[string]interface{}))
					}
					return dbClient.Delete(&db.DeleteInfo{
						Keyspace:    keyspace.Name,
						Table:       table.Name,
						Columns:     columnNames,
						QueryParams: queryParams,
						IfCondition: ifCondition,
						IfExists:    params.Args["ifExists"] == true}, queryOptions)
				}

				return false, fmt.Errorf("operation '%s' not supported", operation)
			} else {
				return nil, fmt.Errorf("unable to find table for type name '%s'", params.Info.FieldName)
			}

		}
	}
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

func checkAuthUserOrRole(params graphql.ResolveParams) (string, error) {
	// TODO: Return an error if we're expecting a user/role, but one isn't provided
	value := params.Context.Value(AuthUserOrRole)
	if value == nil {
		return "", nil
	}
	return value.(string), nil
}
