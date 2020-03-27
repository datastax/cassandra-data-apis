package graphql

import (
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/riptano/data-endpoints/config"
	"github.com/riptano/data-endpoints/types"
	"log"
	"strings"

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
	naming            config.NamingConvention
	supportedOps      config.Operations
	useUserOrRoleAuth bool
}

func NewSchemaGenerator(dbClient *db.Db, cfg config.Config) *SchemaGenerator {
	return &SchemaGenerator{
		dbClient:          dbClient,
		naming:            cfg.Naming(),
		supportedOps:      cfg.SupportedOperations(),
		useUserOrRoleAuth: cfg.UseUserOrRoleAuth(),
	}
}

func (sg *SchemaGenerator) buildQueriesFields(schema *KeyspaceGraphQLSchema, tables map[string]*gocql.TableMetadata, resolve graphql.FieldResolveFn) graphql.Fields {
	fields := graphql.Fields{}
	for name, table := range tables {
		if schema.ignoredTables[table.Name] {
			continue
		}

		fields[sg.naming.ToGraphQLOperation("", name)] = &graphql.Field{
			Type: schema.resultSelectTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"data":    {Type: graphql.NewNonNull(schema.tableScalarInputTypes[table.Name])},
				"orderBy": {Type: graphql.NewList(schema.orderEnums[table.Name])},
				"options": {Type: inputQueryOptions},
			},
			Resolve: resolve,
		}

		fields[sg.naming.ToGraphQLOperation("", name)+"Filter"] = &graphql.Field{
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

func (sg *SchemaGenerator) buildQuery(schema *KeyspaceGraphQLSchema, tables map[string]*gocql.TableMetadata, resolve graphql.FieldResolveFn) *graphql.Object {
	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "TableQuery",
			Fields: sg.buildQueriesFields(schema, tables, resolve),
		})
}

func (sg *SchemaGenerator) buildMutationFields(schema *KeyspaceGraphQLSchema, tables map[string]*gocql.TableMetadata, resolve graphql.FieldResolveFn) graphql.Fields {
	fields := graphql.Fields{}
	for name, table := range tables {
		if schema.ignoredTables[table.Name] {
			continue
		}
		fields[sg.naming.ToGraphQLOperation(insertPrefix, name)] = &graphql.Field{
			Type: schema.resultUpdateTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"data":        {Type: graphql.NewNonNull(schema.tableScalarInputTypes[table.Name])},
				"ifNotExists": {Type: graphql.Boolean},
				"options":     {Type: inputMutationOptions},
			},
			Resolve: resolve,
		}

		fields[sg.naming.ToGraphQLOperation(deletePrefix, name)] = &graphql.Field{
			Type: schema.resultUpdateTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"data":        {Type: graphql.NewNonNull(schema.tableScalarInputTypes[table.Name])},
				"ifExists":    {Type: graphql.Boolean},
				"ifCondition": {Type: schema.tableOperatorInputTypes[table.Name]},
				"options":     {Type: inputMutationOptions},
			},
			Resolve: resolve,
		}

		fields[sg.naming.ToGraphQLOperation(updatePrefix, name)] = &graphql.Field{
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

	log.Printf("Building schema for %s", keyspace.Name)

	keyspaceSchema := &KeyspaceGraphQLSchema{
		ignoredTables: make(map[string]bool),
	}
	if err := keyspaceSchema.BuildTypes(keyspace, sg.naming); err != nil {
		return graphql.Schema{}, err
	}

	return graphql.NewSchema(
		graphql.SchemaConfig{
			Query:    sg.buildQuery(keyspaceSchema, keyspace.Tables, sg.queryFieldResolver(keyspace)),
			Mutation: sg.buildMutation(keyspaceSchema, keyspace.Tables, sg.mutationFieldResolver(keyspace)),
		},
	)
}

func (sg *SchemaGenerator) queryFieldResolver(keyspace *gocql.KeyspaceMetadata) graphql.FieldResolveFn {
	return func(params graphql.ResolveParams) (interface{}, error) {
		fieldName := params.Info.FieldName
		switch fieldName {
		case "table":
			return sg.getTable(keyspace, params.Args)
		case "tables":
			return sg.getTables(keyspace)
		default:
			var table *gocql.TableMetadata
			table, tableFound := keyspace.Tables[sg.naming.ToCQLTable(fieldName)]
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
						Column:   sg.naming.ToCQLColumn(key),
						Operator: "=",
						Value:    adaptParameterValue(value),
					})
				}
			} else if strings.HasSuffix(fieldName, "Filter") {
				table, tableFound = keyspace.Tables[sg.naming.ToCQLTable(strings.TrimSuffix(fieldName, "Filter"))]
				if !tableFound {
					return nil, fmt.Errorf("unable to find table '%s'", params.Info.FieldName)
				}

				whereClause = sg.adaptCondition(data)
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

			result, err := sg.dbClient.Select(&db.SelectInfo{
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
				Values:    sg.adaptResultValues(result.Values()),
			}, nil
		}
	}
}

func (sg *SchemaGenerator) adaptCondition(data map[string]interface{}) []types.ConditionItem {
	result := make([]types.ConditionItem, 0, len(data))
	for key, value := range data {
		if value == nil {
			continue
		}
		mapValue := value.(map[string]interface{})

		for operatorName, itemValue := range mapValue {
			result = append(result, types.ConditionItem{
				Column:   sg.naming.ToCQLColumn(key),
				Operator: cqlOperators[operatorName],
				Value:    adaptParameterValue(itemValue),
			})
		}
	}
	return result
}

func (sg *SchemaGenerator) adaptResultValues(values []map[string]interface{}) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(values))
	for _, item := range values {
		resultItem := make(map[string]interface{})
		for k, v := range item {
			resultItem[sg.naming.ToGraphQLField(k)] = v
		}
		result = append(result, resultItem)
	}

	return result
}

func (sg *SchemaGenerator) mutationFieldResolver(keyspace *gocql.KeyspaceMetadata) graphql.FieldResolveFn {
	return func(params graphql.ResolveParams) (interface{}, error) {
		fieldName := params.Info.FieldName
		switch fieldName {
		case "createTable":
			return sg.createTable(keyspace.Name, params)
		case "alterTableAdd":
			return sg.alterTableAdd(keyspace.Name, params)
		case "alterTableDrop":
			return sg.alterTableDrop(keyspace.Name, params)
		case "dropTable":
			return sg.dropTable(keyspace.Name, params)
		default:
			operation, typeName := mutationPrefix(fieldName)
			if table, ok := keyspace.Tables[sg.naming.ToCQLTable(typeName)]; ok {
				data := params.Args["data"].(map[string]interface{})
				columnNames := make([]string, 0, len(data))
				queryParams := make([]interface{}, 0, len(data))

				for key, value := range data {
					columnNames = append(columnNames, sg.naming.ToCQLColumn(key))
					queryParams = append(queryParams, adaptParameterValue(value))
				}

				var options map[string]interface{}

				if params.Args["options"] != nil {
					options = params.Args["options"].(map[string]interface{})
				}

				userOrRole, err := sg.checkUserOrRoleAuth(params)
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
					return sg.dbClient.Insert(&db.InsertInfo{
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
						ifCondition = sg.adaptCondition(params.Args["ifCondition"].(map[string]interface{}))
					}
					return sg.dbClient.Delete(&db.DeleteInfo{
						Keyspace:    keyspace.Name,
						Table:       table.Name,
						Columns:     columnNames,
						QueryParams: queryParams,
						IfCondition: ifCondition,
						IfExists:    params.Args["ifExists"] == true}, queryOptions)
				case updatePrefix:
					var ifCondition []types.ConditionItem
					if params.Args["ifCondition"] != nil {
						ifCondition = sg.adaptCondition(params.Args["ifCondition"].(map[string]interface{}))
					}
					ttl := -1
					if options != nil {
						ttl = options["ttl"].(int)
					}
					return sg.dbClient.Update(&db.UpdateInfo{
						Keyspace:    keyspace.Name,
						Table:       table,
						Columns:     columnNames,
						QueryParams: queryParams,
						IfCondition: ifCondition,
						TTL:         ttl,
						IfExists:    params.Args["ifExists"] == true}, queryOptions)
				}

				return false, fmt.Errorf("operation '%s' not supported", operation)
			} else {
				return nil, fmt.Errorf("unable to find table for type name '%s'", params.Info.FieldName)
			}

		}
	}
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

	//TODO: Adapt maps
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
