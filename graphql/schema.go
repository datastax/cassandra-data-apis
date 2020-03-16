package graphql

import (
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"net/http"
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

type requestBody struct {
	Query string `json:"query"`
}

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

func buildQueryArgs(table *gocql.TableMetadata) graphql.FieldConfigArgument {
	args := graphql.FieldConfigArgument{}

	for _, column := range table.PartitionKey {
		args[strcase.ToLowerCamel(column.Name)] = &graphql.ArgumentConfig{
			Type: graphql.NewNonNull(buildType(column.Type)),
		}
	}

	for _, column := range table.ClusteringColumns {
		args[strcase.ToLowerCamel(column.Name)] = &graphql.ArgumentConfig{
			Type: buildType(column.Type),
		}
	}

	return args
}

func buildQueriesFields(schema *KeyspaceGraphQLSchema, tables map[string]*gocql.TableMetadata, resolve graphql.FieldResolveFn) graphql.Fields {
	fields := graphql.Fields{}
	for name, table := range tables {
		fields[strcase.ToLowerCamel(name)] = &graphql.Field{
			Type:    schema.resultSelectTypes[table.Name],
			Args:    buildQueryArgs(table),
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
			Type:    schema.resultUpdateTypes[table.Name],
			Args:    buildInsertArgs(table),
			Resolve: resolve,
		}

		fields[deletePrefix+strcase.ToCamel(name)] = &graphql.Field{
			Type:    schema.resultUpdateTypes[table.Name],
			Args:    buildQueryArgs(table),
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

// Marks partition and clustering keys as required, the rest as optional
func buildInsertArgs(table *gocql.TableMetadata) graphql.FieldConfigArgument {
	args := graphql.FieldConfigArgument{}

	for _, column := range table.PartitionKey {
		//TODO: Extract name convention configuration
		args[strcase.ToLowerCamel(column.Name)] = &graphql.ArgumentConfig{
			Type: graphql.NewNonNull(buildType(column.Type)),
		}
	}

	for _, column := range table.ClusteringColumns {
		args[strcase.ToLowerCamel(column.Name)] = &graphql.ArgumentConfig{
			Type: graphql.NewNonNull(buildType(column.Type)),
		}
	}

	for _, column := range table.Columns {
		memberName := strcase.ToLowerCamel(column.Name)
		if _, ok := args[memberName]; !ok {
			// Add the rest as optional
			args[memberName] = &graphql.ArgumentConfig{
				Type: buildType(column.Type),
			}
		}
	}

	return args
}

// Build GraphQL schema for tables in the provided keyspace metadata
func BuildSchema(keyspaceName string, db *db.Db) (graphql.Schema, error) {
	keyspace, err := db.Keyspace(keyspaceName)
	if err != nil {
		return graphql.Schema{}, err
	}

	keyspaceSchema := &KeyspaceGraphQLSchema{}
	if err := keyspaceSchema.BuildTypes(keyspace); err != nil {
		return graphql.Schema{}, err
	}

	return graphql.NewSchema(
		graphql.SchemaConfig{
			Query:    buildQuery(keyspaceSchema, keyspace.Tables, queryFieldResolver(keyspace, db)),
			Mutation: buildMutation(keyspaceSchema, keyspace.Tables, mutationFieldResolver(keyspace, db)),
			Types:    keyspaceSchema.AllTypes(),
		},
	)
}

func queryFieldResolver(keyspace *gocql.KeyspaceMetadata, db *db.Db) graphql.FieldResolveFn {
	return func(params graphql.ResolveParams) (interface{}, error) {
		fieldName := params.Info.FieldName
		switch fieldName {
		case "table":
			return getTable(keyspace, params.Args)
		case "tables":
			return getTables(keyspace)
		default:
			if table, ok := keyspace.Tables[strcase.ToSnake(fieldName)]; ok {
				columnNames := make([]string, 0)
				queryParams := make([]interface{}, 0)

				// FIXME: How do we figure out the select expression columns from graphql.ResolveParams?
				//        Also, we need to validate and convert complex types here.

				for key, value := range params.Args {
					columnNames = append(columnNames, strcase.ToSnake(key))
					queryParams = append(queryParams, value)
				}

				return db.Select(columnNames, queryParams, keyspace.Name, table)
			} else {
				return nil, fmt.Errorf("unable to find table '%s'", params.Info.FieldName)
			}

		}
	}
}

func mutationFieldResolver(keyspace *gocql.KeyspaceMetadata, db *db.Db) graphql.FieldResolveFn {
	return func(params graphql.ResolveParams) (interface{}, error) {
		fieldName := params.Info.FieldName
		switch fieldName {
		case "createTable":
			return createTable(db, keyspace.Name, params.Args)
		case "dropTable":
			return dropTable(db, keyspace.Name, params.Args)
		default:
			operation, typeName := mutationPrefix(fieldName)
			// TODO: Extract name conventions
			if table, ok := keyspace.Tables[strcase.ToSnake(typeName)]; ok {
				columnNames := make([]string, 0)
				queryParams := make([]interface{}, 0)

				for key, value := range params.Args {
					columnNames = append(columnNames, strcase.ToSnake(key))
					queryParams = append(queryParams, value)
				}

				switch operation {
				case insertPrefix:
					return db.Insert(columnNames, queryParams, keyspace.Name, table)
				case deletePrefix:
					return db.Delete(columnNames, queryParams, keyspace.Name, table)
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

func GetHandler(schema graphql.Schema) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		result := executeQuery(r.URL.Query().Get("query"), schema)
		json.NewEncoder(w).Encode(result)
	}
}

func PostHandler(schema graphql.Schema) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		if r.Body == nil {
			http.Error(w, "No request body", 400)
			return
		}

		var body requestBody
		err := json.NewDecoder(r.Body).Decode(&body)
		if err != nil {
			http.Error(w, "Request body is invalid", 400)
			return
		}

		result := executeQuery(body.Query, schema)
		json.NewEncoder(w).Encode(result)
	}
}

func executeQuery(query string, schema graphql.Schema) *graphql.Result {
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
	})
	if len(result.Errors) > 0 {
		fmt.Printf("wrong result, unexpected errors: %v", result.Errors)
	}
	return result
}
