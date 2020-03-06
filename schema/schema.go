package schema

import (
	"fmt"
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

func buildType(typeInfo gocql.TypeInfo) graphql.Output {
	switch typeInfo.Type() {
	case gocql.TypeInt:
		return graphql.Int
	case gocql.TypeVarchar:
		return graphql.String
	case gocql.TypeText:
		return graphql.String
	case gocql.TypeUUID:
		return graphql.String
	case gocql.TypeTimeUUID:
		return graphql.String
	default:
		panic("Unsupported type")
	}
}

func buildQueryType(table *gocql.TableMetadata) *graphql.Object {
	fields := graphql.Fields{}

	for name, column := range table.Columns {
		fields[strcase.ToLowerCamel(name)] = &graphql.Field{
			Type: buildType(column.Type),
		}
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:   strcase.ToCamel(table.Name),
		Fields: fields,
	})
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

func buildQueriesFields(tables map[string]*gocql.TableMetadata, resolve graphql.FieldResolveFn) graphql.Fields {
	fields := graphql.Fields{}
	for name, table := range tables {
		fields[strcase.ToLowerCamel(name)] = &graphql.Field{
			Type:    graphql.NewList(buildQueryType(table)),
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
		Type: graphql.NewList(tableType),
		Resolve: resolve,
	}
	return fields
}

func buildQuery(tables map[string]*gocql.TableMetadata, resolve graphql.FieldResolveFn) *graphql.Object {
	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "TableQuery",
			Fields: buildQueriesFields(tables, resolve),
		})
}

func buildMutationFields(tables map[string]*gocql.TableMetadata, resolve graphql.FieldResolveFn) graphql.Fields {
	fields := graphql.Fields{}
	for name, table := range tables {
		fields[insertPrefix+strcase.ToCamel(name)] = &graphql.Field{
			Type:    graphql.Boolean,
			Args:    buildInsertArgs(table),
			Resolve: resolve,
		}

		fields[deletePrefix+strcase.ToCamel(name)] = &graphql.Field{
			Type:    graphql.Boolean,
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
			"primaryKey": &graphql.ArgumentConfig{
				Type: graphql.NewNonNull(graphql.NewList(columnInput)),
			},
			"clusteringKey": &graphql.ArgumentConfig{
				Type: graphql.NewList(columnInput),
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

func buildMutation(tables map[string]*gocql.TableMetadata, resolveFn graphql.FieldResolveFn) *graphql.Object {
	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "TableMutation",
			Fields: buildMutationFields(tables, resolveFn),
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

	return graphql.NewSchema(
		graphql.SchemaConfig{
			Query:    buildQuery(keyspace.Tables, queryFieldResolver(keyspace, db)),
			Mutation: buildMutation(keyspace.Tables, mutationFieldResolver(keyspace, db)),
		},
	)
}

func queryFieldResolver(keyspace *gocql.KeyspaceMetadata, db *db.Db) graphql.FieldResolveFn {
	return func(params graphql.ResolveParams) (interface{}, error) {
		fieldName := params.Info.FieldName
		if fieldName == "table" {
			return getTable(keyspace, params.Args)
		} else if fieldName == "tables" {
			return getTables(keyspace);
		}
		table := keyspace.Tables[strcase.ToSnake(fieldName)]
		if table == nil {
			return nil, fmt.Errorf("unable to find table '%s'", params.Info.FieldName)
		}

		columnNames := make([]string, 0)
		queryParams := make([]interface{}, 0)

		// FIXME: How do we figure out the select expression columns from graphql.ResolveParams?
		//        Also, we need to validate and convert complex type here.

		for key, value := range params.Args {
			columnNames = append(columnNames, strcase.ToSnake(key))
			queryParams = append(queryParams, value)
		}

		return db.Select(columnNames, queryParams, keyspace.Name, table)
	}
}

func mutationFieldResolver(keyspace *gocql.KeyspaceMetadata, db *db.Db) graphql.FieldResolveFn {
	return func(params graphql.ResolveParams) (interface{}, error) {
		fieldName := params.Info.FieldName
		if fieldName == "createTable" {
			return createTable(keyspace, db, params.Args)
		} else if fieldName == "dropTable" {
			return dropTable(db, params.Args)
		}
		operation, typeName := mutationPrefix(fieldName)
		// TODO: Extract name conventions
		table := keyspace.Tables[strcase.ToSnake(typeName)]
		if table == nil {
			return nil, fmt.Errorf("unable to find table for type name '%s'", params.Info.FieldName)
		}

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
