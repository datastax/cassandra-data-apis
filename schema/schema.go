package schema

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/iancoleman/strcase"
	"github.com/riptano/data-endpoints/db"
)

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

func buildQueryType(tableMeta *gocql.TableMetadata) *graphql.Object {
	fields := graphql.Fields{}

	for name, metadata := range tableMeta.Columns {
		fields[strcase.ToLowerCamel(name)] = &graphql.Field{
			Type: buildType(metadata.Type),
		}
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:   strcase.ToCamel(tableMeta.Name),
		Fields: fields,
	})
}

func buildQueryArgs(tableMeta *gocql.TableMetadata) graphql.FieldConfigArgument {
	args := graphql.FieldConfigArgument{}

	for _, metadata := range tableMeta.PartitionKey {
		args[strcase.ToLowerCamel(metadata.Name)] = &graphql.ArgumentConfig{
			Type: graphql.NewNonNull(buildType(metadata.Type)),
		}
	}

	for _, metadata := range tableMeta.ClusteringColumns {
		args[strcase.ToLowerCamel(metadata.Name)] = &graphql.ArgumentConfig{
			Type: buildType(metadata.Type),
		}
	}

	return args
}

func buildQueries(tableMetas map[string]*gocql.TableMetadata, resolve graphql.FieldResolveFn) graphql.Fields {
	fields := graphql.Fields{}
	for name, metadata := range tableMetas {
		fields[strcase.ToLowerCamel(name)] = &graphql.Field{
			Type:    graphql.NewList(buildQueryType(metadata)),
			Args:    buildQueryArgs(metadata),
			Resolve: resolve,
		}
	}
	return fields
}

func buildQuery(tableMetas map[string]*gocql.TableMetadata, resolve graphql.FieldResolveFn) *graphql.Object {
	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "Query",
			Fields: buildQueries(tableMetas, resolve),
		})
}

func buildMutation(tableMetas map[string]*gocql.TableMetadata) *graphql.Object {
	return nil
}

// Build GraphQL schema for tables in the provided keyspace metadata
func BuildSchema(keyspaceName string, db *db.Db) (graphql.Schema, error) {
	keyspaceMeta, err := db.Keyspace(keyspaceName)
	if err != nil {
		return graphql.Schema{}, err
	}

	return graphql.NewSchema(
		graphql.SchemaConfig{
			Query: buildQuery(keyspaceMeta.Tables, queryFieldResolver(keyspaceMeta, db)),
			// Mutation: buildMutation(keyspaceMeta.Tables),
		},
	)
}

func queryFieldResolver(keyspaceMeta *gocql.KeyspaceMetadata, db *db.Db) graphql.FieldResolveFn {
	return func(params graphql.ResolveParams) (interface{}, error) {
		tableMeta := keyspaceMeta.Tables[strcase.ToSnake(params.Info.FieldName)]
		if tableMeta == nil {
			return nil, fmt.Errorf("Unable to find table '%s'", params.Info.FieldName)
		}

		queryParams := make([]interface{}, 0)

		// FIXME: How do we figure out the filter columns from graphql.ResolveParams?
		//        Also, we need to valid and convert complex type here.

		whereClause := ""
		for _, column := range tableMeta.PartitionKey {
			if params.Args[column.Name] == nil {
				return nil, fmt.Errorf("Query does not contain full primary key")
			}

			queryParams = append(queryParams, params.Args[column.Name])
			if len(whereClause) > 0 {
				whereClause += fmt.Sprintf(" AND %s = ?", column.Name)
			} else {
				whereClause += fmt.Sprintf(" %s = ?", column.Name)
			}
		}

		for _, column := range tableMeta.ClusteringColumns {
			if params.Args[column.Name] != nil {
				queryParams = append(queryParams, params.Args[column.Name])
				if len(whereClause) > 0 {
					whereClause += fmt.Sprintf(" AND %s = ?", column.Name)
				} else {
					whereClause += fmt.Sprintf(" %s = ?", column.Name)
				}
			}
		}

		query := fmt.Sprintf("SELECT * FROM %s.%s WHERE%s", keyspaceMeta.Name, tableMeta.Name, whereClause)

		iter := db.Select(query, gocql.LocalOne, queryParams...)

		results := make([]map[string]interface{}, 0)
		row := map[string]interface{}{}

		for iter.MapScan(row) {
			rowCamel := map[string]interface{}{}
			for k, v := range row {
				rowCamel[strcase.ToLowerCamel(k)] = v
			}
			results = append(results, rowCamel)
			row = map[string]interface{}{}
		}

		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("Error executing query: %v", err)
		}

		return results, nil
	}
}
