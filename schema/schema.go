package schema

import (
	"fmt"
	"strings"

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
			Name:   "TableQuery",
			Fields: buildQueries(tableMetas, resolve),
		})
}

func buildMutationFields(tableMetas map[string]*gocql.TableMetadata, resolve graphql.FieldResolveFn) graphql.Fields {
	fields := graphql.Fields{}
	for name, table := range tableMetas {
		fields["insert"+strcase.ToCamel(name)] = &graphql.Field{
			Type:    graphql.Boolean,
			Args:    buildInsertArgs(table),
			Resolve: resolve,
		}

		//fields["delete" + strcase.ToCamel(name)] = &graphql.Field{
		//	Type:    graphql.Boolean,
		//	Args:    buildQueryArgs(table),
		//	Resolve: resolve,
		//}
	}
	return fields
}

func buildMutation(tableMetas map[string]*gocql.TableMetadata, resolveFn graphql.FieldResolveFn) *graphql.Object {
	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "TableMutation",
			Fields: buildMutationFields(tableMetas, resolveFn),
		})
}

// Marks partition and clustering keys as required, the rest as optional
func buildInsertArgs(tableMeta *gocql.TableMetadata) graphql.FieldConfigArgument {
	args := graphql.FieldConfigArgument{}

	for _, column := range tableMeta.PartitionKey {
		//TODO: Extract name convention configuration
		args[strcase.ToLowerCamel(column.Name)] = &graphql.ArgumentConfig{
			Type: graphql.NewNonNull(buildType(column.Type)),
		}
	}

	for _, column := range tableMeta.ClusteringColumns {
		args[strcase.ToLowerCamel(column.Name)] = &graphql.ArgumentConfig{
			Type: graphql.NewNonNull(buildType(column.Type)),
		}
	}

	for _, column := range tableMeta.Columns {
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
	keyspaceMeta, err := db.Keyspace(keyspaceName)
	if err != nil {
		return graphql.Schema{}, err
	}

	return graphql.NewSchema(
		graphql.SchemaConfig{
			Query:    buildQuery(keyspaceMeta.Tables, queryFieldResolver(keyspaceMeta, db)),
			Mutation: buildMutation(keyspaceMeta.Tables, mutationFieldResolver(keyspaceMeta, db)),
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

func mutationFieldResolver(keyspaceMeta *gocql.KeyspaceMetadata, db *db.Db) graphql.FieldResolveFn {
	return func(params graphql.ResolveParams) (interface{}, error) {
		// TODO: Extract name conventions
		tableName := strcase.ToSnake(removeMutationPrefix(params.Info.FieldName))
		table := keyspaceMeta.Tables[tableName]
		if table == nil {
			return nil, fmt.Errorf("Unable to find table '%s'", tableName)
		}

		queryParams := make([]interface{}, 0)
		columnNames := []string{}
		placeholders := []string{}

		for key, value := range params.Args {
			columnNames = append(columnNames, key)
			queryParams = append(queryParams, value)
			placeholders = append(placeholders, "?")
			// Probably not very idiomatic...
		}

		query := fmt.Sprintf(
			"INSERT INTO %s.%s (%s) VALUES (%s)",
			keyspaceMeta.Name, table.Name, strings.Join(columnNames, ","), strings.Join(placeholders, ","))

		fmt.Printf(query + "\n")

		iter := db.Select(query, gocql.LocalOne, queryParams...)

		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("Error executing query: %v", err)
		}

		return true, nil
	}
}

func removeMutationPrefix(value string) string {
	if strings.Index(value, "insert") == 0 {
		return value[len("insert"):]
	}
	panic("Unsupported mutation")
}
