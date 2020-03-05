package schema

import (
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/iancoleman/strcase"
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
func BuildSchema(keyspaceMeta *gocql.KeyspaceMetadata, resolve graphql.FieldResolveFn) (graphql.Schema, error) {
	return graphql.NewSchema(
		graphql.SchemaConfig{
			Query: buildQuery(keyspaceMeta.Tables, resolve),
			// Mutation: buildMutation(keyspaceMeta.Tables),
		},
	)
}
