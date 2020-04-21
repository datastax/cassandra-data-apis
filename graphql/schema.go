package graphql

import (
	"fmt"
	"github.com/datastax/cassandra-data-apis/auth"
	"github.com/datastax/cassandra-data-apis/config"
	"github.com/datastax/cassandra-data-apis/db"
	"github.com/datastax/cassandra-data-apis/log"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
)

const (
	insertPrefix = "insert"
	deletePrefix = "delete"
	updatePrefix = "update"
)

var systemKeyspaces = []string{
	"system", "system_auth", "system_distributed", "system_schema", "system_traces", "system_views", "system_virtual_schema",
	"dse_insights", "dse_insights_local", "dse_leases", "dse_perf", "dse_security", "dse_system", "dse_system_local",
	"solr_admin",
}

type SchemaGenerator struct {
	dbClient          *db.Db
	namingFn          config.NamingConventionFn
	useUserOrRoleAuth bool
	ksExcluded        map[string]bool
	logger            log.Logger
}

func NewSchemaGenerator(dbClient *db.Db, cfg config.Config) *SchemaGenerator {
	ksExcluded := map[string]bool{}
	for _, ksName := range systemKeyspaces {
		ksExcluded[ksName] = true
	}
	for _, ksName := range cfg.ExcludedKeyspaces() {
		ksExcluded[ksName] = true
	}
	return &SchemaGenerator{
		dbClient:          dbClient,
		namingFn:          cfg.Naming(),
		useUserOrRoleAuth: cfg.UseUserOrRoleAuth(),
		ksExcluded:        ksExcluded,
		logger:            cfg.Logger(),
	}
}

func (sg *SchemaGenerator) buildQueriesFields(
	ksSchema *KeyspaceGraphQLSchema,
	keyspace *gocql.KeyspaceMetadata,
) graphql.Fields {
	fields := graphql.Fields{}
	for _, table := range keyspace.Tables {
		if ksSchema.ignoredTables[table.Name] {
			continue
		}

		fields[ksSchema.naming.ToGraphQLOperation("", table.Name)] = &graphql.Field{
			Type: ksSchema.resultSelectTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"data":    {Type: ksSchema.tableScalarInputTypes[table.Name]},
				"orderBy": {Type: graphql.NewList(ksSchema.orderEnums[table.Name])},
				"options": {Type: inputQueryOptions, DefaultValue: inputQueryOptionsDefault},
			},
			Resolve: sg.queryFieldResolver(table, ksSchema, false),
		}

		fields[ksSchema.naming.ToGraphQLOperation("", table.Name)+"Filter"] = &graphql.Field{
			Type: ksSchema.resultSelectTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"filter":  {Type: graphql.NewNonNull(ksSchema.tableOperatorInputTypes[table.Name])},
				"orderBy": {Type: graphql.NewList(ksSchema.orderEnums[table.Name])},
				"options": {Type: inputQueryOptions, DefaultValue: inputQueryOptionsDefault},
			},
			Resolve: sg.queryFieldResolver(table, ksSchema, true),
		}
	}

	if len(keyspace.Tables) == 0 {
		// graphql-go requires at least a single query and a single mutation
		fields["__keyspaceEmptyQuery"] = &graphql.Field{
			Type: graphql.Boolean,
			Resolve: func(params graphql.ResolveParams) (interface{}, error) {
				return true, nil
			},
		}
	}

	return fields
}

func (sg *SchemaGenerator) buildQuery(
	schema *KeyspaceGraphQLSchema,
	keyspace *gocql.KeyspaceMetadata,
) *graphql.Object {
	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "TableQuery",
			Fields: sg.buildQueriesFields(schema, keyspace),
		})
}

func (sg *SchemaGenerator) buildMutationFields(
	ksSchema *KeyspaceGraphQLSchema,
	keyspace *gocql.KeyspaceMetadata,
) graphql.Fields {
	fields := graphql.Fields{}
	for name, table := range keyspace.Tables {
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
			Resolve: sg.mutationFieldResolver(table, ksSchema, insertOperation),
		}

		fields[ksSchema.naming.ToGraphQLOperation(deletePrefix, name)] = &graphql.Field{
			Type: ksSchema.resultUpdateTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"data":        {Type: graphql.NewNonNull(ksSchema.tableScalarInputTypes[table.Name])},
				"ifExists":    {Type: graphql.Boolean},
				"ifCondition": {Type: ksSchema.tableOperatorInputTypes[table.Name]},
				"options":     {Type: inputMutationOptions, DefaultValue: inputMutationOptionsDefault},
			},
			Resolve: sg.mutationFieldResolver(table, ksSchema, deleteOperation),
		}

		fields[ksSchema.naming.ToGraphQLOperation(updatePrefix, name)] = &graphql.Field{
			Type: ksSchema.resultUpdateTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"data":        {Type: graphql.NewNonNull(ksSchema.tableScalarInputTypes[table.Name])},
				"ifExists":    {Type: graphql.Boolean},
				"ifCondition": {Type: ksSchema.tableOperatorInputTypes[table.Name]},
				"options":     {Type: inputMutationOptions, DefaultValue: inputMutationOptionsDefault},
			},
			Resolve: sg.mutationFieldResolver(table, ksSchema, updateOperation),
		}
	}

	if len(keyspace.Tables) == 0 {
		// graphql-go requires at least a single query and a single mutation
		fields["__keyspaceEmptyMutation"] = &graphql.Field{
			Type: graphql.Boolean,
			Resolve: func(params graphql.ResolveParams) (interface{}, error) {
				return true, nil
			},
		}
	}

	return fields
}

func (sg *SchemaGenerator) buildMutation(
	schema *KeyspaceGraphQLSchema,
	keyspace *gocql.KeyspaceMetadata,
) *graphql.Object {
	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "TableMutation",
			Fields: sg.buildMutationFields(schema, keyspace),
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

	return graphql.NewSchema(
		graphql.SchemaConfig{
			Query:    sg.buildQuery(keyspaceSchema, keyspace),
			Mutation: sg.buildMutation(keyspaceSchema, keyspace),
		},
	)
}

func (sg *SchemaGenerator) isKeyspaceExcluded(ksName string) bool {
	return sg.ksExcluded[ksName]
}

func (sg *SchemaGenerator) checkUserOrRoleAuth(params graphql.ResolveParams) (string, error) {
	if sg.useUserOrRoleAuth {
		value := auth.ContextUserOrRole(params.Context)
		if value == "" {
			return "", fmt.Errorf("expected user or role for this operation")
		}
		return value, nil
	}
	return "", nil
}
