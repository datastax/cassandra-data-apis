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

type SchemaGenerator struct {
	dbClient          *db.Db
	namingFn          config.NamingConventionFn
	useUserOrRoleAuth bool
	ksExcluded        map[string]bool
	logger            log.Logger
}

func NewSchemaGenerator(dbClient *db.Db, cfg config.Config) *SchemaGenerator {
	ksExcluded := map[string]bool{}
	for _, ksName := range config.SystemKeyspaces {
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
			Description: fmt.Sprintf("Retrieves data from '%s' table using the equality operator.\n", table.Name) +
				"The amount of values contained in the result is limited by the page size " +
				fmt.Sprintf(" (defaults to %d). Use the pageState included in the result to ", config.DefaultPageSize) +
				"obtain the following rows.\n" +
				"When no fields are provided, it returns all rows in the table, limited by the page size.",
			Type: ksSchema.resultSelectTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"value":   {Type: ksSchema.tableScalarInputTypes[table.Name]},
				"orderBy": {Type: graphql.NewList(ksSchema.orderEnums[table.Name])},
				"options": {Type: inputQueryOptions, DefaultValue: inputQueryOptionsDefault},
			},
			Resolve: sg.queryFieldResolver(table, ksSchema, false),
		}

		fields[ksSchema.naming.ToGraphQLOperation("", table.Name)+"Filter"] = &graphql.Field{
			Description: fmt.Sprintf("Retrieves data from '%s' table using equality \n", table.Name) +
				"and non-equality operators.\n" +
				"The amount of values contained in the result is limited by the page size " +
				fmt.Sprintf(" (defaults to %d). Use the pageState included in the result to ", config.DefaultPageSize) +
				"obtain the following rows.\n",
			Type: ksSchema.resultSelectTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"filter":  {Type: graphql.NewNonNull(ksSchema.tableOperatorInputTypes[table.Name])},
				"orderBy": {Type: graphql.NewList(ksSchema.orderEnums[table.Name])},
				"options": {Type: inputQueryOptions, DefaultValue: inputQueryOptionsDefault},
			},
			Resolve: sg.queryFieldResolver(table, ksSchema, true),
		}
	}

	if len(keyspace.Tables) == 0 || len(keyspace.Tables) == len(ksSchema.ignoredTables) {
		// graphql-go requires at least a single query and a single mutation
		fields["__keyspaceEmptyQuery"] = &graphql.Field{
			Description: "Placeholder query that is exposed when a keyspace is empty.",
			Type:        graphql.Boolean,
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
			Name:   "Query",
			Fields: sg.buildQueriesFields(schema, keyspace),
		})
}

func (sg *SchemaGenerator) buildMutationFields(
	ksSchema *KeyspaceGraphQLSchema,
	keyspace *gocql.KeyspaceMetadata,
	views map[string]bool,
) graphql.Fields {
	fields := graphql.Fields{}
	for name, table := range keyspace.Tables {
		if ksSchema.ignoredTables[table.Name] || views[name] {
			continue
		}

		fields[ksSchema.naming.ToGraphQLOperation(insertPrefix, name)] = &graphql.Field{
			Description: fmt.Sprintf("Inserts an entire row or upserts data into an existing row of '%s' table. ", table.Name) +
				"Requires a value for each component of the primary key, but not for any other columns. " +
				"Missing values are left unset.",
			Type: ksSchema.resultUpdateTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"value":       {Type: graphql.NewNonNull(ksSchema.tableScalarInputTypes[table.Name])},
				"ifNotExists": {Type: graphql.Boolean},
				"options":     {Type: inputMutationOptions, DefaultValue: inputMutationOptionsDefault},
			},
			Resolve: sg.mutationFieldResolver(table, ksSchema, insertOperation),
		}

		fields[ksSchema.naming.ToGraphQLOperation(deletePrefix, name)] = &graphql.Field{
			Description: fmt.Sprintf("Removes an entire row in '%s' table.", table.Name),
			Type:        ksSchema.resultUpdateTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"value":       {Type: graphql.NewNonNull(ksSchema.tableScalarInputTypes[table.Name])},
				"ifExists":    {Type: graphql.Boolean},
				"ifCondition": {Type: ksSchema.tableOperatorInputTypes[table.Name]},
				"options":     {Type: inputMutationOptions, DefaultValue: inputMutationOptionsDefault},
			},
			Resolve: sg.mutationFieldResolver(table, ksSchema, deleteOperation),
		}

		fields[ksSchema.naming.ToGraphQLOperation(updatePrefix, name)] = &graphql.Field{
			Description: fmt.Sprintf("Updates one or more column values to a row in '%s' table.", table.Name) +
				"Like the insert operation, update is an upsert operation: if the specified row does not exist," +
				"the command creates it.",
			Type: ksSchema.resultUpdateTypes[table.Name],
			Args: graphql.FieldConfigArgument{
				"value":       {Type: graphql.NewNonNull(ksSchema.tableScalarInputTypes[table.Name])},
				"ifExists":    {Type: graphql.Boolean},
				"ifCondition": {Type: ksSchema.tableOperatorInputTypes[table.Name]},
				"options":     {Type: inputMutationOptions, DefaultValue: inputMutationOptionsDefault},
			},
			Resolve: sg.mutationFieldResolver(table, ksSchema, updateOperation),
		}
	}

	if len(keyspace.Tables) == 0 || len(keyspace.Tables) == len(ksSchema.ignoredTables) {
		// graphql-go requires at least a single query and a single mutation
		fields["__keyspaceEmptyMutation"] = &graphql.Field{
			Description: "Placeholder mutation that is exposed when a keyspace is empty.",
			Type:        graphql.Boolean,
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
	views map[string]bool,
) *graphql.Object {
	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "Mutation",
			Fields: sg.buildMutationFields(schema, keyspace, views),
		})
}

func (sg *SchemaGenerator) BuildSchemas(singleKeyspace string) (map[string]*graphql.Schema, error) {
	if singleKeyspace != "" {
		sg.logger.Info("building schema", "keyspace", singleKeyspace)
		// Schema generator is only focused on a single keyspace
		if schema, err := sg.buildSchema(singleKeyspace); err != nil {
			return nil, err
		} else {
			return map[string]*graphql.Schema{singleKeyspace: &schema}, nil
		}
	}

	keyspaces, err := sg.dbClient.Keyspaces("")
	if err != nil {
		return nil, err
	}

	sg.logger.Info("building schemas")
	result := make(map[string]*graphql.Schema, len(keyspaces))
	builtKeyspaces := make([]string, 0, len(keyspaces))
	for _, ksName := range keyspaces {
		if sg.isKeyspaceExcluded(ksName) {
			continue
		}
		schema, err := sg.buildSchema(ksName)
		if err != nil {
			return nil, err
		}

		result[ksName] = &schema
		builtKeyspaces = append(builtKeyspaces, ksName)
	}

	if len(builtKeyspaces) > 0 {
		sg.logger.Info("built keyspace schemas", "keyspaces", builtKeyspaces)
	}

	return result, nil
}

// Build GraphQL schema for tables in the provided keyspace metadata
func (sg *SchemaGenerator) buildSchema(keyspaceName string) (graphql.Schema, error) {
	keyspace, err := sg.dbClient.Keyspace(keyspaceName)
	if err != nil {
		return graphql.Schema{}, err
	}

	views, err := sg.dbClient.Views(keyspaceName) // Used to exclude views from mutations
	if err != nil {
		return graphql.Schema{}, err
	}

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
			Mutation: sg.buildMutation(keyspaceSchema, keyspace, views),
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
