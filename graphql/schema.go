package graphql

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/riptano/data-endpoints/auth"
	"github.com/riptano/data-endpoints/config"
	"github.com/riptano/data-endpoints/db"
	"github.com/riptano/data-endpoints/log"
	"github.com/riptano/data-endpoints/types"
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
	ksExcluded        []string
	logger            log.Logger
}

var appliedModificationResult = types.ModificationResult{Applied: true}

func NewSchemaGenerator(dbClient *db.Db, cfg config.Config) *SchemaGenerator {
	return &SchemaGenerator{
		dbClient:          dbClient,
		namingFn:          cfg.Naming(),
		supportedOps:      cfg.SupportedOperations(),
		useUserOrRoleAuth: cfg.UseUserOrRoleAuth(),
		ksExcluded:        cfg.ExcludedKeyspaces(),
		logger:            cfg.Logger(),
	}
}

func (sg *SchemaGenerator) buildQueriesFields(
	ksSchema *KeyspaceGraphQLSchema,
	tables map[string]*gocql.TableMetadata,
	resolve graphql.FieldResolveFn,
) graphql.Fields {
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

func (sg *SchemaGenerator) buildQuery(
	schema *KeyspaceGraphQLSchema,
	tables map[string]*gocql.TableMetadata,
	resolve graphql.FieldResolveFn,
) *graphql.Object {
	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "TableQuery",
			Fields: sg.buildQueriesFields(schema, tables, resolve),
		})
}

func (sg *SchemaGenerator) buildMutationFields(ksSchema *KeyspaceGraphQLSchema,
	tables map[string]*gocql.TableMetadata,
	resolve graphql.FieldResolveFn,
) graphql.Fields {
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

func (sg *SchemaGenerator) buildMutation(
	schema *KeyspaceGraphQLSchema,
	tables map[string]*gocql.TableMetadata,
	resolveFn graphql.FieldResolveFn,
) *graphql.Object {
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

func (sg* SchemaGenerator) isKeyspaceExcluded(ksName string) bool {
	return isKeyspaceExcluded(ksName, systemKeyspaces) || isKeyspaceExcluded(ksName, sg.ksExcluded)
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
