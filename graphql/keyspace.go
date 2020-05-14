package graphql

import (
	"fmt"
	"github.com/datastax/cassandra-data-apis/config"
	"github.com/datastax/cassandra-data-apis/db"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"strconv"
	"strings"
)

type dataCenterValue struct {
	Name     string `json:"name"`
	Replicas int    `json:"replicas"`
}

type ksValue struct {
	Name     string            `json:"name"`
	DCs      []dataCenterValue `json:"dcs"`
	keyspace *gocql.KeyspaceMetadata
}

var dataCenterType = graphql.NewObject(graphql.ObjectConfig{
	Name: "DataCenter",
	Fields: graphql.Fields{
		"name": &graphql.Field{
			Type: graphql.NewNonNull(graphql.String),
		},
		"replicas": &graphql.Field{
			Type: graphql.NewNonNull(graphql.Int),
		},
	},
})

var dataCenterInput = graphql.NewInputObject(graphql.InputObjectConfig{
	Name: "DataCenterInput",
	Fields: graphql.InputObjectConfigFieldMap{
		"name": &graphql.InputObjectFieldConfig{
			Type: graphql.NewNonNull(graphql.String),
		},
		"replicas": &graphql.InputObjectFieldConfig{
			Type: graphql.NewNonNull(graphql.Int),
		},
	},
})

var keyspaceType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Keyspace",
	Fields: graphql.Fields{
		"name": &graphql.Field{
			Type: graphql.NewNonNull(graphql.String),
		},
		"dcs": &graphql.Field{
			Type: graphql.NewList(dataCenterType),
		},
		"table": &graphql.Field{
			Type: tableType,
			Args: graphql.FieldConfigArgument{
				"name": {
					Type: graphql.NewNonNull(graphql.String),
				},
			},
			Resolve: func(p graphql.ResolveParams) (i interface{}, err error) {
				parent := p.Source.(ksValue)
				return getTables(parent.keyspace, p.Args)
			},
		},
		"tables": &graphql.Field{
			Type: graphql.NewList(tableType),
			Resolve: func(p graphql.ResolveParams) (i interface{}, err error) {
				parent := p.Source.(ksValue)
				return getTables(parent.keyspace, p.Args)
			},
		},
	},
})

func (sg *SchemaGenerator) BuildKeyspaceSchema(singleKeyspace string, ops config.SchemaOperations) (graphql.Schema, error) {
	return graphql.NewSchema(
		graphql.SchemaConfig{
			Query:    sg.buildKeyspaceQuery(singleKeyspace),
			Mutation: sg.buildKeyspaceMutation(singleKeyspace, ops),
		})
}

func (sg *SchemaGenerator) buildKeyspaceValue(keyspace *gocql.KeyspaceMetadata) ksValue {
	dcs := make([]dataCenterValue, 0)
	if strings.Contains(keyspace.StrategyClass, "NetworkTopologyStrategy") {
		for dc, replicas := range keyspace.StrategyOptions {
			count, err := strconv.Atoi(replicas.(string))
			if err != nil {
				sg.logger.Error("invalid replicas value for keyspace",
					"replicas", replicas,
					"keyspace", keyspace.Name)
				continue
			}
			dcs = append(dcs, dataCenterValue{
				Name:     dc,
				Replicas: count,
			})
		}
	}
	return ksValue{
		keyspace.Name,
		dcs,
		keyspace,
	}
}

func (sg *SchemaGenerator) buildKeyspaceQuery(singleKeyspace string) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"keyspace": &graphql.Field{
				Type: keyspaceType,
				Args: graphql.FieldConfigArgument{
					"name": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.String),
					},
				},
				Resolve: func(params graphql.ResolveParams) (interface{}, error) {
					ksName := params.Args["name"].(string)
					if sg.isKeyspaceExcluded(ksName) || (singleKeyspace != "" && ksName != singleKeyspace) {
						return nil, fmt.Errorf("keyspace does not exist '%s'", ksName)
					}
					keyspace, err := sg.dbClient.Keyspace(ksName)
					if err != nil {
						return nil, err
					}

					return sg.buildKeyspaceValue(keyspace), nil
				},
			},
			"keyspaces": &graphql.Field{
				Type: graphql.NewList(keyspaceType),
				Resolve: func(params graphql.ResolveParams) (interface{}, error) {
					ksValues := make([]ksValue, 0)
					if singleKeyspace == "" {
						ksNames, err := sg.dbClient.Keyspaces()
						if err != nil {
							return nil, err
						}

						for _, ksName := range ksNames {
							if sg.isKeyspaceExcluded(ksName) {
								continue
							}
							keyspace, err := sg.dbClient.Keyspace(ksName)
							if err != nil {
								return nil, err
							}
							ksValues = append(ksValues, sg.buildKeyspaceValue(keyspace))
						}
					} else {
						if keyspace, err := sg.dbClient.Keyspace(singleKeyspace); err == nil {
							ksValues = append(ksValues, sg.buildKeyspaceValue(keyspace))
						} else {
							sg.logger.Warn("unable to get single keyspace",
								"keyspace", singleKeyspace,
								"error", err)
						}
					}
					return ksValues, nil
				},
			},
		},
	})
}

func (sg *SchemaGenerator) buildKeyspaceMutation(singleKeyspace string, ops config.SchemaOperations) *graphql.Object {
	fields := graphql.Fields{}

	if ops.IsSupported(config.KeyspaceCreate) && singleKeyspace == "" {
		fields["createKeyspace"] = &graphql.Field{
			Type: graphql.Boolean,
			Args: graphql.FieldConfigArgument{
				"name": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.String),
				},
				"dcs": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.NewList(dataCenterInput)),
				},
				"ifNotExists": &graphql.ArgumentConfig{
					Type: graphql.Boolean,
				},
			},
			Resolve: func(params graphql.ResolveParams) (interface{}, error) {
				args := params.Args
				ksName := args["name"].(string)
				dcs := args["dcs"].([]interface{})

				dcReplicas := make(map[string]int)
				for _, dc := range dcs {
					dcReplica := dc.(map[string]interface{})
					dcReplicas[dcReplica["name"].(string)] = dcReplica["replicas"].(int)
				}

				userOrRole, err := sg.checkUserOrRoleAuth(params)
				if err != nil {
					return nil, err
				}

				err = sg.dbClient.CreateKeyspace(&db.CreateKeyspaceInfo{
					Name:        ksName,
					DCReplicas:  dcReplicas,
					IfNotExists: getBoolArg(args, "ifNotExists"),
				}, db.NewQueryOptions().WithUserOrRole(userOrRole).WithContext(params.Context))
				return err != nil, err
			},
		}
	}

	if ops.IsSupported(config.KeyspaceDrop) && singleKeyspace == "" {
		fields["dropKeyspace"] = &graphql.Field{
			Type: graphql.Boolean,
			Args: graphql.FieldConfigArgument{
				"name": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.String),
				},
				"ifExists": &graphql.ArgumentConfig{
					Type: graphql.Boolean,
				},
			},
			Resolve: func(params graphql.ResolveParams) (interface{}, error) {
				args := params.Args
				ksName := args["name"].(string)

				userOrRole, err := sg.checkUserOrRoleAuth(params)
				if err != nil {
					return nil, err
				}

				err = sg.dbClient.DropKeyspace(&db.DropKeyspaceInfo{
					Name:     ksName,
					IfExists: getBoolArg(args, "ifExists"),
				}, db.NewQueryOptions().WithUserOrRole(userOrRole).WithContext(params.Context))
				return err != nil, err
			},
		}

	}

	if ops.IsSupported(config.TableCreate) {
		fields["createTable"] = &graphql.Field{
			Type: graphql.Boolean,
			Args: graphql.FieldConfigArgument{
				"keyspaceName": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.String),
				},
				"tableName": &graphql.ArgumentConfig{
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
				"ifNotExists": &graphql.ArgumentConfig{
					Type: graphql.Boolean,
				},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return checkSingleKeyspace(singleKeyspace, p, sg.createTable)
			},
		}
	}

	if ops.IsSupported(config.TableAlterAdd) {
		fields["alterTableAdd"] = &graphql.Field{
			Type: graphql.Boolean,
			Args: graphql.FieldConfigArgument{
				"keyspaceName": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.String),
				},
				"tableName": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.String),
				},
				"toAdd": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.NewList(columnInput)),
				},
			},
			Resolve: func(p graphql.ResolveParams) (i interface{}, err error) {
				return checkSingleKeyspace(singleKeyspace, p, sg.alterTableAdd)
			},
		}
	}

	if ops.IsSupported(config.TableAlterDrop) {
		fields["alterTableDrop"] = &graphql.Field{
			Type: graphql.Boolean,
			Args: graphql.FieldConfigArgument{
				"keyspaceName": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.String),
				},
				"tableName": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.String),
				},
				"toDrop": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.NewList(graphql.String)),
				},
			},
			Resolve: func(p graphql.ResolveParams) (i interface{}, err error) {
				return checkSingleKeyspace(singleKeyspace, p, sg.alterTableDrop)
			},
		}
	}

	if ops.IsSupported(config.TableDrop) {
		fields["dropTable"] = &graphql.Field{
			Type: graphql.Boolean,
			Args: graphql.FieldConfigArgument{
				"keyspaceName": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.String),
				},
				"tableName": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.String),
				},
				"ifExists": &graphql.ArgumentConfig{
					Type: graphql.Boolean,
				},
			},
			Resolve: func(p graphql.ResolveParams) (i interface{}, err error) {
				return checkSingleKeyspace(singleKeyspace, p, sg.dropTable)
			},
		}
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:   "Mutation",
		Fields: fields,
	})
}

func getBoolArg(args map[string]interface{}, name string) bool {
	if value, ok := args[name]; ok {
		return value.(bool)
	}
	return false
}

func checkSingleKeyspace(singleKeyspace string, p graphql.ResolveParams,
	op func(params graphql.ResolveParams) (i interface{}, err error)) (i interface{}, err error) {
	ksName := p.Args["keyspaceName"].(string)
	if singleKeyspace != "" && ksName != singleKeyspace {
		return nil, fmt.Errorf("keyspace does not exist '%s'", ksName)
	}
	return op(p)
}

