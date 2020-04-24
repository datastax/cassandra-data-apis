package graphql

import (
	"errors"
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
		"tables": &graphql.Field{
			Type: graphql.NewList(tableType),
			Args: graphql.FieldConfigArgument{
				"name": {Type: graphql.String},
			},
			Resolve: func(p graphql.ResolveParams) (i interface{}, err error) {
				parent := p.Source.(ksValue)
				return getTables(parent.keyspace, p.Args)
			},
		},
	},
})

func (sg *SchemaGenerator) BuildKeyspaceSchema(ops config.SchemaOperations) (graphql.Schema, error) {
	return graphql.NewSchema(
		graphql.SchemaConfig{
			Query:    sg.buildKeyspaceQuery(),
			Mutation: sg.buildKeyspaceMutation(ops),
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

func (sg *SchemaGenerator) buildKeyspaceQuery() *graphql.Object {
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
					if sg.isKeyspaceExcluded(ksName) {
						return nil, errors.New("keyspace does not exist")
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
					ksNames, err := sg.dbClient.Keyspaces()
					if err != nil {
						return nil, err
					}

					ksValues := make([]ksValue, 0)
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

					return ksValues, nil
				},
			},
		},
	})
}

func (sg *SchemaGenerator) buildKeyspaceMutation(ops config.SchemaOperations) *graphql.Object {
	fields := graphql.Fields{}

	if ops.IsSupported(config.KeyspaceCreate) {
		fields["createKeyspace"] = &graphql.Field{
			Type: keyspaceType,
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

				err =  sg.dbClient.CreateKeyspace(&db.CreateKeyspaceInfo{
					Name:        ksName,
					DCReplicas:  dcReplicas,
					IfNotExists: getBoolArgs(args, "ifNotExists"),
				}, db.NewQueryOptions().WithUserOrRole(userOrRole))
				if err != nil {
					return nil, err
				}

				keyspace, err := sg.dbClient.Keyspace(ksName)
				if err != nil {
					return err, nil
				}

				return sg.buildKeyspaceValue(keyspace), nil
			},
		}
	}

	if ops.IsSupported(config.KeyspaceDrop) {
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

				return true, sg.dbClient.DropKeyspace(&db.DropKeyspaceInfo{
					Name:     ksName,
					IfExists: getBoolArgs(args, "ifExists"),
				}, db.NewQueryOptions().WithUserOrRole(userOrRole))
			},
		}

	}

	if ops.IsSupported(config.TableCreate) {
		fields["createTable"] = &graphql.Field{
			Type: tableType,
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
				return sg.createTable(p)
			},
		}
	}

	if ops.IsSupported(config.TableAlterAdd) {
		fields["alterTableAdd"] = &graphql.Field{
			Type: tableType,
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
				return sg.alterTableAdd(p)
			},
		}
	}

	if ops.IsSupported(config.TableAlterDrop) {
		fields["alterTableDrop"] = &graphql.Field{
			Type: tableType,
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
				return sg.alterTableDrop(p)
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
				return sg.dropTable(p)
			},
		}
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:   "Mutation",
		Fields: fields,
	})
}

func getBoolArgs(args map[string]interface{}, name string) bool {
	if value, ok := args[name]; ok {
		return value.(bool)
	}
	return false
}
