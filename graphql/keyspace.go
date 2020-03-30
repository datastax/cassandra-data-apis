package graphql

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/riptano/data-endpoints/config"
	"github.com/riptano/data-endpoints/db"
	"os"
	"strconv"
	"strings"
)

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
	},
})

func (sg *SchemaGenerator) BuildKeyspaceSchema() (graphql.Schema, error) {
	return graphql.NewSchema(
		graphql.SchemaConfig{
			Query:    sg.buildKeyspaceQuery(),
			Mutation: sg.buildKeyspaceMutation(),
		})
}

type dataCenterValue struct {
	Name     string `json:"name"`
	Replicas int    `json:"replicas"`
}

type ksValue struct {
	Name string            `json:"name"`
	DCs  []dataCenterValue `json:"dcs"`
}

func buildKeyspaceValue(keyspace *gocql.KeyspaceMetadata) ksValue {
	dcs := make([]dataCenterValue, 0)
	if strings.Contains(keyspace.StrategyClass, "NetworkTopologyStrategy") {
		for dc, replicas := range keyspace.StrategyOptions {
			count, err := strconv.Atoi(replicas.(string))
			if err != nil {
				// TODO: We need log
				fmt.Fprintf(os.Stderr, "invalid replicas value ('%s') for keyspace '%s'\n", replicas, keyspace.Name)
				continue
			}
			dcs = append(dcs, dataCenterValue{
				Name:     dc,
				Replicas: count,
			})
		}
	}
	return ksValue{keyspace.Name, dcs}
}

func (sg *SchemaGenerator) buildKeyspaceQuery() *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: "KeyspaceQuery",
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
					keyspace, err := sg.dbClient.Keyspace(ksName)
					if err != nil {
						return nil, err
					}

					return buildKeyspaceValue(keyspace), nil
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
						keyspace, err := sg.dbClient.Keyspace(ksName)
						if err != nil {
							return nil, err
						}
						ksValues = append(ksValues, buildKeyspaceValue(keyspace))
					}

					return ksValues, nil
				},
			},
		},
	})
}

func (sg *SchemaGenerator) buildKeyspaceMutation() *graphql.Object {
	fields := graphql.Fields{}

	if sg.supportedOps.IsSupported(config.KeyspaceCreate) {
		fields["createKeyspace"] = &graphql.Field{
			Type: graphql.Boolean,
			Args: graphql.FieldConfigArgument{
				"name": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.String),
				},
				"dcs": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.NewList(dataCenterInput)),
				},
			},
			Resolve: func(params graphql.ResolveParams) (interface{}, error) {
				ksName := params.Args["name"].(string)
				dcs := params.Args["dcs"].([]interface{})

				dcReplicas := make(map[string]int)
				for _, dc := range dcs {
					dcReplica := dc.(map[string]interface{})
					dcReplicas[dcReplica["name"].(string)] = dcReplica["replicas"].(int)
				}

				userOrRole, err := sg.checkUserOrRoleAuth(params)
				if err != nil {
					return nil, err
				}
				return sg.dbClient.CreateKeyspace(ksName, dcReplicas, db.NewQueryOptions().WithUserOrRole(userOrRole))
			},
		}
	}

	if sg.supportedOps.IsSupported(config.KeyspaceDrop) {
		fields["dropKeyspace"] = &graphql.Field{
			Type: graphql.Boolean,
			Args: graphql.FieldConfigArgument{
				"name": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.String),
				},
			},
			Resolve: func(params graphql.ResolveParams) (interface{}, error) {
				ksName := params.Args["name"].(string)

				userOrRole, err := sg.checkUserOrRoleAuth(params)
				if err != nil {
					return nil, err
				}
				return sg.dbClient.DropKeyspace(ksName, db.NewQueryOptions().WithUserOrRole(userOrRole))
			},
		}

	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:   "KeyspaceMutation",
		Fields: fields,
	})
}
