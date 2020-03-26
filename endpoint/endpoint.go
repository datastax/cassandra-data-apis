package endpoint // TODO: Change package name?

import (
	"github.com/riptano/data-endpoints/config"
	"github.com/riptano/data-endpoints/db"
	"github.com/riptano/data-endpoints/graphql"
	"time"
)

type DataEndpointConfig struct {
	DbHosts              []string
	DbUsername           string
	DbPassword           string
	ExcludedKeyspaces    []string
	SchemaUpdateInterval time.Duration
	Naming               config.NamingConvention
	SupportedOperations  config.Operations
}

type DataEndpoint struct {
	graphQLRouteGen *graphql.RouteGenerator
}

func NewEndpointConfig(hosts ...string) *DataEndpointConfig {
	return &DataEndpointConfig{
		DbHosts:              hosts,
		SchemaUpdateInterval: 10 * time.Second,
		Naming:               config.DefaultNaming,
	}
}

func (cfg *DataEndpointConfig) NewEndpoint() (*DataEndpoint, error) {
	dbClient, err := db.NewDb(cfg.DbUsername, cfg.DbPassword, cfg.DbHosts...)
	if err != nil {
		return nil, err
	}
	return &DataEndpoint{
		graphQLRouteGen: graphql.NewRouteGenerator(dbClient, cfg.ExcludedKeyspaces, cfg.SchemaUpdateInterval,
			cfg.Naming, cfg.SupportedOperations),
	}, nil
}

func (e *DataEndpoint) RoutesGraphQL(pattern string) ([]graphql.Route, error) {
	return e.graphQLRouteGen.Routes(pattern)
}

func (e *DataEndpoint) RoutesKeyspaceGraphQL(pattern string, ksName string) ([]graphql.Route, error) {
	return e.graphQLRouteGen.RoutesKeyspace(pattern, ksName)
}
