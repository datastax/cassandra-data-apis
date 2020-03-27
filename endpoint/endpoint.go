package endpoint // TODO: Change package name?

import (
	"github.com/riptano/data-endpoints/config"
	"github.com/riptano/data-endpoints/db"
	"github.com/riptano/data-endpoints/graphql"
	"time"
)

type DataEndpointConfig struct {
	dbHosts           []string
	dbUsername        string
	dbPassword        string
	ksExcluded        []string
	updateInterval    time.Duration
	naming            config.NamingConvention
	supportedOps      config.Operations
	useUserOrRoleAuth bool
}

func (cfg DataEndpointConfig) ExcludedKeyspaces() []string {
	return cfg.ksExcluded
}

func (cfg DataEndpointConfig) SchemaUpdateInterval() time.Duration {
	return cfg.updateInterval
}

func (cfg DataEndpointConfig) Naming() config.NamingConvention {
	return cfg.naming
}

func (cfg DataEndpointConfig) SupportedOperations() config.Operations {
	return cfg.supportedOps
}

func (cfg DataEndpointConfig) UseUserOrRoleAuth() bool {
	return cfg.useUserOrRoleAuth
}

func (cfg *DataEndpointConfig) SetExcludedKeyspaces(ksExcluded []string) {
	cfg.ksExcluded = ksExcluded
}

func (cfg *DataEndpointConfig) SetSchemaUpdateInterval(updateInterval time.Duration) {
	cfg.updateInterval = updateInterval
}

func (cfg *DataEndpointConfig) SetNaming(naming config.NamingConvention) {
	cfg.naming = naming
}

func (cfg *DataEndpointConfig) SetSupportedOperations(supportedOps config.Operations) {
	cfg.supportedOps = supportedOps
}

func (cfg *DataEndpointConfig) SetUseUserOrRoleAuth(useUserOrRowAuth bool) {
	cfg.useUserOrRoleAuth = useUserOrRowAuth
}

func (cfg *DataEndpointConfig) SetDbUsername(dbUsername string) {
	cfg.dbUsername = dbUsername
}

func (cfg *DataEndpointConfig) SetDbPassword(dbPassword string) {
	cfg.dbPassword = dbPassword
}

func (cfg DataEndpointConfig) NewEndpoint() (*DataEndpoint, error) {
	dbClient, err := db.NewDb(cfg.dbUsername, cfg.dbPassword, cfg.dbHosts...)
	if err != nil {
		return nil, err
	}
	return cfg.newEndpointWithDb(dbClient), nil
}

func (cfg DataEndpointConfig) newEndpointWithDb(dbClient* db.Db) *DataEndpoint {
	return &DataEndpoint{
		graphQLRouteGen: graphql.NewRouteGenerator(dbClient, cfg),
	}
}

type DataEndpoint struct {
	graphQLRouteGen *graphql.RouteGenerator
}

func NewEndpointConfig(hosts ...string) *DataEndpointConfig {
	return &DataEndpointConfig{
		dbHosts:        hosts,
		updateInterval: 10 * time.Second,
		naming:         config.DefaultNaming,
	}
}

func (e *DataEndpoint) RoutesGraphQL(pattern string) ([]graphql.Route, error) {
	return e.graphQLRouteGen.Routes(pattern)
}

func (e *DataEndpoint) RoutesKeyspaceGraphQL(pattern string, ksName string) ([]graphql.Route, error) {
	return e.graphQLRouteGen.RoutesKeyspace(pattern, ksName)
}
