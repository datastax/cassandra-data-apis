package endpoint

import (
	"github.com/datastax/cassandra-data-apis/config"
	"github.com/datastax/cassandra-data-apis/db"
	"github.com/datastax/cassandra-data-apis/graphql"
	"github.com/datastax/cassandra-data-apis/log"
	"github.com/datastax/cassandra-data-apis/rest"
	"github.com/datastax/cassandra-data-apis/types"
	"go.uber.org/zap"
	"time"
)

const DefaultSchemaUpdateDuration = 10 * time.Second
const DefaultSchemaExpireDuration = 30 * time.Second

type DataEndpointConfig struct {
	dbConfig          db.Config
	dbHosts           []string
	ksExcluded        []string
	updateInterval    time.Duration
	expireInterval    time.Duration
	naming            config.NamingConventionFn
	useUserOrRoleAuth bool
	logger            log.Logger
	routerInfo        config.HttpRouterInfo
}

func (cfg DataEndpointConfig) ExcludedKeyspaces() []string {
	return cfg.ksExcluded
}

func (cfg DataEndpointConfig) SchemaUpdateInterval() time.Duration {
	return cfg.updateInterval
}

func (cfg DataEndpointConfig) SchemaExpireInterval() time.Duration {
	return cfg.expireInterval
}

func (cfg DataEndpointConfig) Naming() config.NamingConventionFn {
	return cfg.naming
}

func (cfg DataEndpointConfig) UseUserOrRoleAuth() bool {
	return cfg.useUserOrRoleAuth
}

func (cfg DataEndpointConfig) DbConfig() db.Config {
	return cfg.dbConfig
}

func (cfg DataEndpointConfig) Logger() log.Logger {
	return cfg.logger
}

func (cfg DataEndpointConfig) RouterInfo() config.HttpRouterInfo {
	return cfg.routerInfo
}

func (cfg *DataEndpointConfig) WithExcludedKeyspaces(ksExcluded []string) *DataEndpointConfig {
	cfg.ksExcluded = ksExcluded
	return cfg
}

func (cfg *DataEndpointConfig) WithSchemaUpdateInterval(updateInterval time.Duration) *DataEndpointConfig {
	cfg.updateInterval = updateInterval
	return cfg
}

func (cfg *DataEndpointConfig) WithNaming(naming config.NamingConventionFn) *DataEndpointConfig {
	cfg.naming = naming
	return cfg
}

func (cfg *DataEndpointConfig) WithUseUserOrRoleAuth(useUserOrRowAuth bool) *DataEndpointConfig {
	cfg.useUserOrRoleAuth = useUserOrRowAuth
	return cfg
}

func (cfg *DataEndpointConfig) WithDbConfig(dbConfig db.Config) *DataEndpointConfig {
	cfg.dbConfig = dbConfig
	return cfg
}

// WithRouterInfo sets the http router information to be used for url parameters
func (cfg *DataEndpointConfig) WithRouterInfo(routerInfo config.HttpRouterInfo) *DataEndpointConfig {
	cfg.routerInfo = routerInfo
	return cfg
}

func (cfg DataEndpointConfig) NewEndpoint() (*DataEndpoint, error) {
	dbClient, err := db.NewDb(cfg.dbConfig, cfg.dbHosts...)
	if err != nil {
		return nil, err
	}
	return cfg.newEndpointWithDb(dbClient), nil
}

func (cfg DataEndpointConfig) newEndpointWithDb(dbClient *db.Db) *DataEndpoint {
	return &DataEndpoint{
		graphQLRouteGen: graphql.NewRouteGenerator(dbClient, cfg),
		restRouteGen:    rest.NewRouteGenerator(dbClient, cfg),
	}
}

type DataEndpoint struct {
	graphQLRouteGen *graphql.RouteGenerator
	restRouteGen    *rest.RouteGenerator
}

func NewEndpointConfig(hosts ...string) (*DataEndpointConfig, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}
	return NewEndpointConfigWithLogger(log.NewZapLogger(logger), hosts...), nil
}

func NewEndpointConfigWithLogger(logger log.Logger, hosts ...string) *DataEndpointConfig {
	return &DataEndpointConfig{
		dbHosts:        hosts,
		updateInterval: DefaultSchemaUpdateDuration,
		expireInterval: DefaultSchemaExpireDuration,
		naming:         config.NewDefaultNaming,
		logger:         logger,
		routerInfo:     config.DefaultRouterInfo(),
	}
}

func (e *DataEndpoint) RoutesGraphQL(pattern string) ([]types.Route, error) {
	return e.graphQLRouteGen.Routes(pattern, "")
}

func (e *DataEndpoint) RoutesKeyspaceGraphQL(pattern string, ksName string) ([]types.Route, error) {
	return e.graphQLRouteGen.Routes(pattern, ksName)
}

func (e *DataEndpoint) RoutesSchemaManagementGraphQL(pattern string, ops config.SchemaOperations) ([]types.Route, error) {
	return e.graphQLRouteGen.RoutesSchemaManagement(pattern, "", ops)
}

func (e *DataEndpoint) RoutesSchemaManagementKeyspaceGraphQL(pattern string, ksName string, ops config.SchemaOperations) ([]types.Route, error) {
	return e.graphQLRouteGen.RoutesSchemaManagement(pattern, ksName, ops)
}

// Keyspaces gets a slice of keyspace names that are considered by the endpoint when used in multi-keyspace mode.
func (e *DataEndpoint) Keyspaces() ([]string, error) {
	return e.graphQLRouteGen.Keyspaces()
}

func (e *DataEndpoint) RoutesRest(pattern string, operations config.SchemaOperations, singleKs string) []types.Route {
	return e.restRouteGen.Routes(pattern, operations, singleKs)
}
