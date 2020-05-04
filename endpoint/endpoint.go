package endpoint

import (
	"github.com/datastax/cassandra-data-apis/config"
	"github.com/datastax/cassandra-data-apis/db"
	"github.com/datastax/cassandra-data-apis/graphql"
	"github.com/datastax/cassandra-data-apis/log"
	"go.uber.org/zap"
	"time"
)

const DefaultSchemaUpdateDuration = 10 * time.Second

type DataEndpointConfig struct {
	dbConfig          db.Config
	dbHosts           []string
	ksExcluded        []string
	updateInterval    time.Duration
	naming            config.NamingConventionFn
	useUserOrRoleAuth bool
	logger            log.Logger
	urlPattern        config.UrlPattern
}

func (cfg DataEndpointConfig) ExcludedKeyspaces() []string {
	return cfg.ksExcluded
}

func (cfg DataEndpointConfig) SchemaUpdateInterval() time.Duration {
	return cfg.updateInterval
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

func (cfg DataEndpointConfig) UrlPattern() config.UrlPattern {
	return cfg.urlPattern
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

// WithUrlPattern sets the url pattern to be use to separate url parameters
// For example: "/graphql/:param1" (colon, default) or "/graphql/{param1}" (brackets)
func (cfg *DataEndpointConfig) WithUrlPattern(pattern config.UrlPattern) *DataEndpointConfig {
	cfg.urlPattern = pattern
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
	}
}

type DataEndpoint struct {
	graphQLRouteGen *graphql.RouteGenerator
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
		naming:         config.NewDefaultNaming,
		logger:         logger,
		urlPattern:     config.UrlPatternColon,
	}
}

func (e *DataEndpoint) RoutesGraphQL(pattern string) ([]graphql.Route, error) {
	return e.graphQLRouteGen.Routes(pattern, "")
}

func (e *DataEndpoint) RoutesKeyspaceGraphQL(pattern string, ksName string) ([]graphql.Route, error) {
	return e.graphQLRouteGen.Routes(pattern, ksName)
}

func (e *DataEndpoint) RoutesSchemaManagementGraphQL(pattern string, ops config.SchemaOperations) ([]graphql.Route, error) {
	return e.graphQLRouteGen.RoutesSchemaManagement(pattern, ops)
}

// Keyspaces gets a slice of keyspace names that are considered by the endpoint when used in multi-keyspace mode.
func (e *DataEndpoint) Keyspaces() ([]string, error) {
	return e.graphQLRouteGen.Keyspaces()
}
