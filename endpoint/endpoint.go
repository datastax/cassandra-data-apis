package endpoint

import (
	"github.com/riptano/data-endpoints/config"
	"github.com/riptano/data-endpoints/db"
	"github.com/riptano/data-endpoints/graphql"
	"github.com/riptano/data-endpoints/log"
	"go.uber.org/zap"
	"time"
)

const DefaultSchemaUpdateDuration = 10 * time.Second

type DataEndpointConfig struct {
	dbHosts           []string
	dbUsername        string
	dbPassword        string
	ksExcluded        []string
	updateInterval    time.Duration
	naming            config.NamingConventionFn
	supportedOps      config.Operations
	useUserOrRoleAuth bool
	logger            log.Logger
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

func (cfg DataEndpointConfig) SupportedOperations() config.Operations {
	return cfg.supportedOps
}

func (cfg DataEndpointConfig) UseUserOrRoleAuth() bool {
	return cfg.useUserOrRoleAuth
}

func (cfg DataEndpointConfig) Logger() log.Logger {
	return cfg.logger
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

func (cfg *DataEndpointConfig) WithSupportedOperations(supportedOps config.Operations) *DataEndpointConfig {
	cfg.supportedOps = supportedOps
	return cfg
}

func (cfg *DataEndpointConfig) WithUseUserOrRoleAuth(useUserOrRowAuth bool) *DataEndpointConfig {
	cfg.useUserOrRoleAuth = useUserOrRowAuth
	return cfg
}

func (cfg *DataEndpointConfig) WithDbUsername(dbUsername string) *DataEndpointConfig {
	cfg.dbUsername = dbUsername
	return cfg
}

func (cfg *DataEndpointConfig) WithDbPassword(dbPassword string) *DataEndpointConfig {
	cfg.dbPassword = dbPassword
	return cfg
}

func (cfg DataEndpointConfig) NewEndpoint() (*DataEndpoint, error) {
	dbClient, err := db.NewDb(cfg.dbUsername, cfg.dbPassword, cfg.dbHosts...)
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
	}
}

func (e *DataEndpoint) RoutesGraphQL(pattern string) ([]graphql.Route, error) {
	return e.graphQLRouteGen.Routes(pattern)
}

func (e *DataEndpoint) RoutesKeyspaceGraphQL(pattern string, ksName string) ([]graphql.Route, error) {
	return e.graphQLRouteGen.RoutesKeyspace(pattern, ksName)
}
