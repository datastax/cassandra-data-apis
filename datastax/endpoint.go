package datastax // TODO: Change package name?

import (
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
}

type DataEndpoint struct {
	db  *db.Db
	cfg DataEndpointConfig
}

func NewConfig(hosts ...string) *DataEndpointConfig {
	return &DataEndpointConfig{
		DbHosts:              hosts,
		SchemaUpdateInterval: 10 * time.Second,
	}
}

func (cfg *DataEndpointConfig) NewEndpoint() (*DataEndpoint, error) {
	db, err := db.NewDb(cfg.DbUsername, cfg.DbPassword, cfg.DbHosts...)
	if err != nil {
		return nil, err
	}
	return &DataEndpoint{
		db:  db,
		cfg: *cfg,
	}, nil
}

func (pnt *DataEndpoint) RoutesGql(pattern string) ([]graphql.Route, error) {
	return graphql.Routes(pattern, pnt.cfg.ExcludedKeyspaces, pnt.db, pnt.cfg.SchemaUpdateInterval)
}

func (pnt *DataEndpoint) RoutesKeyspaceGql(pattern string, ksName string) ([]graphql.Route, error) {
	return graphql.RoutesKeyspace(pattern, ksName, pnt.db, pnt.cfg.SchemaUpdateInterval)
}
