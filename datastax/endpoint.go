package datastax // TODO: Change package name?

import (
	"github.com/riptano/data-endpoints/db"
	"github.com/riptano/data-endpoints/graphql"
)

type DataEndpointConfig struct {
	Hosts []string
	ExcludedKeyspaces []string
}

type DataEndpoint struct {
	db  *db.Db
	cfg DataEndpointConfig
}

func NewConfig(hosts ...string) *DataEndpointConfig {
	return &DataEndpointConfig{
		Hosts:hosts,
	}
}

func (cfg *DataEndpointConfig) NewEndpoint() (*DataEndpoint, error) {
	db, err := db.NewDb(cfg.Hosts...)
	if err != nil {
		return nil, err
	}
	return &DataEndpoint{
		db: db,
		cfg: *cfg,
	}, nil
}

func (pnt *DataEndpoint) RoutesGql(pattern string) ([]graphql.Route, error) {
	return graphql.Routes(pattern, pnt.cfg.ExcludedKeyspaces, pnt.db)
}

func (pnt *DataEndpoint) RoutesKeyspaceGql(pattern string, ksName string) ([]graphql.Route, error) {
	return graphql.RoutesKeyspace(pattern, ksName, pnt.db)
}

