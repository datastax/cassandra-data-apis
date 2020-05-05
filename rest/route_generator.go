package rest

import (
	"github.com/datastax/cassandra-data-apis/config"
	"github.com/datastax/cassandra-data-apis/db"
	restEndpointV1 "github.com/datastax/cassandra-data-apis/rest/endpoint/v1"
	"github.com/datastax/cassandra-data-apis/types"
)

type RouteGenerator struct {
	dbClient *db.Db
	config   config.Config
}

func NewRouteGenerator(
	dbClient *db.Db,
	cfg config.Config,
) *RouteGenerator {
	return &RouteGenerator{
		dbClient: dbClient,
		config:   cfg,
	}
}

func (g *RouteGenerator) Routes(prefix string, operations config.SchemaOperations, singleKs string) []types.Route {
	return restEndpointV1.Routes(prefix, operations, singleKs, g.config, g.dbClient)
}
