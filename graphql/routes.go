package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/graphql-go/graphql"
	"github.com/riptano/data-endpoints/config"
	"github.com/riptano/data-endpoints/db"
	"github.com/riptano/data-endpoints/log"
	"net/http"
	"path"
	"time"
)

var systemKeyspaces = []string{
	"system", "system_auth", "system_distributed", "system_schema", "system_traces", "system_views", "system_virtual_schema",
	"dse_insights", "dse_insights_local", "dse_leases", "dse_perf", "dse_security", "dse_system", "dse_system_local",
	"solr_admin",
}

type executeQueryFunc func(query string, ctx context.Context) *graphql.Result

type RouteGenerator struct {
	dbClient       *db.Db
	updateInterval time.Duration
	logger         log.Logger
	schemaGen      *SchemaGenerator
}

type Route struct {
	Method  string
	Pattern string
	Handler http.Handler
}

type Config struct {
	ksExcluded []string
}

type RequestBody struct {
	Query string `json:"query"`
}

func NewRouteGenerator(dbClient *db.Db, cfg config.Config) *RouteGenerator {
	return &RouteGenerator{
		dbClient:       dbClient,
		updateInterval: cfg.SchemaUpdateInterval(),
		logger:         cfg.Logger(),
		schemaGen:      NewSchemaGenerator(dbClient, cfg),
	}
}

func (rg *RouteGenerator) Routes(prefixPattern string) ([]Route, error) {
	ksNames, err := rg.dbClient.Keyspaces()
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve keyspace names: %s", err)
	}

	routes := make([]Route, 0, len(ksNames)+1)

	ksManageRoutes, err := rg.RoutesKeyspaceManagement(prefixPattern)
	if err != nil {
		return nil, err
	}
	routes = append(routes, ksManageRoutes...)

	for _, ksName := range ksNames {
		if rg.schemaGen.isKeyspaceExcluded(ksName) {
			continue
		}
		ksRoutes, err := rg.RoutesKeyspace(path.Join(prefixPattern, ksName), ksName)
		if err != nil {
			return nil, err
		}
		routes = append(routes, ksRoutes...)
	}

	return routes, nil
}

func (rg *RouteGenerator) RoutesKeyspaceManagement(pattern string) ([]Route, error) {
	schema, err := rg.schemaGen.BuildKeyspaceSchema()
	if err != nil {
		return nil, fmt.Errorf("unable to build graphql schema for keyspace management: %s", err)
	}
	return routesForSchema(pattern, func(query string, ctx context.Context) *graphql.Result {
		return rg.executeQuery(query, ctx, schema)
	}), nil
}

func (rg *RouteGenerator) RoutesKeyspace(pattern string, ksName string) ([]Route, error) {
	updater, err := NewUpdater(rg.schemaGen, ksName, rg.updateInterval, rg.logger)
	if err != nil {
		return nil, fmt.Errorf("unable to build graphql schema for keyspace '%s': %s", ksName, err)
	}
	go updater.Start()
	return routesForSchema(pattern, func(query string, ctx context.Context) *graphql.Result {
		return rg.executeQuery(query, ctx, *updater.Schema())
	}), nil
}

func isKeyspaceExcluded(ksName string, ksExcluded []string) bool {
	for _, excluded := range ksExcluded {
		if ksName == excluded {
			return true
		}
	}
	return false
}

func routesForSchema(pattern string, execute executeQueryFunc) []Route {
	return []Route{
		{
			Method:  http.MethodGet,
			Pattern: pattern,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				result := execute(r.URL.Query().Get("query"), r.Context())
				json.NewEncoder(w).Encode(result)
			}),
		},
		{
			Method:  http.MethodPost,
			Pattern: pattern,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Body == nil {
					http.Error(w, "No request body", 400)
					return
				}

				var body RequestBody
				err := json.NewDecoder(r.Body).Decode(&body)
				if err != nil {
					http.Error(w, "Request body is invalid", 400)
					return
				}

				result := execute(body.Query, r.Context())
				json.NewEncoder(w).Encode(result)
			}),
		},
	}
}

func (rg *RouteGenerator) executeQuery(query string, ctx context.Context, schema graphql.Schema) *graphql.Result {
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       ctx,
	})
	if len(result.Errors) > 0 {
		rg.logger.Error("unexpected errors processing graphql query", "errors", result.Errors)
	}
	return result
}
