package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/datastax/cassandra-data-apis/config"
	"github.com/datastax/cassandra-data-apis/db"
	"github.com/datastax/cassandra-data-apis/log"
	"github.com/datastax/cassandra-data-apis/types"
	"github.com/graphql-go/graphql"
	"net/http"
	"path"
	"regexp"
	"strings"
	"time"
)

type executeQueryFunc func(query string, urlPath string, ctx context.Context) *graphql.Result

type RouteGenerator struct {
	dbClient       *db.Db
	updateInterval time.Duration
	expireInterval time.Duration
	logger         log.Logger
	schemaGen      *SchemaGenerator
	routerInfo     config.HttpRouterInfo
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
		routerInfo:     cfg.RouterInfo(),
	}
}

func (rg *RouteGenerator) RoutesSchemaManagement(pattern string, singleKeyspace string, ops config.SchemaOperations) ([]types.Route, error) {
	schema, err := rg.schemaGen.BuildKeyspaceSchema(singleKeyspace, ops)
	if err != nil {
		return nil, fmt.Errorf("unable to build graphql schema for schema management: %s", err)
	}
	return routesForSchema(pattern, func(query string, urlPath string, ctx context.Context) *graphql.Result {
		return rg.executeQuery(query, ctx, schema)
	}), nil
}

func (rg *RouteGenerator) Routes(pattern string, singleKeyspace string) ([]types.Route, error) {
	updater, err := NewUpdater(rg.schemaGen, singleKeyspace, rg.updateInterval, rg.expireInterval, rg.logger)
	if err != nil {
		return nil, fmt.Errorf("unable to build graphql schema: %s", err)
	}

	go updater.Start()

	pathParser := getPathParser(pattern)
	if singleKeyspace == "" {
		// Use a single route with keyspace as dynamic parameter
		pattern = rg.routerInfo.UrlPattern().UrlPathFormat(path.Join(pattern, "%s"), "keyspace")
	}

	return routesForSchema(pattern, func(query string, urlPath string, ctx context.Context) *graphql.Result {
		ksName := singleKeyspace
		if ksName == "" {
			// Multiple keyspace support
			// The keyspace is part of the url path
			ksName = pathParser(urlPath)
			if ksName == "" {
				// Invalid url parameter
				return nil
			}
		}
		schema := updater.Schema(ksName)

		if schema == nil {
			// The keyspace was not found or is invalid
			return nil
		}

		return rg.executeQuery(query, ctx, *schema)
	}), nil
}

// Keyspaces gets a slice of keyspace names that are considered by the route generator.
func (rg *RouteGenerator) Keyspaces() ([]string, error) {
	keyspaces, err := rg.dbClient.Keyspaces("")

	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(keyspaces))
	for _, ksName := range keyspaces {
		if !rg.schemaGen.isKeyspaceExcluded(ksName) {
			result = append(result, ksName)
		}
	}

	return result, nil
}

func getPathParser(root string) func(string) string {
	if !strings.HasSuffix(root, "/") {
		root += "/"
	}
	regexString := fmt.Sprintf(`^%s([\w-]+)/?(?:\?.*)?$`, root)
	r := regexp.MustCompile(regexString)
	return func(urlPath string) string {
		subMatches := r.FindStringSubmatch(urlPath)
		if len(subMatches) != 2 {
			return ""
		}
		return subMatches[1]
	}
}

func routesForSchema(pattern string, execute executeQueryFunc) []types.Route {
	return []types.Route{
		{
			Method:  http.MethodGet,
			Pattern: pattern,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				result := execute(r.URL.Query().Get("query"), r.URL.Path, r.Context())
				if result == nil {
					// The execution function is signaling that it shouldn't be processing this request
					http.NotFound(w, r)
					return
				}
				err := json.NewEncoder(w).Encode(result)
				if err != nil {
					http.Error(w, "response could not be encoded: "+err.Error(), 500)
				}
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

				result := execute(body.Query, r.URL.Path, r.Context())
				if result == nil {
					// The execution function is signaling that it shouldn't be processing this request
					http.NotFound(w, r)
					return
				}

				err = json.NewEncoder(w).Encode(result)
				if err != nil {
					http.Error(w, "response could not be encoded: "+err.Error(), 500)
				}
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
