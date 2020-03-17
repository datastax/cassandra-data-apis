package graphql

import (
	"encoding/json"
	"fmt"
	"github.com/graphql-go/graphql"
	"github.com/riptano/data-endpoints/db"
	"net/http"
	"path"
)

var systemKeyspaces = []string{
	"system", "system_auth", "system_distributed", "system_schema", "system_traces", "system_views", "system_virtual_schema",
	"dse_insights", "dse_insights_local", "dse_leases", "dse_perf", "dse_security", "dse_system", "dse_system_local",
	"solr_admin",
}

type Route struct {
	Method      string
	Pattern     string
	HandlerFunc http.HandlerFunc
}

type Config struct {
	ksExcluded []string
}

type requestBody struct {
	Query string `json:"query"`
}

func Routes(prefixPattern string, ksExcluded []string, db *db.Db) ([]Route, error) {
	ksNames, err := db.Keyspaces()
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve keyspace names: %s", err)
	}

	routes := make([]Route, 0, len(ksNames) + 1)

	ksManageRoutes, err := RoutesKeyspaceManagement(prefixPattern, db)
	if err != nil {
		return nil, err
	}
	routes = append(routes, ksManageRoutes...)

	for _, ksName := range ksNames {
		if isKeyspaceExcluded(ksName, systemKeyspaces) || isKeyspaceExcluded(ksName, ksExcluded) {
			continue
		}
		ksRoutes, err := RoutesKeyspace(path.Join(prefixPattern, ksName), ksName, db)
		if err != nil {
			return nil, err
		}
		routes = append(routes, ksRoutes...)
	}

	return routes, nil
}

func RoutesKeyspaceManagement(pattern string, db *db.Db) ([]Route, error) {
	schema, err := BuildKeyspaceSchema(db)
	if err != nil {
		return nil, fmt.Errorf("unable to build graphql schema for keyspace management: %s", err)
	}
	return routesForSchema(pattern, schema), nil
}

func RoutesKeyspace(pattern string, ksName string, db *db.Db) ([]Route, error) {
	schema, err := BuildSchema(ksName, db)
	if err != nil {
		return nil, fmt.Errorf("unable to build graphql schema for keyspace '%s': %s", ksName, err)
	}
	return routesForSchema(pattern, schema), nil
}

func isKeyspaceExcluded(ksName string, ksExcluded []string) bool {
	for _, excluded := range ksExcluded {
		if ksName == excluded {
			return true
		}
	}
	return false
}

func routesForSchema(pattern string, schema graphql.Schema) []Route {
	return []Route {
		{
			Method: http.MethodGet,
			Pattern: pattern,
			HandlerFunc: func(w http.ResponseWriter, r *http.Request) {
				result := executeQuery(r.URL.Query().Get("query"), schema)
				json.NewEncoder(w).Encode(result)
			},
		},
		{
			Method: http.MethodPost,
			Pattern: pattern,
			HandlerFunc: func(w http.ResponseWriter, r *http.Request) {
				if r.Body == nil {
					http.Error(w, "No request body", 400)
					return
				}

				var body requestBody
				err := json.NewDecoder(r.Body).Decode(&body)
				if err != nil {
					http.Error(w, "Request body is invalid", 400)
					return
				}

				result := executeQuery(body.Query, schema)
				json.NewEncoder(w).Encode(result)
			},
		},
	}
}

func executeQuery(query string, schema graphql.Schema) *graphql.Result {
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
	})
	if len(result.Errors) > 0 {
		fmt.Printf("wrong result, unexpected errors: %v", result.Errors)
	}
	return result
}
