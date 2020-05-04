package endpoint

import (
	"github.com/datastax/cassandra-data-apis/graphql"
	"github.com/datastax/cassandra-data-apis/log"
	"net/http"

	"github.com/datastax/cassandra-data-apis/rest/db"
)

// Route describes how to route an endpoint
type routeList struct {
	dbConn *db.DatabaseConnection
	logger log.Logger
	params func(*http.Request, string) string
}

// Routes returns a slice of all the endpoint routes
func Routes(dbConn *db.DatabaseConnection) []graphql.Route {
	rl := routeList{dbConn: dbConn}

	routes := []graphql.Route{
		{
			Method:  http.MethodGet,
			Pattern: "/v1/keyspaces/{keyspaceName}/tables/{tableName}/columns",
			Handler: http.HandlerFunc(rl.GetColumns),
		},
		{
			Method:  http.MethodPost,
			Pattern: "/v1/keyspaces/{keyspaceName}/tables/{tableName}/columns",
			Handler: http.HandlerFunc(rl.AddColumn),
		},
		{
			Method:  http.MethodGet,
			Pattern: "/v1/keyspaces/{keyspaceName}/tables/{tableName}/columns/{columnName}",
			Handler: http.HandlerFunc(rl.GetColumn),
		},
		{
			Method:  http.MethodPut,
			Pattern: "/v1/keyspaces/{keyspaceName}/tables/{tableName}/columns/{columnName}",
			Handler: http.HandlerFunc(rl.UpdateColumn),
		},
		{
			Method:  http.MethodDelete,
			Pattern: "/v1/keyspaces/{keyspaceName}/tables/{tableName}/columns/{columnName}",
			Handler: http.HandlerFunc(rl.DeleteColumn),
		},
		{
			Method:  http.MethodGet,
			Pattern: "/v1/keyspaces/{keyspaceName}/tables/{tableName}/rows/{rowIdentifier}",
			Handler: http.HandlerFunc(rl.GetRow),
		},
		{
			Method:  http.MethodPost,
			Pattern: "/v1/keyspaces/{keyspaceName}/tables/{tableName}/rows",
			Handler: http.HandlerFunc(rl.AddRow),
		},
		{
			Method:  http.MethodPost,
			Pattern: "/v1/keyspaces/{keyspaceName}/tables/{tableName}/rows/query",
			Handler: http.HandlerFunc(rl.Query),
		},
		{
			Method:  http.MethodPut,
			Pattern: "/v1/keyspaces/{keyspaceName}/tables/{tableName}/rows/{rowIdentifier}",
			Handler: http.HandlerFunc(rl.UpdateRow),
		},
		{
			Method:  http.MethodDelete,
			Pattern: "/v1/keyspaces/{keyspaceName}/tables/{tableName}/rows/{rowIdentifier}",
			Handler: http.HandlerFunc(rl.DeleteRow),
		},
		{
			Method:  http.MethodGet,
			Pattern: "/v1/keyspaces/{keyspaceName}/tables",
			Handler: http.HandlerFunc(rl.GetTables),
		},
		{
			Method:  http.MethodGet,
			Pattern: "/v1/keyspaces/{keyspaceName}/tables/{tableName}",
			Handler: http.HandlerFunc(rl.GetTable),
		},
		{
			Method:  http.MethodPost,
			Pattern: "/v1/keyspaces/{keyspaceName}/tables",
			Handler: http.HandlerFunc(rl.AddTable),
		},
		{
			Method:  http.MethodDelete,
			Pattern: "/v1/keyspaces/{keyspaceName}/tables/{tableName}",
			Handler: http.HandlerFunc(rl.DeleteTable),
		},
		{
			Method:  http.MethodGet,
			Pattern: "/v1/keyspaces",
			Handler: http.HandlerFunc(rl.GetKeyspaces),
		},
	}
	return routes
}
