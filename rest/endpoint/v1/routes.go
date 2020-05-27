package endpoint

import (
	"github.com/datastax/cassandra-data-apis/config"
	"github.com/datastax/cassandra-data-apis/db"
	"github.com/datastax/cassandra-data-apis/log"
	"github.com/datastax/cassandra-data-apis/types"
	"net/http"
	"path"
)

const (
	keyspaceParam = "keyspaceName"
	tableParam    = "tableName"
)

const (
	KeyspacesPathFormat    = "v1/keyspaces"
	TablesPathFormat       = "v1/keyspaces/%s/tables"
	TableSinglePathFormat  = "v1/keyspaces/%s/tables/%s"
	ColumnsPathFormat      = "v1/keyspaces/%s/tables/%s/columns"
	ColumnSinglePathFormat = "v1/keyspaces/%s/tables/%s/columns/%s"
	RowsPathFormat         = "v1/keyspaces/%s/tables/%s/rows"
	RowSinglePathFormat    = "v1/keyspaces/%s/tables/%s/rows/%s"
	QueryPathFormat        = "v1/keyspaces/%s/tables/%s/rows/query"
)

// routeList describes how to route an endpoint
type routeList struct {
	logger            log.Logger
	params            config.UrlParamGetter
	dbClient          *db.Db
	operations        config.SchemaOperations
	excludedKeyspaces map[string]bool
	singleKeyspace    string
}

// Routes returns a slice of all the REST endpoint routes
func Routes(prefix string, operations config.SchemaOperations, singleKeyspace string, cfg config.Config, dbClient *db.Db) []types.Route {
	excludedKeyspaces := make(map[string]bool)
	for _, ks := range cfg.ExcludedKeyspaces() {
		excludedKeyspaces[ks] = true
	}

	rl := routeList{
		logger:            cfg.Logger(),
		params:            cfg.RouterInfo().UrlParams(),
		dbClient:          dbClient,
		operations:        operations,
		excludedKeyspaces: excludedKeyspaces,
		singleKeyspace:    singleKeyspace,
	}

	urlPattern := cfg.RouterInfo().UrlPattern()

	urlKeyspaces := url(prefix, urlPattern, KeyspacesPathFormat)
	urlTables := url(prefix, urlPattern, TablesPathFormat, keyspaceParam)
	urlSingleTable := url(prefix, urlPattern, TableSinglePathFormat, keyspaceParam, tableParam)
	urlColumns := url(prefix, urlPattern, ColumnsPathFormat, keyspaceParam, tableParam)
	urlSingleColumn := url(prefix, urlPattern, ColumnSinglePathFormat, keyspaceParam, tableParam, "columnName")
	urlRows := url(prefix, urlPattern, RowsPathFormat, keyspaceParam, tableParam)
	urlSingleRow := url(prefix, urlPattern, RowSinglePathFormat, keyspaceParam, tableParam, "rowIdentifier")
	urlQuery := url(prefix, urlPattern, QueryPathFormat, keyspaceParam, tableParam)

	routes := []types.Route{
		{
			Method:  http.MethodGet,
			Pattern: urlColumns,
			Handler: rl.validateKeyspace(rl.GetColumns),
		},
		{
			Method:  http.MethodPost,
			Pattern: urlColumns,
			Handler: rl.validateKeyspace(rl.isSupported(config.TableAlterAdd, rl.AddColumn)),
		},
		{
			Method:  http.MethodDelete,
			Pattern: urlSingleColumn,
			Handler: rl.validateKeyspace(rl.isSupported(config.TableAlterDrop, rl.DeleteColumn)),
		},
		{
			Method:  http.MethodGet,
			Pattern: urlSingleColumn,
			Handler: rl.validateKeyspace(rl.GetColumn),
		},
		{
			Method:  http.MethodPost,
			Pattern: urlRows,
			Handler: rl.validateKeyspace(rl.AddRow),
		},
		{
			Method:  http.MethodGet,
			Pattern: urlSingleRow,
			Handler: rl.validateKeyspace(rl.GetRow),
		},
		{
			Method:  http.MethodPut,
			Pattern: urlSingleRow,
			Handler: rl.validateKeyspace(rl.UpdateRow),
		},
		{
			Method:  http.MethodDelete,
			Pattern: urlSingleRow,
			Handler: rl.validateKeyspace(rl.DeleteRow),
		},
		{
			Method:  http.MethodPost,
			Pattern: urlQuery,
			Handler: rl.validateKeyspace(rl.Query),
		},
		{
			Method:  http.MethodGet,
			Pattern: urlTables,
			Handler: rl.validateKeyspace(rl.GetTables),
		},
		{
			Method:  http.MethodPost,
			Pattern: urlTables,
			Handler: rl.validateKeyspace(rl.isSupported(config.TableCreate, rl.AddTable)),
		},
		{
			Method:  http.MethodGet,
			Pattern: urlSingleTable,
			Handler: rl.validateKeyspace(rl.GetTable),
		},
		{
			Method:  http.MethodDelete,
			Pattern: urlSingleTable,
			Handler: rl.validateKeyspace(rl.isSupported(config.TableDrop, rl.DeleteTable)),
		},
		{
			Method:  http.MethodGet,
			Pattern: urlKeyspaces,
			Handler: http.HandlerFunc(rl.GetKeyspaces),
		},
	}

	return routes
}

func url(prefix string, urlPattern config.UrlPattern, format string, parameterNames ...string) string {
	return path.Join(prefix, urlPattern.UrlPathFormat(format, parameterNames...))
}

func (s *routeList) validateKeyspace(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		keyspaceName := s.params(r, keyspaceParam)

		if s.singleKeyspace != "" && s.singleKeyspace != keyspaceName {
			// Only a single keyspace is allowed and it's not the provided one
			RespondWithKeyspaceNotAllowed(w)
			return
		}

		if s.excludedKeyspaces[keyspaceName] {
			RespondWithKeyspaceNotAllowed(w)
			return
		}

		next(w, r)
	}
}

func (s *routeList) isSupported(requiredOp config.SchemaOperations, handler http.HandlerFunc) http.HandlerFunc {
	if s.operations.IsSupported(requiredOp) {
		return handler
	}

	return forbiddenHandler
}

func forbiddenHandler(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}
