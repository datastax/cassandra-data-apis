package config

import (
	"fmt"
	"github.com/datastax/cassandra-data-apis/log"
	"github.com/gocql/gocql"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"time"
)

var SystemKeyspaces = []string{
	"system", "system_auth", "system_distributed", "system_schema", "system_traces", "system_views", "system_virtual_schema",
	"dse_insights", "dse_insights_local", "dse_leases", "dse_perf", "dse_security", "dse_system", "dse_system_local",
	"solr_admin", "OpsCenter", "dse_analytics", "system_backups", "dsefs",
}

type Config interface {
	ExcludedKeyspaces() []string
	SchemaUpdateInterval() time.Duration
	SchemaExpireInterval() time.Duration
	Naming() NamingConventionFn
	UseUserOrRoleAuth() bool
	Logger() log.Logger
	RouterInfo() HttpRouterInfo
}

type UrlParamGetter func(*http.Request, string) string

// UrlPattern determines how parameters are represented in the url
// For example: "/graphql/:param1" (colon, default) or "/graphql/{param1}" (brackets)
type UrlPattern int

type HttpRouterInfo interface {
	UrlPattern() UrlPattern
	UrlParams() UrlParamGetter
}

const (
	DefaultPageSize               = 100
	DefaultConsistencyLevel       = gocql.LocalQuorum
	DefaultSerialConsistencyLevel = gocql.Serial
)

const (
	UrlPatternColon UrlPattern = iota
	UrlPatternBrackets
)

type routerInfo struct {
	urlPattern     UrlPattern
	urlParamGetter UrlParamGetter
}

func (r *routerInfo) UrlPattern() UrlPattern {
	return r.urlPattern
}

func (r *routerInfo) UrlParams() UrlParamGetter {
	return r.urlParamGetter
}

func DefaultRouterInfo() HttpRouterInfo {
	return &routerInfo{
		urlPattern: UrlPatternColon,
		urlParamGetter: func(r *http.Request, name string) string {
			params := httprouter.ParamsFromContext(r.Context())
			return params.ByName(name)
		},
	}
}

func (p UrlPattern) UrlPathFormat(format string, parameterNames ...string) string {
	return fmt.Sprintf(format, p.formatParameters(parameterNames)...)
}

func (p UrlPattern) formatParameters(names []string) []interface{} {
	switch p {
	case UrlPatternColon:
		return formatParametersWithColon(names)
	case UrlPatternBrackets:
		return formatParametersWithBrackets(names)
	default:
		panic("unexpected url pattern")
	}
}

func formatParametersWithColon(names []string) []interface{} {
	result := make([]interface{}, len(names))
	for i, value := range names {
		result[i] = ":" + value
	}
	return result
}

func formatParametersWithBrackets(names []string) []interface{} {
	result := make([]interface{}, len(names))
	for i, value := range names {
		result[i] = "{" + value + "}"
	}
	return result
}
