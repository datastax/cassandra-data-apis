package config

import (
	"github.com/datastax/cassandra-data-apis/log"
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
	Naming() NamingConventionFn
	UseUserOrRoleAuth() bool
	Logger() log.Logger
	UrlPattern() UrlPattern
}

type UrlPattern int

const (
	UrlPatternColon UrlPattern = iota
	UrlPatternBrackets
)
