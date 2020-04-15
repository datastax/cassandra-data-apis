package config

import (
	"github.com/datastax/cassandra-data-apis/log"
	"time"
)

type Config interface {
	ExcludedKeyspaces() []string
	SchemaUpdateInterval() time.Duration
	Naming() NamingConventionFn
	UseUserOrRoleAuth() bool
	Logger() log.Logger
}
