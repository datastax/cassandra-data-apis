package config

import (
	"github.com/riptano/data-endpoints/log"
	"time"
)

type Config interface {
	ExcludedKeyspaces() []string
	SchemaUpdateInterval() time.Duration
	Naming() NamingConventionFn
	SupportedOperations() Operations
	UseUserOrRoleAuth() bool
	Logger() log.Logger
}
