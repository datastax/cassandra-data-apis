package config

import (
	"github.com/riptano/data-endpoints/log"
	"time"
)

type Config interface {
	ExcludedKeyspaces() []string
	SchemaUpdateInterval() time.Duration
	Naming() NamingConventionFn
	UseUserOrRoleAuth() bool
	Logger() log.Logger
}
