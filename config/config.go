package config

import "time"

type Config interface {
	ExcludedKeyspaces() []string
	SchemaUpdateInterval() time.Duration
	Naming() NamingConvention
	SupportedOperations() Operations
	UseUserOrRoleAuth() bool
}
