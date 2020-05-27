// types package contains the public API types
// that are shared between both REST and GraphQL
package types

import "net/http"

type ModificationResult struct {
	Applied bool                   `json:"applied"`
	Value   map[string]interface{} `json:"value"`
}

type QueryResult struct {
	PageState string                   `json:"pageState"`
	Values    []map[string]interface{} `json:"values"`
}

type QueryOptions struct {
	PageState         string `json:"pageState"`
	PageSize          int    `json:"pageSize"`
	Limit             int    `json:"limit"`
	Consistency       int    `json:"consistency"`
	SerialConsistency int    `json:"serialConsistency"`
}

type MutationOptions struct {
	TTL               int `json:"ttl"`
	Consistency       int `json:"consistency"`
	SerialConsistency int `json:"serialConsistency"`
}

type ConditionItem struct {
	Column   string      `json:"column"` // json representation using for error information only
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

// CqlOperators contains the CQL operator for a given "graphql" operator
var CqlOperators = map[string]string{
	"eq":    "=",
	"notEq": "!=",
	"gt":    ">",
	"gte":   ">=",
	"lt":    "<",
	"lte":   "<=",
	"in":    "IN",
}

// Route represents a request route to be served
type Route struct {
	Method  string
	Pattern string
	Handler http.Handler
}
