// types package contains the public API types
// that are shared between both REST and GraphQL
package types

type ModificationResult struct {
	Applied bool                   `json:"applied"`
	Value   map[string]interface{} `json:"value"`
}

type QueryResult struct {
	PageState string                   `json:"pageState"`
	Values    []map[string]interface{} `json:"values"`
}

type QueryOptions struct {
	PageState string `json:"pageState"`
	PageSize  int    `json:"pageSize"`
	Limit     int    `json:"pageState"`
}

type OperatorAndValue struct {
	Operator string
	Value    interface{}
}
