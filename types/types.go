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
