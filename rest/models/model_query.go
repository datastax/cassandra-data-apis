package models

type Query struct {
	ColumnNames []string              `json:"columnNames,omitempty"`
	Filters     []Filter              `validate:"required"`
	OrderBy     *ClusteringExpression `json:"orderBy,omitempty"`
	PageSize    int                   `json:"pageSize,omitempty"`
	PageState   string                `json:"pageState,omitempty"`
}

type Filter struct {
	ColumnName string        `validate:"required"`
	Operator   string        `validate:"required,oneof=eq notEq gt gte lt lte in"`
	Value      []interface{} `validate:"required"`
}
