package models

type Query struct {
	ColumnNames []string              `json:"columnNames,omitempty"`
	Filters     []Filter              `json:"filters" validate:"required"`
	OrderBy     *ClusteringExpression `json:"orderBy,omitempty"`
	PageSize    int                   `json:"pageSize,omitempty"`
	PageState   string                `json:"pageState,omitempty"`
}

type Filter struct {
	ColumnName string        `json:"columnName" validate:"required"`
	Operator   string        `json:"operator" validate:"required,oneof=eq notEq gt gte lt lte in"`
	Value      []interface{} `json:"value" validate:"required"`
}
