package models

// RowAdd defines a row to be added to a table
type RowAdd struct {
	Columns []Column `validate:"required"`
}
