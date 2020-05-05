package models

// ColumnUpdate changes the name of a primary key column and preserves the existing values.
type ColumnUpdate struct {

	// NewName is the new name of the column.
	NewName string `json:"newName" validate:"required"`
}
