package models

// RowsUpdate defines an update operation on rows within a table.
type RowsUpdate struct {
	Changeset []Changeset `json:"changeset" validate:"required"`
}

// Changeset is a column and associated value to be used when updating a row.
type Changeset struct {
	// The name of the column to be updated.
	Column string `json:"column" validate:"required"`

	// The value for the column that will be updated for all matching rows.
	Value interface{} `json:"value" validate:"required"`
}
