package models

// Column is a column within a row to be added to a table
type Column struct {
	Name *string `validate:"required"`

	// The value to store in the column, can be either a literal or collection
	Value interface{} `validate:"required"`
}
