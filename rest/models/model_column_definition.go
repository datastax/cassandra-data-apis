package models

// ColumnDefinition defines a column to be added to a table
type ColumnDefinition struct {

	// Name is a unique name for the column.
	Name string `validate:"required"`

	// TypeDefinition defines the type of data allowed in the column
	TypeDefinition string `validate:"required,oneof=ascii text varchar tinyint smallint int bigint varint decimal float double date DateRangeType duration time timestamp uuid timeuuid blob boolean counter inet PointType LineStringType PolygonType frozen list map set tuple"`

	// Denotes that the column is shared by all rows of a partition
	Static bool `json:"static,omitempty"`
}
