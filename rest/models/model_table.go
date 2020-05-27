package models

type Table struct {
	Name              string             `json:"name,omitempty"`
	Keyspace          string             `json:"keyspace,omitempty"`
	ColumnDefinitions []ColumnDefinition `json:"columnDefinitions,omitempty"`
	PrimaryKey        *PrimaryKey        `json:"primaryKey,omitempty"`
	TableOptions      *TableOptions      `json:"tableOptions,omitempty"`
}
