package models

// TableAdd defines the table to be added to an existing keyspace
type TableAdd struct {
	Name string `validate:"required"`

	// Attempting to create an existing table returns an error unless the IF NOT EXISTS option is used. If the option is
	// used, the statement if a no-op is the table already exists.
	IfNotExists bool `json:"ifNotExists,omitempty"`

	ColumnDefinitions []ColumnDefinition `json:"columnDefinitions,omitempty"`

	// Defines a column list for the primary key. Can be either a single column, compound primary key, or composite partition key.
	PrimaryKey *PrimaryKey `validate:"required"`

	TableOptions *TableOptions `json:"tableOptions,omitempty"`
}

// PrimaryKey defines a column list for the primary key. Can be either a single column, compound primary key, or composite partition
// key. Provide multiple columns for the partition key to define a composite partition key.
type PrimaryKey struct {

	// The column(s) that will constitute the partition key.
	PartitionKey []string `validate:"required"`

	// The column(s) that will constitute the clustering key.
	ClusteringKey []string `json:"clusteringKey,omitempty"`
}

// TableOptions are various properties that tune data handling, including I/O operations, compression, and compaction.
type TableOptions struct {

	// TTL (Time To Live) in seconds, where zero is disabled. The maximum configurable value is 630720000 (20 years). If
	// the value is greater than zero, TTL is enabled for the entire table and an expiration timestamp is added to each
	// column. A new TTL timestamp is calculated each time the data is updated and the row is removed after all the data expires.
	DefaultTimeToLive *int32 `validate:"gte=0,lte=630720000"`

	ClusteringExpression []ClusteringExpression `json:"clusteringExpression,omitempty"`
}

// ClusteringExpression allows for ordering rows so that storage is able to make use of the on-disk sorting of columns. Specifying
// order can make query results more efficient.
type ClusteringExpression struct {
	Column *string `validate:"required"`
	Order  *string `validate:"required"`
}
