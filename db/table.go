package db

import (
	"fmt"
	"github.com/gocql/gocql"
)

type CreateTableInfo struct {
	Keyspace string
	Table string
	PartitionKeys []*gocql.ColumnMetadata
	ClusteringKeys []*gocql.ColumnMetadata
	Values []*gocql.ColumnMetadata
}

type DropTableInfo struct {
	Keyspace string
	Table string
}

func (db *Db) CreateTable(info* CreateTableInfo, options *QueryOptions) (bool, error) {

	columns := ""
	primaryKeys := ""
	clusteringOrder := ""

	for _, c := range info.PartitionKeys {
		columns += fmt.Sprintf("%s %s, ", c.Name, c.Type)
		if len(primaryKeys) > 0 {
			primaryKeys += ", "
		}
		primaryKeys += c.Name
	}

	if info.ClusteringKeys != nil {
		primaryKeys = fmt.Sprintf("(%s)", primaryKeys)

		for _, c := range info.ClusteringKeys {
			columns += fmt.Sprintf("%s %s, ", c.Name, c.Type)
			primaryKeys += fmt.Sprintf(", %s", c.Name)
			if len(clusteringOrder) > 0 {
				clusteringOrder += ", "
			}
			order := c.ClusteringOrder
			if order == "" {
				order = "ASC"
			}
			clusteringOrder += fmt.Sprintf("%s %s", c.Name, order)
		}
	}

	if info.Values != nil {
		for _, c := range info.Values {
			columns += fmt.Sprintf("%s %s, ", c.Name, c.Type)
		}
	}

	query := fmt.Sprintf("CREATE TABLE %s.%s (%sPRIMARY KEY (%s))", info.Keyspace, info.Table, columns, primaryKeys)

	if clusteringOrder != "" {
		query += fmt.Sprintf(" WITH CLUSTERING ORDER BY (%s)", clusteringOrder)
	}

	err := db.session.Execute(query, options)
	return err == nil, err
}

func (db *Db) DropTable(info* DropTableInfo, options *QueryOptions) (bool, error) {
	// TODO: Escape keyspace/table name?
	query := fmt.Sprintf("DROP TABLE %s.%s", info.Table, info.Keyspace)
	err := db.session.Execute(query, options)
	return err == nil, err
}
