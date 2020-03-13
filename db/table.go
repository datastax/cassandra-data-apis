package db

import (
	"fmt"
	"github.com/gocql/gocql"
)

func (db *Db) CreateTable(
	ksName string, name string, partitionKeys []*gocql.ColumnMetadata,
	clusteringKeys []*gocql.ColumnMetadata, values []*gocql.ColumnMetadata) error {

	columns := ""
	primaryKeys := ""
	clusteringOrder := ""

	for _, c := range partitionKeys {
		columns += fmt.Sprintf("%s %s, ", c.Name, c.Type)
		if len(primaryKeys) > 0 {
			primaryKeys += ", "
		}
		primaryKeys += c.Name
	}

	if clusteringKeys != nil {
		primaryKeys = fmt.Sprintf("(%s)", primaryKeys)

		for _, c := range clusteringKeys {
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

	if values != nil {
		for _, c := range values {
			columns += fmt.Sprintf("%s %s, ", c.Name, c.Type)
		}
	}

	query := fmt.Sprintf("CREATE TABLE %s.%s (%sPRIMARY KEY (%s))", ksName, name, columns, primaryKeys)

	if clusteringOrder != "" {
		query += fmt.Sprintf(" WITH CLUSTERING ORDER BY (%s)", clusteringOrder)
	}

	return db.session.ExecuteSimple(query, gocql.Any)
}

func (db *Db) DropTable(ksName string, tableName string) (bool, error) {
	// TODO: Escape keyspace/table name?
	query := fmt.Sprintf("DROP TABLE %s.%s", ksName, tableName)
	err := db.session.ExecuteSimple(query, gocql.Any)

	return err == nil, err
}
