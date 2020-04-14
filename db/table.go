package db

import (
	"fmt"
	"github.com/gocql/gocql"
)

type CreateTableInfo struct {
	Keyspace       string
	Table          string
	PartitionKeys  []*gocql.ColumnMetadata
	ClusteringKeys []*gocql.ColumnMetadata
	Values         []*gocql.ColumnMetadata
}

type AlterTableAddInfo struct {
	Keyspace string
	Table    string
	ToAdd    []*gocql.ColumnMetadata
}

type AlterTableDropInfo struct {
	Keyspace string
	Table    string
	ToDrop   []string
}

type DropTableInfo struct {
	Keyspace string
	Table    string
}

func (db *Db) CreateTable(info *CreateTableInfo, options *QueryOptions) (bool, error) {
	columns := ""
	primaryKeys := ""
	clusteringOrder := ""

	for _, c := range info.PartitionKeys {
		columns += fmt.Sprintf(`"%s" %s, `, c.Name, c.Type)
		primaryKeys += fmt.Sprintf(`, "%s"`, c.Name)
	}

	if info.ClusteringKeys != nil {
		primaryKeys = fmt.Sprintf("(%s)", primaryKeys)

		for _, c := range info.ClusteringKeys {
			columns += fmt.Sprintf(`"%s" %s, `, c.Name, c.Type)
			primaryKeys += fmt.Sprintf(`, "%s"`, c.Name)
			order := c.ClusteringOrder
			if order == "" {
				order = "ASC"
			}
			clusteringOrder += fmt.Sprintf(`, "%s" %s`, c.Name, order)
		}
	}

	if info.Values != nil {
		for _, c := range info.Values {
			columns += fmt.Sprintf(`"%s" %s, `, c.Name, c.Type)
		}
	}

	query := fmt.Sprintf(`CREATE TABLE "%s"."%s" (%sPRIMARY KEY (%s))`, info.Keyspace, info.Table, columns, primaryKeys[2:])

	if clusteringOrder != "" {
		query += fmt.Sprintf(" WITH CLUSTERING ORDER BY (%s)", clusteringOrder[2:])
	}

	err := db.session.Execute(query, options)
	return err == nil, err
}

func (db *Db) AlterTableAdd(info *AlterTableAddInfo, options *QueryOptions) (bool, error) {
	columns := ""
	for _, c := range info.ToAdd {
		columns += fmt.Sprintf(`, "%s" %s`, c.Name, c.Type)
	}
	query := fmt.Sprintf(`ALTER TABLE "%s"."%s" ADD(%s)`, info.Keyspace, info.Table, columns[2:])
	err := db.session.Execute(query, options)
	return err == nil, err
}

func (db *Db) AlterTableDrop(info *AlterTableDropInfo, options *QueryOptions) (bool, error) {
	columns := ""
	for _, column := range info.ToDrop {
		columns += fmt.Sprintf(`, "%s"`, column)
	}
	query := fmt.Sprintf(`ALTER TABLE "%s"."%s" DROP %s`, info.Keyspace, info.Table, columns[2:])
	err := db.session.Execute(query, options)
	return err == nil, err
}

func (db *Db) DropTable(info *DropTableInfo, options *QueryOptions) (bool, error) {
	query := fmt.Sprintf(`DROP TABLE "%s"."%s"`, info.Keyspace, info.Table)
	err := db.session.Execute(query, options)
	return err == nil, err
}
