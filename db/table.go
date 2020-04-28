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
	IfNotExists    bool
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
	IfExists bool
}

func toTypeString(info gocql.TypeInfo) string {
	if coll, ok := info.(gocql.CollectionType); ok {
		switch coll.Type() {
		case gocql.TypeList:
			fallthrough
		case gocql.TypeSet:
			return fmt.Sprintf("%s<%s>",
				coll.Type().String(), toTypeString(coll.Elem))
		case gocql.TypeMap:
			return fmt.Sprintf("%s<%s, %s>",
				coll.Type().String(), toTypeString(coll.Key), toTypeString(coll.Elem))
		}
	}
	return info.Type().String()
}

func (db *Db) CreateTable(info *CreateTableInfo, options *QueryOptions) error {
	columns := ""
	primaryKeys := ""
	clusteringOrder := ""

	for _, c := range info.PartitionKeys {
		columns += fmt.Sprintf(`"%s" %s, `, c.Name, toTypeString(c.Type))
		primaryKeys += fmt.Sprintf(`, "%s"`, c.Name)
	}

	if info.ClusteringKeys != nil {
		primaryKeys = fmt.Sprintf("(%s)", primaryKeys[2:])

		for _, c := range info.ClusteringKeys {
			columns += fmt.Sprintf(`"%s" %s, `, c.Name, toTypeString(c.Type))
			primaryKeys += fmt.Sprintf(`, "%s"`, c.Name)
			order := c.ClusteringOrder
			if order == "" {
				order = "ASC"
			}
			clusteringOrder += fmt.Sprintf(`, "%s" %s`, c.Name, order)
		}
	} else {
		primaryKeys = primaryKeys[2:]
	}

	if info.Values != nil {
		for _, c := range info.Values {
			columns += fmt.Sprintf(`"%s" %s, `, c.Name, toTypeString(c.Type))
		}
	}

	query := fmt.Sprintf(`CREATE TABLE %s"%s"."%s" (%sPRIMARY KEY (%s))`,
		ifNotExistsStr(info.IfNotExists), info.Keyspace, info.Table, columns, primaryKeys)

	if clusteringOrder != "" {
		query += fmt.Sprintf(" WITH CLUSTERING ORDER BY (%s)", clusteringOrder[2:])
	}

	return db.session.ChangeSchema(query, options)
}

func (db *Db) AlterTableAdd(info *AlterTableAddInfo, options *QueryOptions) error {
	columns := ""
	for _, c := range info.ToAdd {
		columns += fmt.Sprintf(`, "%s" %s`, c.Name, toTypeString(c.Type))
	}
	query := fmt.Sprintf(`ALTER TABLE "%s"."%s" ADD(%s)`, info.Keyspace, info.Table, columns[2:])
	return db.session.ChangeSchema(query, options)
}

func (db *Db) AlterTableDrop(info *AlterTableDropInfo, options *QueryOptions) error {
	columns := ""
	for _, column := range info.ToDrop {
		columns += fmt.Sprintf(`, "%s"`, column)
	}
	query := fmt.Sprintf(`ALTER TABLE "%s"."%s" DROP %s`, info.Keyspace, info.Table, columns[2:])
	return db.session.ChangeSchema(query, options)
}

func (db *Db) DropTable(info *DropTableInfo, options *QueryOptions) error {
	query := fmt.Sprintf(`DROP TABLE %s"%s"."%s"`, ifExistsStr(info.IfExists), info.Keyspace, info.Table)
	return db.session.ChangeSchema(query, options)
}
