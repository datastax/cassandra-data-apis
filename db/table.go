package db

import (
"fmt"
"github.com/gocql/gocql"
)

func (db *Db) CreateTable(name string) (bool, error) {
	return false, nil
}

func (db *Db) DropTable(ksName string, tableName string) (bool, error) {
	// TODO: Escape keyspace/table name?
	query := fmt.Sprintf("DROP TABLE %s.%s", ksName, tableName)
	err := db.ExecuteNoResult(query, gocql.Any)

	return err == nil, err
}
