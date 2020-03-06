package db

import (
	"fmt"
	"github.com/gocql/gocql"
	"strings"
)

func (db *Db) Insert(columnNames []string, queryParams []interface{}, ksName string,
	table *gocql.TableMetadata) (interface{}, error) {
	placeholders := "?"
	for i := 1; i < len(columnNames); i++ {
		placeholders += ", ?"
	}

	query := fmt.Sprintf(
		"INSERT INTO %s.%s (%s) VALUES (%s)",
		ksName, table.Name, strings.Join(columnNames, ","), placeholders)

	err := db.ExecuteNoResult(query, gocql.LocalOne, queryParams...)
	return err == nil, err
}

func (db *Db) Delete(columnNames []string, queryParams []interface{}, ksName string,
	table *gocql.TableMetadata) (interface{}, error) {
	whereClause := columnNames[0] + " = ?"
	for i := 1; i < len(columnNames); i++ {
		whereClause += " AND " + columnNames[i] + " = ?"
	}

	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", ksName, table.Name, whereClause)
	err := db.ExecuteNoResult(query, gocql.LocalOne, queryParams...)
	return err == nil, err
}
