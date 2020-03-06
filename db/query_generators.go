package db

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/iancoleman/strcase"
	"strings"
)

func (db *Db) Select(columnNames []string, queryParams []interface{}, ksName string,
	table *gocql.TableMetadata) ([]map[string]interface{}, error) {

	whereClause := buildWhereClause(columnNames)
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", ksName, table.Name, whereClause)

	iter := db.Execute(query, gocql.LocalOne, queryParams...)

	results := make([]map[string]interface{}, 0)
	row := map[string]interface{}{}

	for iter.MapScan(row) {
		rowCamel := map[string]interface{}{}
		for k, v := range row {
			rowCamel[strcase.ToLowerCamel(k)] = v
		}
		results = append(results, rowCamel)
		row = map[string]interface{}{}
	}

	return results, iter.Close()
}

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
	whereClause := buildWhereClause(columnNames)
	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", ksName, table.Name, whereClause)
	err := db.ExecuteNoResult(query, gocql.LocalOne, queryParams...)
	return err == nil, err
}

func buildWhereClause(columnNames []string) string {
	whereClause := columnNames[0] + " = ?"
	for i := 1; i < len(columnNames); i++ {
		whereClause += " AND " + columnNames[i] + " = ?"
	}
	return whereClause
}
