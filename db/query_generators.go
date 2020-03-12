package db

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/iancoleman/strcase"
	"reflect"
	"strings"
)

func mapScan(scanner gocql.Scanner, columns []gocql.ColumnInfo) (map[string]interface{}, error) {
	values := make([]interface{}, len(columns))

	for i := range values {
		typeInfo := columns[i].TypeInfo
		switch typeInfo.Type() {
		case gocql.TypeVarchar, gocql.TypeAscii, gocql.TypeInet, gocql.TypeText:
			values[i] = reflect.New(reflect.TypeOf(*new(*string))).Interface()
		case gocql.TypeBigInt, gocql.TypeCounter:
			values[i] = reflect.New(reflect.TypeOf(*new(*int64))).Interface()
		case gocql.TypeBoolean:
			values[i] = reflect.New(reflect.TypeOf(*new(*bool))).Interface()
		case gocql.TypeFloat:
			values[i] = reflect.New(reflect.TypeOf(*new(*float32))).Interface()
		case gocql.TypeDouble:
			values[i] = reflect.New(reflect.TypeOf(*new(*float64))).Interface()
		case gocql.TypeInt:
			values[i] = reflect.New(reflect.TypeOf(*new(*int))).Interface()
		case gocql.TypeSmallInt:
			values[i] = reflect.New(reflect.TypeOf(*new(*int16))).Interface()
		case gocql.TypeTinyInt:
			values[i] = reflect.New(reflect.TypeOf(*new(*int8))).Interface()
		default:
			values[i] = columns[i].TypeInfo.New()
		}
	}

	if err := scanner.Scan(values...); err != nil {
		return nil, err
	}

	mapped := make(map[string]interface{}, len(values))
	for i, column := range columns {
		value := values[i]
		switch column.TypeInfo.Type() {
		case gocql.TypeVarchar, gocql.TypeAscii, gocql.TypeInet, gocql.TypeText,
			gocql.TypeBigInt, gocql.TypeInt, gocql.TypeSmallInt, gocql.TypeTinyInt,
			gocql.TypeCounter, gocql.TypeBoolean,
			gocql.TypeFloat, gocql.TypeDouble:
			value = reflect.Indirect(reflect.ValueOf(value)).Interface()
		default:
			mapped[strcase.ToLowerCamel(column.Name)] = value
		}
	}

	return mapped, nil
}

func (db *Db) Select(columnNames []string, queryParams []interface{}, ksName string,
	table *gocql.TableMetadata) ([]map[string]interface{}, error) {

	whereClause := buildWhereClause(columnNames)
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", ksName, table.Name, whereClause)

	iter := db.Execute(query, gocql.LocalOne, queryParams...)

	columns := iter.Columns()
	scanner := iter.Scanner()

	results := make([]map[string]interface{}, 0)

	for scanner.Next() {
		row, err := mapScan(scanner, columns)
		if err != nil {
			return nil, err
		}
		results = append(results, row)
	}

	return results, nil
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
