package db

import (
	"encoding/hex"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/iancoleman/strcase"
	"github.com/riptano/data-endpoints/types"
	"reflect"
	"strings"
)

func mapScan(scanner gocql.Scanner, columns []gocql.ColumnInfo) (map[string]interface{}, error) {
	values := make([]interface{}, len(columns))

	for i := range values {
		typeInfo := columns[i].TypeInfo
		switch typeInfo.Type() {
		case gocql.TypeVarchar, gocql.TypeAscii, gocql.TypeInet, gocql.TypeText, gocql.TypeTimeUUID, gocql.TypeUUID,
			gocql.TypeBigInt, gocql.TypeCounter:
			values[i] = reflect.New(reflect.TypeOf(*new(*string))).Interface()
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
		}

		mapped[strcase.ToLowerCamel(column.Name)] = value
	}

	return mapped, nil
}

func (db *Db) Select(columnNames []string, queryParams []interface{}, ksName string,
	table *gocql.TableMetadata) (*types.QueryResult, error) {

	whereClause := buildWhereClause(columnNames)
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", ksName, table.Name, whereClause)

	iter := db.session.ExecuteIter(query, gocql.LocalOne, queryParams...)

	pageState := hex.EncodeToString(iter.PageState())
	columns := iter.Columns()
	scanner := iter.Scanner()

	values := make([]map[string]interface{}, 0)

	for scanner.Next() {
		row, err := mapScan(scanner, columns)
		if err != nil {
			return nil, err
		}
		values = append(values, row)
	}

	return &types.QueryResult{
		PageState: pageState,
		Values:    values,
	}, nil
}

func (db *Db) Insert(columnNames []string, queryParams []interface{}, ksName string,
	table *gocql.TableMetadata) (*types.ModificationResult, error) {
	placeholders := "?"
	for i := 1; i < len(columnNames); i++ {
		placeholders += ", ?"
	}

	query := fmt.Sprintf(
		"INSERT INTO %s.%s (%s) VALUES (%s)",
		ksName, table.Name, strings.Join(columnNames, ","), placeholders)

	err := db.session.Execute(query, gocql.LocalOne, queryParams...)

	return &types.ModificationResult{Applied: err == nil}, err
}

func (db *Db) Delete(columnNames []string, queryParams []interface{}, ksName string,
	table *gocql.TableMetadata) (*types.ModificationResult, error) {
	whereClause := buildWhereClause(columnNames)
	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", ksName, table.Name, whereClause)
	err := db.session.Execute(query, gocql.LocalOne, queryParams...)
	return &types.ModificationResult{Applied: err == nil}, err
}

func buildWhereClause(columnNames []string) string {
	whereClause := columnNames[0] + " = ?"
	for i := 1; i < len(columnNames); i++ {
		whereClause += " AND " + columnNames[i] + " = ?"
	}
	return whereClause
}
