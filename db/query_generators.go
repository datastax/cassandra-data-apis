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

type SelectInfo struct {
	Keyspace string
	Table    string
	Columns  []string
	Values   []types.OperatorAndValue
	Options  *types.QueryOptions
	OrderBy  []ColumnOrder
}

type InsertInfo struct {
	Keyspace    string
	Table       string
	Columns     []string
	QueryParams []interface{}
	IfNotExists bool
	TTL         int
}

type DeleteInfo struct {
	Keyspace    string
	Table       string
	Columns     []string
	QueryParams []interface{}
	IfCondition map[string]interface{}
	IfExists    bool
}

type ColumnOrder struct {
	Column string
	Order  string
}

func mapScan(scanner gocql.Scanner, columns []gocql.ColumnInfo) (map[string]interface{}, error) {
	values := make([]interface{}, len(columns))

	for i := range values {
		typeInfo := columns[i].TypeInfo
		switch typeInfo.Type() {
		case gocql.TypeVarchar, gocql.TypeAscii, gocql.TypeInet, gocql.TypeText,
			gocql.TypeBigInt, gocql.TypeCounter:
			values[i] = new(*string)
		case gocql.TypeBoolean:
			values[i] = new(*bool)
		case gocql.TypeFloat:
			values[i] = new(*float32)
		case gocql.TypeDouble:
			values[i] = new(*float64)
		case gocql.TypeInt:
			values[i] = new(*int)
		case gocql.TypeSmallInt:
			values[i] = new(*int16)
		case gocql.TypeTinyInt:
			values[i] = new(*int8)
		case gocql.TypeTimeUUID, gocql.TypeUUID:
			values[i] = new(*gocql.UUID)
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
			gocql.TypeTimeUUID, gocql.TypeUUID,
			gocql.TypeFloat, gocql.TypeDouble:
			value = reflect.Indirect(reflect.ValueOf(value)).Interface()
		}

		mapped[strcase.ToLowerCamel(column.Name)] = value
	}

	return mapped, nil
}

func (db *Db) Select(info *SelectInfo, options *QueryOptions) (*types.QueryResult, error) {
	values := make([]interface{}, 0, len(info.Columns))
	whereClause := ""
	for i := 0; i < len(info.Columns); i++ {
		if i > 0 {
			whereClause += " AND "
		}

		opValue := info.Values[i]
		whereClause += fmt.Sprintf("%s %s ?", info.Columns[i], opValue.Operator)
		values = append(values, opValue.Value)
	}

	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", info.Keyspace, info.Table, whereClause)

	if info.Options.Limit > 0 {
		query += " LIMIT ?"
		values = append(values, info.Options.Limit)
	}

	if len(info.OrderBy) > 0 {
		query += " ORDER BY "
		for i, order := range info.OrderBy {
			if i > 0 {
				query += ", "
			}
			query += order.Column + " " + order.Order
		}
	}

	iter := db.session.ExecuteIter(query, options, values...)

	pageState := hex.EncodeToString(iter.PageState())
	columns := iter.Columns()
	scanner := iter.Scanner()

	items := make([]map[string]interface{}, 0)

	for scanner.Next() {
		row, err := mapScan(scanner, columns)
		if err != nil {
			return nil, err
		}
		items = append(items, row)
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return &types.QueryResult{
		PageState: pageState,
		Values:    items,
	}, nil
}

func (db *Db) Insert(info *InsertInfo, options *QueryOptions) (*types.ModificationResult, error) {

	placeholders := "?"
	for i := 1; i < len(info.Columns); i++ {
		placeholders += ", ?"
	}

	query := fmt.Sprintf(
		"INSERT INTO %s.%s (%s) VALUES (%s)",
		info.Keyspace, info.Table, strings.Join(info.Columns, ","), placeholders)

	if info.IfNotExists {
		query += " IF NOT EXISTS"
	}

	if info.TTL >= 0 {
		query += " USING TTL ?"
		info.QueryParams = append(info.QueryParams, info.TTL)
	}

	err := db.session.Execute(query, options, info.QueryParams...)

	return &types.ModificationResult{Applied: err == nil}, err
}

func (db *Db) Delete(info *DeleteInfo, options *QueryOptions) (*types.ModificationResult, error) {
	whereClause := buildWhereClause(info.Columns)
	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", info.Keyspace, info.Table, whereClause)
	err := db.session.Execute(query, options, info.QueryParams...)
	return &types.ModificationResult{Applied: err == nil}, err
}

func buildWhereClause(columnNames []string) string {
	whereClause := columnNames[0] + " = ?"
	for i := 1; i < len(columnNames); i++ {
		whereClause += " AND " + columnNames[i] + " = ?"
	}
	return whereClause
}
