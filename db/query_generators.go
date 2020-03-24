package db

import (
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/riptano/data-endpoints/types"
	"reflect"
	"strings"
)

type SelectInfo struct {
	Keyspace string
	Table    string
	Where    []types.ConditionItem
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
	IfCondition []types.ConditionItem
	IfExists    bool
}

type UpdateInfo struct {
	Keyspace    string
	Table       *gocql.TableMetadata
	Columns     []string
	QueryParams []interface{}
	IfCondition []types.ConditionItem
	IfExists    bool
	TTL         int
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
		case gocql.TypeVarchar, gocql.TypeAscii, gocql.TypeInet, gocql.TypeText, gocql.TypeBigInt, gocql.TypeCounter:
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
		case gocql.TypeList:
			values[i] = new([]int)
		default:
			panic("Support for CQL type not found: " + typeInfo.Type().String())
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

		mapped[column.Name] = value
	}

	return mapped, nil
}

func (db *Db) Select(info *SelectInfo, options *QueryOptions) (ResultSet, error) {
	values := make([]interface{}, 0, len(info.Where))
	whereClause := buildCondition(info.Where, &values)
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", info.Keyspace, info.Table, whereClause)

	if len(info.OrderBy) > 0 {
		query += " ORDER BY "
		for i, order := range info.OrderBy {
			if i > 0 {
				query += ", "
			}
			query += order.Column + " " + order.Order
		}
	}

	if info.Options.Limit > 0 {
		query += " LIMIT ?"
		values = append(values, info.Options.Limit)
	}

	return db.session.ExecuteIter(query, options, values...)
}

func (db *Db) Insert(info *InsertInfo, options *QueryOptions) (*types.ModificationResult, error) {

	placeholders := "?"
	for i := 1; i < len(info.Columns); i++ {
		placeholders += ", ?"
	}

	query := fmt.Sprintf(
		"INSERT INTO %s.%s (%s) VALUES (%s)",
		info.Keyspace, info.Table, strings.Join(info.Columns, ", "), placeholders)

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
	queryParameters := make([]interface{}, len(info.QueryParams))
	copy(queryParameters, info.QueryParams)

	if info.IfExists {
		query += " IF EXISTS"
	} else if len(info.IfCondition) > 0 {
		query += " IF " + buildCondition(info.IfCondition, &queryParameters)
	}

	err := db.session.Execute(query, options, queryParameters...)
	return &types.ModificationResult{Applied: err == nil}, err
}

func (db *Db) Update(info *UpdateInfo, options *QueryOptions) (*types.ModificationResult, error) {
	// We have to differentiate between WHERE and SET clauses
	setClause := ""
	whereClause := ""
	setParameters := make([]interface{}, 0, len(info.QueryParams))
	whereParameters := make([]interface{}, 0, len(info.QueryParams))

	keys := make(map[string]bool)
	for _, c := range info.Table.PartitionKey {
		keys[c.Name] = true
	}
	for _, c := range info.Table.ClusteringColumns {
		keys[c.Name] = true
	}

	for i, columnName := range info.Columns {
		if keys[columnName] {
			whereClause += fmt.Sprintf(" AND %s = ?", columnName)
			whereParameters = append(whereParameters, info.QueryParams[i])
		} else {
			setClause += fmt.Sprintf(", %s = ?", columnName)
			setParameters = append(setParameters, info.QueryParams[i])
		}
	}

	if len(whereClause) == 0 {
		return nil, errors.New("Partition and clustering keys must be included in query")
	}
	if len(setClause) == 0 {
		return nil, errors.New("Query must include columns to update")
	}

	queryParameters := make([]interface{}, 0, len(info.QueryParams))

	ttl := ""
	if info.TTL >= 0 {
		ttl = " USING TTL ?"
		queryParameters = append(queryParameters, info.TTL)
	}

	for _, v := range setParameters {
		queryParameters = append(queryParameters, v)
	}
	for _, v := range whereParameters {
		queryParameters = append(queryParameters, v)
	}

	// Remove the initial AND operator
	whereClause = whereClause[5:]
	// Remove the initial , operator
	setClause = setClause[2:]

	query := fmt.Sprintf("UPDATE %s.%s%s SET %s WHERE %s", info.Keyspace, info.Table.Name, ttl, setClause, whereClause)

	if info.IfExists {
		query += " IF EXISTS"
	} else if len(info.IfCondition) > 0 {
		query += " IF " + buildCondition(info.IfCondition, &queryParameters)
	}

	err := db.session.Execute(query, options, queryParameters...)
	return &types.ModificationResult{Applied: err == nil}, err
}

func buildWhereClause(columnNames []string) string {
	whereClause := columnNames[0] + " = ?"
	for i := 1; i < len(columnNames); i++ {
		whereClause += " AND " + columnNames[i] + " = ?"
	}
	return whereClause
}

func buildCondition(condition []types.ConditionItem, queryParameters *[]interface{}) string {
	conditionClause := ""
	for _, item := range condition {
		if conditionClause != "" {
			conditionClause += " AND "
		}

		conditionClause += fmt.Sprintf("%s %s ?", item.Column, item.Operator)
		*queryParameters = append(*queryParameters, item.Value)
	}
	return conditionClause
}
