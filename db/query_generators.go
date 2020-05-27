package db

import (
	"errors"
	"fmt"
	"github.com/datastax/cassandra-data-apis/types"
	"github.com/gocql/gocql"
)

type SelectInfo struct {
	Keyspace string
	Table    string
	Columns  []string
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

func (db *Db) Select(info *SelectInfo, options *QueryOptions) (ResultSet, error) {
	values := make([]interface{}, 0, len(info.Where))
	whereClause := buildCondition(info.Where, &values)
	columns := "  *"

	if len(info.Columns) > 0 {
		columns = ""
		for _, columnName := range info.Columns {
			columns += fmt.Sprintf(`, "%s"`, columnName)
		}
	}

	query := fmt.Sprintf(`SELECT %s FROM "%s"."%s"`, columns[2:], info.Keyspace, info.Table)

	if whereClause != "" {
		query += fmt.Sprintf(" WHERE %s", whereClause)
	}

	if len(info.OrderBy) > 0 {
		query += " ORDER BY "
		for i, order := range info.OrderBy {
			if i > 0 {
				query += ", "
			}
			query += fmt.Sprintf(`"%s" %s`, order.Column, order.Order)
		}
	}

	if info.Options != nil && info.Options.Limit > 0 {
		query += " LIMIT ?"
		values = append(values, info.Options.Limit)
	}

	return db.session.ExecuteIter(query, options, values...)
}

func (db *Db) Insert(info *InsertInfo, options *QueryOptions) (ResultSet, error) {
	placeholders := ""
	columns := ""
	for _, columnName := range info.Columns {
		placeholders += ", ?"
		columns += fmt.Sprintf(`, "%s"`, columnName)
	}

	query := fmt.Sprintf(
		`INSERT INTO "%s"."%s" (%s) VALUES (%s)`, info.Keyspace, info.Table,
		// Remove the initial ", " token
		columns[2:], placeholders[2:])

	if info.IfNotExists {
		query += " IF NOT EXISTS"
	}

	if info.TTL >= 0 {
		query += " USING TTL ?"
		info.QueryParams = append(info.QueryParams, info.TTL)
	}

	return db.session.ExecuteIter(query, options, info.QueryParams...)
}

func (db *Db) Delete(info *DeleteInfo, options *QueryOptions) (ResultSet, error) {
	whereClause := buildWhereClause(info.Columns)
	query := fmt.Sprintf(`DELETE FROM "%s"."%s" WHERE %s`, info.Keyspace, info.Table, whereClause)
	queryParameters := make([]interface{}, len(info.QueryParams))
	copy(queryParameters, info.QueryParams)

	if info.IfExists {
		query += " IF EXISTS"
	} else if len(info.IfCondition) > 0 {
		query += " IF " + buildCondition(info.IfCondition, &queryParameters)
	}

	return db.session.ExecuteIter(query, options, queryParameters...)
}

func (db *Db) Update(info *UpdateInfo, options *QueryOptions) (ResultSet, error) {
	// We have to differentiate between WHERE and SET clauses
	setClause := ""
	whereClause := ""
	setParameters := make([]interface{}, 0, len(info.QueryParams))
	whereParameters := make([]interface{}, 0, len(info.QueryParams))

	for i, columnName := range info.Columns {
		column, ok := info.Table.Columns[columnName]
		if ok && (column.Kind == gocql.ColumnPartitionKey || column.Kind == gocql.ColumnClusteringKey) {
			whereClause += fmt.Sprintf(` AND "%s" = ?`, columnName)
			whereParameters = append(whereParameters, info.QueryParams[i])
		} else {
			setClause += fmt.Sprintf(`, "%s" = ?`, columnName)
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
	// Remove the initial ", " token
	setClause = setClause[2:]

	query := fmt.Sprintf(
		`UPDATE "%s"."%s"%s SET %s WHERE %s`, info.Keyspace, info.Table.Name, ttl, setClause, whereClause)

	if info.IfExists {
		query += " IF EXISTS"
	} else if len(info.IfCondition) > 0 {
		query += " IF " + buildCondition(info.IfCondition, &queryParameters)
	}

	return db.session.ExecuteIter(query, options, queryParameters...)
}

func buildWhereClause(columnNames []string) string {
	whereClause := ""
	for _, name := range columnNames {
		whereClause += fmt.Sprintf(` AND "%s" = ?`, name)
	}

	// Remove initial " AND " characters
	return whereClause[5:]
}

func buildCondition(condition []types.ConditionItem, queryParameters *[]interface{}) string {
	if len(condition) == 0 {
		return ""
	}

	conditionClause := ""
	for _, item := range condition {
		conditionClause += fmt.Sprintf(` AND "%s" %s ?`, item.Column, item.Operator)
		*queryParameters = append(*queryParameters, item.Value)
	}

	// Remove initial " AND " characters
	return conditionClause[5:]
}
