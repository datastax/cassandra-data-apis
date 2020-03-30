package db

import (
	"github.com/gocql/gocql"
	"github.com/riptano/data-endpoints/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestDeleteGeneration(t *testing.T) {
	items := []struct {
		columnNames []string
		queryParams []interface{}
		query       string
		ifExists    bool
		ifCondition []types.ConditionItem
	}{
		{[]string{"a"}, []interface{}{"b"}, "DELETE FROM ks1.tbl1 WHERE a = ?", false, nil},
		{[]string{"a", "b"}, []interface{}{"A Value", 2}, "DELETE FROM ks1.tbl1 WHERE a = ? AND b = ?", false, nil},
		{[]string{"a"}, []interface{}{"b"}, "DELETE FROM ks1.tbl1 WHERE a = ? IF EXISTS", true, nil},
		{[]string{"a"}, []interface{}{"b"}, "DELETE FROM ks1.tbl1 WHERE a = ? IF c = ?", false, []types.ConditionItem{{"c", "=", "z"}}},
	}

	for _, item := range items {
		sessionMock := SessionMock{}
		db := &Db{
			session: &sessionMock,
		}

		sessionMock.On("Execute", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		_, err := db.Delete(&DeleteInfo{
			Keyspace:    "ks1",
			Table:       "tbl1",
			Columns:     item.columnNames,
			QueryParams: item.queryParams,
			IfExists:    item.ifExists,
			IfCondition: item.ifCondition,
		}, nil)
		assert.Nil(t, err)

		expectedQueryParams := make([]interface{}, len(item.queryParams))
		copy(expectedQueryParams, item.queryParams)

		if len(item.ifCondition) > 0 {
			for _, condition := range item.ifCondition {
				expectedQueryParams = append(expectedQueryParams, condition.Value)
			}
		}
		sessionMock.AssertCalled(t, "Execute", item.query, mock.Anything, expectedQueryParams)
		sessionMock.AssertExpectations(t)
	}
}

func TestUpdateGeneration(t *testing.T) {
	table := &gocql.TableMetadata{
		Name:              "tbl1",
		PartitionKey:      []*gocql.ColumnMetadata{{Name: "pk1"}, {Name: "pk2"}},
		ClusteringColumns: []*gocql.ColumnMetadata{{Name: "ck1"}},
	}

	items := []struct {
		columnNames    []string
		queryParams    []interface{}
		ifExists       bool
		ifCondition    []types.ConditionItem
		ttl            int
		query          string
		expectedParams []interface{}
	}{
		{[]string{"ck1", "a", "b", "pk2", "pk1"}, []interface{}{1, 2, 3, 4, 5}, false, nil, -1,
			"UPDATE ks1.tbl1 SET a = ?, b = ? WHERE ck1 = ? AND pk2 = ? AND pk1 = ?", []interface{}{2, 3, 1, 4, 5}},
		{[]string{"a", "ck1", "pk1", "pk2"}, []interface{}{1, 2, 3, 4}, true, nil, 60,
			"UPDATE ks1.tbl1 USING TTL ? SET a = ? WHERE ck1 = ? AND pk1 = ? AND pk2 = ? IF EXISTS",
			[]interface{}{60, 1, 2, 3, 4}},
		{[]string{"a", "ck1", "pk1", "pk2"}, []interface{}{1, 2, 3, 4}, false,
			[]types.ConditionItem{{"c", ">", 100}}, -1,
			"UPDATE ks1.tbl1 SET a = ? WHERE ck1 = ? AND pk1 = ? AND pk2 = ? IF c > ?",
			[]interface{}{1, 2, 3, 4, 100}},
	}

	for _, item := range items {
		sessionMock := SessionMock{}
		db := &Db{
			session: &sessionMock,
		}

		sessionMock.On("Execute", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		_, err := db.Update(&UpdateInfo{
			Keyspace:    "ks1",
			Table:       table,
			Columns:     item.columnNames,
			QueryParams: item.queryParams,
			IfExists:    item.ifExists,
			IfCondition: item.ifCondition,
			TTL:         item.ttl,
		}, nil)
		assert.Nil(t, err)

		sessionMock.AssertCalled(t, "Execute", item.query, mock.Anything, item.expectedParams)
		sessionMock.AssertExpectations(t)
	}
}

func TestInsertGeneration(t *testing.T) {
	items := []struct {
		columnNames []string
		queryParams []interface{}
		ttl         int
		ifNotExists bool
		query       string
	}{
		{[]string{"a"}, []interface{}{100}, -1, false, "INSERT INTO ks1.tbl1 (a) VALUES (?)"},
		{[]string{"a", "b"}, []interface{}{100, 2}, -1, false, "INSERT INTO ks1.tbl1 (a, b) VALUES (?, ?)"},
		{[]string{"a"}, []interface{}{100}, -1, true, "INSERT INTO ks1.tbl1 (a) VALUES (?) IF NOT EXISTS"},
		{[]string{"a"}, []interface{}{"z"}, 3600, true,
			"INSERT INTO ks1.tbl1 (a) VALUES (?) IF NOT EXISTS USING TTL ?"},
	}

	for _, item := range items {
		sessionMock := SessionMock{}
		db := &Db{
			session: &sessionMock,
		}

		sessionMock.On("Execute", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		expectedQueryParams := make([]interface{}, len(item.queryParams))
		copy(expectedQueryParams, item.queryParams)

		if item.ttl >= 0 {
			expectedQueryParams = append(expectedQueryParams, item.ttl)
		}

		_, err := db.Insert(&InsertInfo{
			Keyspace:    "ks1",
			Table:       "tbl1",
			Columns:     item.columnNames,
			QueryParams: item.queryParams,
			TTL:         item.ttl,
			IfNotExists: item.ifNotExists,
		}, nil)
		assert.Nil(t, err)
		sessionMock.AssertCalled(t, "Execute", item.query, mock.Anything, expectedQueryParams)
		sessionMock.AssertExpectations(t)
	}
}

func TestSelectGeneration(t *testing.T) {
	resultMock := &ResultMock{}
	resultMock.
		On("PageState").Return("").
		On("Values").Return([]map[string]interface{}{}, nil)

	items := []struct {
		where   []types.ConditionItem
		options *types.QueryOptions
		orderBy []ColumnOrder
		query   string
	}{
		{[]types.ConditionItem{{"a", "=", 1}}, &types.QueryOptions{}, nil,
			"SELECT * FROM ks1.tbl1 WHERE a = ?"},
		{[]types.ConditionItem{{"a", "=", 1}, {"b", ">", 2}}, &types.QueryOptions{}, nil,
			"SELECT * FROM ks1.tbl1 WHERE a = ? AND b > ?"},
		{[]types.ConditionItem{{"a", "=", 1}, {"b", ">", 2}, {"b", "<=", 5}}, &types.QueryOptions{}, nil,
			"SELECT * FROM ks1.tbl1 WHERE a = ? AND b > ? AND b <= ?"},
		{[]types.ConditionItem{{"a", "=", 1}}, &types.QueryOptions{}, []ColumnOrder{{"c", "DESC"}},
			"SELECT * FROM ks1.tbl1 WHERE a = ? ORDER BY c DESC"},
		{[]types.ConditionItem{{"a", "=", "z"}}, &types.QueryOptions{Limit: 1}, []ColumnOrder{{"c", "ASC"}},
			"SELECT * FROM ks1.tbl1 WHERE a = ? ORDER BY c ASC LIMIT ?"},
	}

	for _, item := range items {
		sessionMock := SessionMock{}
		db := &Db{
			session: &sessionMock,
		}
		sessionMock.On("ExecuteIter", mock.Anything, mock.Anything, mock.Anything).Return(resultMock, nil)
		queryParams := make([]interface{}, 0)

		for _, v := range item.where {
			queryParams = append(queryParams, v.Value)
		}

		if item.options != nil && item.options.Limit > 0 {
			queryParams = append(queryParams, item.options.Limit)
		}

		_, err := db.Select(&SelectInfo{
			Keyspace: "ks1",
			Table:    "tbl1",
			Where:    item.where,
			Options:  item.options,
			OrderBy:  item.orderBy,
		}, nil)
		assert.Nil(t, err)
		sessionMock.AssertCalled(t, "ExecuteIter", item.query, mock.Anything, queryParams)
		sessionMock.AssertExpectations(t)
	}
}
