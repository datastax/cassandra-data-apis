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
	}{
		{[]string{"a"}, []interface{}{"b"}, "DELETE FROM ks1.tbl1 WHERE a = ?"},
		{[]string{"a", "b"}, []interface{}{"A Value", 2}, "DELETE FROM ks1.tbl1 WHERE a = ? AND b = ?"},
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
			QueryParams: item.queryParams}, nil)
		assert.Nil(t, err)
		sessionMock.AssertCalled(t, "Execute", item.query, mock.Anything, item.queryParams)
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
	scannerMock := &ScannerMock{}
	resultMock.
		On("PageState").Return([]byte{}).
		On("Columns").Return([]gocql.ColumnInfo{}).
		On("Scanner").Return(scannerMock).
		On("Close").Return(nil)
	scannerMock.On("Next").Return(false)

	items := []struct {
		columnNames []string
		values      []types.OperatorAndValue
		options     *types.QueryOptions
		orderBy     []ColumnOrder
		query       string
	}{
		{[]string{"a"}, []types.OperatorAndValue{{"=", 1}}, &types.QueryOptions{}, nil,
			"SELECT * FROM ks1.tbl1 WHERE a = ?"},
		{[]string{"a", "b"}, []types.OperatorAndValue{{"=", 1}, {">", 2}}, &types.QueryOptions{}, nil,
			"SELECT * FROM ks1.tbl1 WHERE a = ? AND b > ?"},
		{[]string{"a"}, []types.OperatorAndValue{{"=", 1}}, &types.QueryOptions{}, []ColumnOrder{{"c", "DESC"}},
			"SELECT * FROM ks1.tbl1 WHERE a = ? ORDER BY c DESC"},
		{[]string{"a"}, []types.OperatorAndValue{{"=", "z"}}, &types.QueryOptions{Limit: 1}, []ColumnOrder{{"c", "ASC"}},
			"SELECT * FROM ks1.tbl1 WHERE a = ? LIMIT ? ORDER BY c ASC"},
	}

	for _, item := range items {
		sessionMock := SessionMock{}
		db := &Db{
			session: &sessionMock,
		}
		sessionMock.On("ExecuteIter", mock.Anything, mock.Anything, mock.Anything).Return(resultMock)
		queryParams := make([]interface{}, 0)

		for _, v := range item.values {
			queryParams = append(queryParams, v.Value)
		}

		if item.options != nil && item.options.Limit > 0 {
			queryParams = append(queryParams, item.options.Limit)
		}

		_, err := db.Select(&SelectInfo{
			Keyspace: "ks1",
			Table:    "tbl1",
			Columns:  item.columnNames,
			Values:   item.values,
			Options:  item.options,
			OrderBy:  item.orderBy,
		}, nil)
		assert.Nil(t, err)
		sessionMock.AssertCalled(t, "ExecuteIter", item.query, mock.Anything, queryParams)
		sessionMock.AssertExpectations(t)
	}
}

type SessionMock struct {
	mock.Mock
}

func (o *SessionMock) Execute(query string, options *QueryOptions, values ...interface{}) error {
	args := o.Called(query, options, values)
	return args.Error(0)
}

func (o *SessionMock) ExecuteIter(query string, options *QueryOptions, values ...interface{}) ResultIterator {
	args := o.Called(query, options, values)
	return args.Get(0).(ResultIterator)
}

func (o *SessionMock) KeyspaceMetadata(keyspaceName string) (*gocql.KeyspaceMetadata, error) {
	args := o.Called(keyspaceName)
	return args.Get(0).(*gocql.KeyspaceMetadata), args.Error(1)
}

type ResultMock struct {
	mock.Mock
}

type ScannerMock struct {
	mock.Mock
}

func (o ScannerMock) Next() bool {
	return o.Called().Bool(0)
}

func (o ScannerMock) Scan(dest ...interface{}) error {
	return o.Called(dest).Error(0)
}

func (o ScannerMock) Err() error {
	return o.Called().Error(0)
}

func (o ResultMock) Close() error {
	return o.Called().Error(0)
}

func (o ResultMock) Columns() []gocql.ColumnInfo {
	return o.Called().Get(0).([]gocql.ColumnInfo)
}

func (o ResultMock) Scanner() gocql.Scanner {
	return o.Called().Get(0).(gocql.Scanner)
}

func (o ResultMock) PageState() []byte {
	return o.Called().Get(0).([]byte)
}

func (o ResultMock) Scan(dest ...interface{}) bool {
	return o.Called(dest).Bool(0)
}

func (o ResultMock) MapScan(m map[string]interface{}) bool {
	return o.Called(m).Bool(0)
}
