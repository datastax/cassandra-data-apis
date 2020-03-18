package db

import (
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

const consistency = gocql.LocalOne

func TestDeleteGeneration(t *testing.T) {
	sessionMock := SessionMock{}
	db := &Db{
		session: &sessionMock,
	}

	sessionMock.On("Execute", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	items := []struct {
		columnNames []string
		queryParams []interface{}
		query       string
	}{
		{[]string{"a"}, []interface{}{"b"}, "DELETE FROM ks1.tbl1 WHERE a = ?"},
		{[]string{"a", "b"}, []interface{}{"A Value", 2}, "DELETE FROM ks1.tbl1 WHERE a = ? AND b = ?"},
	}

	for _, item := range items {
		_, err := db.Delete("ks1", "tbl1", item.columnNames, item.queryParams, nil, false)
		assert.Nil(t, err)
		sessionMock.AssertCalled(t, "Execute", item.query, consistency, item.queryParams)
	}

	sessionMock.AssertExpectations(t)
}

func TestSelectGeneration(t *testing.T) {
	// TODO: WIP
	//sessionMock := SessionMock{}
	//db := &Db{
	//	session: &sessionMock,
	//}
	//
	//sessionMock.On("ExecuteIter", mock.Anything, mock.Anything, mock.Anything).Return(&gocql.Iter{})
	//
	//items := []struct {
	//	columnNames []string
	//	values 		[]types.OperatorAndValue
	//	options     *types.QueryOptions
	//	orderBy     []ColumnOrder
	//	query       string
	//}{
	//	{[]string{"a"}, []types.OperatorAndValue{{"=", 1}}, &types.QueryOptions{}, nil,
	//		"SELECT * FROM ks1.tbl1 WHERE a = ?"},
	//}
	//
	//for _, item := range items {
	//	queryParams := make([]interface{}, 0)
	//
	//	for _, v := range item.values {
	//		queryParams = append(queryParams, v.Value)
	//	}
	//
	//	_, err := db.Select(&SelectInfo{
	//		Keyspace: "ks1",
	//		Table:    "tbl1",
	//		Columns:  item.columnNames,
	//		Values:   item.values,
	//		Options:  item.options,
	//		OrderBy:  item.orderBy,
	//	})
	//	assert.Nil(t, err)
	//	sessionMock.AssertCalled(t, "ExecuteIter", item.query, consistency, queryParams)
	//}
	//
	//sessionMock.AssertExpectations(t)
}

type SessionMock struct {
	mock.Mock
}

func (o *SessionMock) Execute(query string, consistency gocql.Consistency, values ...interface{}) error {
	args := o.Called(query, consistency, values)
	return args.Error(0)
}

func (o *SessionMock) ExecuteSimple(query string, consistency gocql.Consistency, values ...interface{}) error {
	args := o.Called(query, consistency, values)
	return args.Error(0)
}

func (o *SessionMock) ExecuteIter(query string, consistency gocql.Consistency, values ...interface{}) ResultIterator {
	return nil
}

func (o *SessionMock) ExecuteIterSimple(query string, consistency gocql.Consistency, values ...interface{}) ResultIterator {
	return nil
}

func (o *SessionMock) KeyspaceMetadata(keyspaceName string) (*gocql.KeyspaceMetadata, error) {
	return nil, nil
}
