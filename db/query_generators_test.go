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
		_, err := db.Delete("ks1", "tbl1", item.columnNames, item.queryParams)
		assert.Nil(t, err)
		sessionMock.AssertCalled(t, "Execute", item.query, consistency, item.queryParams)
	}

	sessionMock.AssertExpectations(t)
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

func (o *SessionMock) ExecuteIter(query string, consistency gocql.Consistency, values ...interface{}) *gocql.Iter {
	return nil
}

func (o *SessionMock) ExecuteIterSimple(query string, consistency gocql.Consistency, values ...interface{}) *gocql.Iter {
	return nil
}

func (o *SessionMock) KeyspaceMetadata(keyspaceName string) (*gocql.KeyspaceMetadata, error) {
	return nil, nil
}
