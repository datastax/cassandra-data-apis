package db

import (
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/mock"
	"sort"
)

type SessionMock struct {
	mock.Mock
}

func (o *SessionMock) Execute(query string, options *QueryOptions, values ...interface{}) error {
	args := o.Called(query, options, values)
	return args.Error(0)
}

func (o *SessionMock) ExecuteIter(query string, options *QueryOptions, values ...interface{}) (ResultSet, error) {
	args := o.Called(query, options, values)
	return args.Get(0).(ResultSet), args.Error(1)
}

func (o *SessionMock) KeyspaceMetadata(keyspaceName string) (*gocql.KeyspaceMetadata, error) {
	args := o.Called(keyspaceName)
	return args.Get(0).(*gocql.KeyspaceMetadata), args.Error(1)
}

type ResultMock struct {
	mock.Mock
}

func (o ResultMock) PageState() string {
	return o.Called().String(0)
}

func (o ResultMock) Values() []map[string]interface{} {
	args := o.Called()
	return args.Get(0).([]map[string]interface{})
}

func NewSessionMock() *SessionMock {
	sessionMock := &SessionMock{}

	columns := map[string]*gocql.ColumnMetadata{
		"title": &gocql.ColumnMetadata{
			Keyspace:        "store",
			Table:           "books",
			Name:            "title",
			ComponentIndex:  0,
			Kind:            gocql.ColumnPartitionKey,
			Type:            gocql.NewNativeType(0, gocql.TypeText, ""),
			ClusteringOrder: "",
			Order:           false,
		},
		"pages": &gocql.ColumnMetadata{
			Keyspace:        "store",
			Table:           "books",
			Name:            "pages",
			ComponentIndex:  1,
			Kind:            gocql.ColumnRegular,
			Type:            gocql.NewNativeType(0, gocql.TypeInt, ""),
			ClusteringOrder: "",
			Order:           false,
		},
		"first_name": &gocql.ColumnMetadata{
			Keyspace:        "store",
			Table:           "books",
			Name:            "first_name",
			ComponentIndex:  2,
			Kind:            gocql.ColumnRegular,
			Type:            gocql.NewNativeType(0, gocql.TypeText, ""),
			ClusteringOrder: "",
			Order:           false,
		},
		"last_name": &gocql.ColumnMetadata{
			Keyspace:        "store",
			Table:           "books",
			Name:            "last_name",
			ComponentIndex:  3,
			Kind:            gocql.ColumnRegular,
			Type:            gocql.NewNativeType(0, gocql.TypeText, ""),
			ClusteringOrder: "",
			Order:           false,
		},
	}
	sessionMock.On("KeyspaceMetadata", "store").Return(&gocql.KeyspaceMetadata{
		Name:          "store",
		DurableWrites: true,
		StrategyClass: "NetworkTopologyStrategy",
		StrategyOptions: map[string]interface{}{
			"dc1": "3",
		},
		Tables: map[string]*gocql.TableMetadata{
			"books": &gocql.TableMetadata{
				Keyspace:          "store",
				Name:              "books",
				PartitionKey:      createKey(columns, gocql.ColumnPartitionKey),
				ClusteringColumns: createKey(columns, gocql.ColumnClusteringKey),
				Columns:           columns,
			},
		},
	}, nil)

	schemaVersion := "a78bc282-aff7-4c2a-8f23-4ce3584adbb0"
	schemaVersionResultMock := &ResultMock{}
	schemaVersionResultMock.
		On("Values").Return([]map[string]interface{}{
		map[string]interface{}{"schema_version": &schemaVersion},
	}, nil)

	sessionMock.
		On("ExecuteIter", "SELECT schema_version FROM system.local", mock.Anything, mock.Anything).
		Return(schemaVersionResultMock, nil)

	return sessionMock
}

func createKey(columns map[string]*gocql.ColumnMetadata, kind gocql.ColumnKind) []*gocql.ColumnMetadata {
	key := make([]*gocql.ColumnMetadata, 0)
	for _, column := range columns {
		if column.Kind == kind {
			key = append(key, column)
		}
	}
	sort.Slice(key, func(i, j int) bool {
		return key[i].ComponentIndex < key[j].ComponentIndex
	})
	return key
}
