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

var BooksColumnsMock = []*gocql.ColumnMetadata{
	&gocql.ColumnMetadata{
		Name: "title",
		Kind: gocql.ColumnPartitionKey,
		Type: gocql.NewNativeType(0, gocql.TypeText, ""),
	},
	&gocql.ColumnMetadata{
		Name: "pages",
		Kind: gocql.ColumnRegular,
		Type: gocql.NewNativeType(0, gocql.TypeInt, ""),
	},
	&gocql.ColumnMetadata{
		Name: "first_name",
		Kind: gocql.ColumnRegular,
		Type: gocql.NewNativeType(0, gocql.TypeText, ""),
	},
	&gocql.ColumnMetadata{
		Name: "last_name",
		Kind: gocql.ColumnRegular,
		Type: gocql.NewNativeType(0, gocql.TypeText, ""),
	},
}

func NewKeyspaceMock(ksName string, tables map[string][]*gocql.ColumnMetadata) *gocql.KeyspaceMetadata {
	tableMap := map[string]*gocql.TableMetadata{}

	for tableName, columns := range tables {
		tableEntry := &gocql.TableMetadata{
			Keyspace: ksName,
			Name:     tableName,
			Columns:  map[string]*gocql.ColumnMetadata{},
		}
		for i, column := range columns {
			column.Keyspace = ksName
			column.Table = tableName
			column.ComponentIndex = i
			tableEntry.Columns[column.Name] = column
		}
		tableEntry.PartitionKey = createKey(tableEntry.Columns, gocql.ColumnPartitionKey)
		tableEntry.ClusteringColumns = createKey(tableEntry.Columns, gocql.ColumnClusteringKey)
		tableMap[tableName] = tableEntry
	}

	return &gocql.KeyspaceMetadata{
		Name:          ksName,
		DurableWrites: true,
		StrategyClass: "NetworkTopologyStrategy",
		StrategyOptions: map[string]interface{}{
			"dc1": "3",
		},
		Tables: tableMap,
	}
}

func NewSessionMock() *SessionMock {
	return &SessionMock{}
}

func (o *SessionMock) Default() *SessionMock {
	o.SetSchemaVersion("a78bc282-aff7-4c2a-8f23-4ce3584adbb0")
	o.AddKeyspace(NewKeyspaceMock(
		"store", map[string][]*gocql.ColumnMetadata{
			"books": BooksColumnsMock,
		}))
	return o
}

func (o *SessionMock) SetSchemaVersion(version string) *mock.Call {
	schemaVersionResultMock := &ResultMock{}
	schemaVersionResultMock.
		On("Values").Return([]map[string]interface{}{
		map[string]interface{}{"schema_version": &version},
	}, nil)

	return o.On("ExecuteIter", "SELECT schema_version FROM system.local", mock.Anything, mock.Anything).
		Return(schemaVersionResultMock, nil)
}

func (o* SessionMock) AddKeyspace(keyspace *gocql.KeyspaceMetadata) *mock.Call {
	return o.On("KeyspaceMetadata", keyspace.Name).Return(keyspace, nil)
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
