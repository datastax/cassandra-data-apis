package endpoint

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/riptano/data-endpoints/db"
	"github.com/riptano/data-endpoints/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"net/http"
	"net/http/httptest"
	"path"
	"sort"
	"testing"
)

const (
	getIndex  = 0
	postIndex = 1
)

const host = "127.0.0.1"

type responseBody struct {
	Data   map[string]interface{} `json:"data"`
	Errors []struct {
		Message   string `json:"locations"`
		Locations []struct {
			Line   int `json:"line"`
			Column int `json:"column"`
		}
	} `json:"errors"`
}

func TestDataEndpoint_Query(t *testing.T) {
	session, routes := createRoutes(t, "/graphql", "store")

	title := "book1"
	pages := 42
	result := &ResultMock{}
	result.
		On("PageState").Return("").
		On("Values").Return([]map[string]interface{}{
		map[string]interface{}{"title": &title, "pages": &pages},
	}, nil)

	session.
		On("ExecuteIter", "SELECT * FROM store.books WHERE title = ?", mock.Anything, mock.Anything).
		Return(result, nil)

	body := graphql.RequestBody{
		Query: `query {
  books(data:{title:"abc"}) {
    values {
      pages
      title
    }
  }
}`,
	}

	expected := responseBody{
		Data: map[string]interface{}{
			"books": map[string]interface{}{
				"values": []interface{}{
					map[string]interface{}{
						"pages": float64(pages),
						"title": title,
					},
				},
			},
		},
	}

	buffer, err := executePost(routes, "/graphql", body)
	assert.NoError(t, err, "error executing query")

	var resp responseBody
	err = json.NewDecoder(buffer).Decode(&resp)
	assert.NoError(t, err, "error decoding response")
	assert.Equal(t, expected, resp)
}

func executePost(routes []graphql.Route, target string, body graphql.RequestBody) (*bytes.Buffer, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	r := httptest.NewRequest(http.MethodPost, path.Join(fmt.Sprintf("http://%s", host), target), bytes.NewReader(b))
	w := httptest.NewRecorder()

	routes[postIndex].HandlerFunc(w, r)

	return w.Body, nil
}

func createRoutes(t *testing.T, pattern string, ksName string) (*SessionMock, []graphql.Route) {
	session := NewSessionMock()

	cfg := NewEndpointConfig(host)
	endpoint := cfg.newEndpointWithDb(db.NewDbWithSession(session))
	routes, err := endpoint.RoutesKeyspaceGraphQL("/graphql", "store")

	assert.Len(t, routes, 2, "expected GET and POST routes")
	assert.NoError(t, err, "error getting routes for keyspace")

	return session, routes
}

type SessionMock struct {
	mock.Mock
}

func (o *SessionMock) Execute(query string, options *db.QueryOptions, values ...interface{}) error {
	args := o.Called(query, options, values)
	return args.Error(0)
}

func (o *SessionMock) ExecuteIter(query string, options *db.QueryOptions, values ...interface{}) (db.ResultSet, error) {
	args := o.Called(query, options, values)
	return args.Get(0).(db.ResultSet), args.Error(1)
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
