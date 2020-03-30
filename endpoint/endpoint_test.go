package endpoint

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/riptano/data-endpoints/db"
	"github.com/riptano/data-endpoints/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"net/http"
	"net/http/httptest"
	"path"
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
	resultMock := &db.ResultMock{}
	resultMock.
		On("PageState").Return("").
		On("Values").Return([]map[string]interface{}{
		map[string]interface{}{"title": &title, "pages": &pages},
	}, nil)

	session.
		On("ExecuteIter", "SELECT * FROM store.books WHERE title = ?", mock.Anything, mock.Anything).
		Return(resultMock, nil)

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

func createRoutes(t *testing.T, pattern string, ksName string) (*db.SessionMock, []graphql.Route) {
	sessionMock := db.NewSessionMock().Default()

	cfg := NewEndpointConfig(host)
	endpoint := cfg.newEndpointWithDb(db.NewDbWithSession(sessionMock))
	routes, err := endpoint.RoutesKeyspaceGraphQL("/graphql", "store")

	assert.Len(t, routes, 2, "expected GET and POST routes")
	assert.NoError(t, err, "error getting routes for keyspace")

	return sessionMock, routes
}
