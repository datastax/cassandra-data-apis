package schemas

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/datastax/cassandra-data-apis/graphql"
	"github.com/datastax/cassandra-data-apis/internal/testutil"
	"github.com/datastax/cassandra-data-apis/types"
	"github.com/gocql/gocql"
	"github.com/iancoleman/strcase"
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"path"
)

type ResponseBody struct {
	Data   map[string]interface{} `json:"data"`
	Errors []ErrorEntry           `json:"errors"`
}

type ErrorEntry struct {
	Message   string   `json:"message"`
	Path      []string `json:"path"`
	Locations []struct {
		Line   int `json:"line"`
		Column int `json:"column"`
	} `json:"locations"`
}

const GraphQLTypesQuery = `{
  __schema {
	types {
	  name
	  description
	}
  }
}`

const (
	postIndex = 1
	host      = "127.0.0.1"
)

func DecodeResponse(buffer *bytes.Buffer) ResponseBody {
	var response ResponseBody
	err := json.NewDecoder(buffer).Decode(&response)
	Expect(err).ToNot(HaveOccurred())
	return response
}

func DecodeData(buffer *bytes.Buffer, key string) map[string]interface{} {
	response := DecodeResponse(buffer)
	Expect(response.Errors).To(HaveLen(0))
	value, found := response.Data[key]
	if !found {
		panic(fmt.Sprintf("%s key not in response: %v", key, response))
	}
	return value.(map[string]interface{})
}

func DecodeDataAsSliceOfMaps(buffer *bytes.Buffer, key string, property string) []map[string]interface{} {
	arr := DecodeData(buffer, key)[property].([]interface{})
	result := make([]map[string]interface{}, 0, len(arr))
	for _, item := range arr {
		result = append(result, item.(map[string]interface{}))
	}
	return result
}

func NewResponseBody(operationName string, elementMap map[string]interface{}) ResponseBody {
	return ResponseBody{
		Data: map[string]interface{}{
			operationName: elementMap,
		},
	}
}

func GetTypeNamesByTable(tableName string) []string {
	baseName := strcase.ToCamel(tableName)
	return []string{
		baseName + "Input",
		baseName + "FilterInput",
		baseName,
		baseName + "Result",
		baseName + "Order",
		baseName + "MutationResult",
	}
}

func NewUuid() string {
	uuid, err := gocql.RandomUUID()
	testutil.PanicIfError(err)
	return uuid.String()
}

func ExecutePost(routes []types.Route, target string, body string) *bytes.Buffer {
	b, err := json.Marshal(graphql.RequestBody{Query: body})
	Expect(err).ToNot(HaveOccurred())
	targetUrl := fmt.Sprintf("http://%s", path.Join(host, target))
	r := httptest.NewRequest(http.MethodPost, targetUrl, bytes.NewReader(b))
	w := httptest.NewRecorder()
	routes[postIndex].Handler.ServeHTTP(w, r)
	Expect(w.Code).To(Equal(http.StatusOK))
	return w.Body
}

func ExecutePostWithVariables(routes []types.Route, target string, body string, variables map[string]interface{}) *bytes.Buffer {
	b, err := json.Marshal(graphql.RequestBody{Query: body, Variables: variables})
	Expect(err).ToNot(HaveOccurred())
	targetUrl := fmt.Sprintf("http://%s", path.Join(host, target))
	r := httptest.NewRequest(http.MethodPost, targetUrl, bytes.NewReader(b))
	w := httptest.NewRecorder()
	routes[postIndex].Handler.ServeHTTP(w, r)
	Expect(w.Code).To(Equal(http.StatusOK))
	return w.Body
}

func ExpectQueryToReturnError(routes []types.Route, query string, expectedMessage string) {
	b, err := json.Marshal(graphql.RequestBody{Query: query})
	Expect(err).ToNot(HaveOccurred())
	targetUrl := fmt.Sprintf("http://%s", path.Join(host, "/graphql"))
	r := httptest.NewRequest(http.MethodPost, targetUrl, bytes.NewReader(b))
	w := httptest.NewRecorder()
	routes[postIndex].Handler.ServeHTTP(w, r)
	// GraphQL spec defines the error as a field and HTTP status code should still be 200
	// http://spec.graphql.org/June2018/#sec-Errors
	Expect(w.Code).To(Equal(http.StatusOK))
	response := DecodeResponse(w.Body)
	Expect(response.Data).To(HaveLen(0))
	ExpectError(response, expectedMessage)
}

func ExpectError(response ResponseBody, expectedMessage string) {
	Expect(response.Errors).To(HaveLen(1))
	Expect(response.Errors[0].Message).To(ContainSubstring(expectedMessage))
}
