package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	e "github.com/datastax/cassandra-data-apis/rest/endpoint/v1"
	"github.com/datastax/cassandra-data-apis/rest/models"
	"github.com/datastax/cassandra-data-apis/types"
	"github.com/julienschmidt/httprouter"
	. "github.com/onsi/gomega"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"reflect"
	"regexp"
	"strings"
)

const Prefix = "/rest"

func ExecuteGet(routes []types.Route, routeFormat string, responsePtr interface{}, values ...interface{}) int {
	return execute(http.MethodGet, routes, routeFormat, "", responsePtr, values...)
}

func ExecutePost(
	routes []types.Route,
	routeFormat string,
	requestBody string,
	responsePtr interface{},
	values ...interface{},
) int {
	return execute(http.MethodPost, routes, routeFormat, requestBody, responsePtr, values...)
}

func ExecutePut(
	routes []types.Route,
	routeFormat string,
	requestBody string,
	responsePtr interface{},
	values ...interface{},
) int {
	return execute(http.MethodPut, routes, routeFormat, requestBody, responsePtr, values...)
}

func ExecuteDelete(
	routes []types.Route,
	routeFormat string,
	values ...interface{},
) int {
	return execute(http.MethodDelete, routes, routeFormat, "", nil, values...)
}

// ExecuteGetDataTypeJson performs a GET request and returns the json decoded value of the provided row cell
func ExecuteGetDataTypeJsonValue(routes []types.Route, dataType, id string) interface{} {
	var response models.Rows
	ExecuteGet(routes, e.RowSinglePathFormat, &response, "datatypes", "scalars", id)
	Expect(response.Rows).To(HaveLen(1))
	return response.Rows[0][dataType+"_col"]
}

func execute(
	method string,
	routes []types.Route,
	routeFormat string,
	requestBody string,
	responsePtr interface{},
	values ...interface{},
) int {
	rv := reflect.ValueOf(responsePtr)
	if responsePtr != nil && rv.Kind() != reflect.Ptr {
		panic("Provided value should be a pointer or nil")
	}

	targetPath := path.Join(Prefix, fmt.Sprintf(routeFormat, values...))
	var body io.Reader = nil
	if requestBody != "" {
		body = bytes.NewBuffer([]byte(requestBody))
	}

	r, _ := http.NewRequest(method, targetPath, body)
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}

	w := httptest.NewRecorder()
	route := lookupRoute(routes, method, routeFormat)

	// Use default router for params to be populated
	router := httprouter.New()
	router.Handler(method, route.Pattern, route.Handler)
	router.ServeHTTP(w, r)

	if w.Code < http.StatusOK || w.Code > http.StatusIMUsed {
		// Not in the 2xx range
		if responsePtr == nil {
			return w.Code
		}
		_, ok := responsePtr.(*models.ModelError)
		if !ok {
			panic(fmt.Sprintf("unexpected http error %d: %s", w.Code, w.Body))
		}
	}

	if w.Code != http.StatusNoContent {
		bodyString := w.Body.String()
		err := json.NewDecoder(bytes.NewBufferString(bodyString)).Decode(responsePtr)
		Expect(err).ToNot(HaveOccurred(),
			fmt.Sprintf("Error decoding response with code %d and body: %s", w.Code, bodyString))
	}

	return w.Code
}

func lookupRoute(routes []types.Route, method, format string) types.Route {
	// Word tokens for parameters
	regexStr := strings.Replace(format, `%s`, `[\w:{}]+`, -1)
	// End of the string
	regexStr += `$`

	re := regexp.MustCompile(regexStr)
	for _, route := range routes {
		if re.MatchString(route.Pattern) && route.Method == method {
			return route
		}
	}

	panic("Route not found")
}
