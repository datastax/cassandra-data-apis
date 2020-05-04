package endpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	m "github.com/datastax/cassandra-data-apis/rest/models"
)

// RespondJSONObjectWithCode writes the object and status header to the response. Important to note that if this is being
// used for an error case then an empty return will need to immediately follow the call to this function
func RespondJSONObjectWithCode(w http.ResponseWriter, code int, obj interface{}) {
	setCommonHeaders(w)
	var err error
	var jsonBytes []byte
	if obj != nil {
		jsonBytes, err = json.Marshal(obj)
	}
	writeJSONBytes(w, jsonBytes, err, code)
}

func writeJSONBytes(w http.ResponseWriter, jsonBytes []byte, err error, code int) {
	if err != nil {
		RespondWithError(w, errors.New("unable to marshal response"), http.StatusInternalServerError)
	}

	w.WriteHeader(code)
	if jsonBytes != nil {
		w.Write(jsonBytes)
	}
}

func RespondWithError(w http.ResponseWriter, err error, code int) {
	requestError := m.ModelError{
		Description: fmt.Sprintf(err.Error()),
	}
	RespondJSONObjectWithCode(w, code, requestError)
}

func setCommonHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
}
