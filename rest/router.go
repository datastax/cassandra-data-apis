package rest

import (
	"encoding/json"
	"fmt"
	"github.com/datastax/cassandra-data-apis/db"
	"github.com/julienschmidt/httprouter"
	"net/http"
)

// jsonResult provides a basic root object in order to avoid using a scalar at root level.
type jsonResult struct {
	Meta interface{} `json:"meta"`
	Data interface{} `json:"data"`
}

// ApiRouter gets the router for the REST API
func ApiRouter(dbClient *db.Db) *httprouter.Router {
	router := httprouter.New()
	router.GET("/", index)
	router.GET("/keyspaces", keyspacesHandler(dbClient))
	return router
}

func keyspacesHandler(dbClient *db.Db) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		data, err := dbClient.Keyspaces()
		writeResponse(w, &data, err)
	}
}

func writeResponse(w http.ResponseWriter, data interface{}, err error) {
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(&jsonResult{Data: data}); err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
	}
}

func writeErrorResponse(w http.ResponseWriter, errorCode int, errorMsg string) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(errorCode)
	_ = json.NewEncoder(w).Encode(errorMsg)
}

func index(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// TODO: Print API paths
	if _, err := fmt.Fprint(w, "Welcome to the REST API!\n"); err != nil {
		panic(err)
	}
}
