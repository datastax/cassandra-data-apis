package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/riptano/data-endpoints/db"
	"github.com/riptano/data-endpoints/schema"

	"github.com/graphql-go/graphql"
	"github.com/julienschmidt/httprouter"
)

func executeQuery(query string, schema graphql.Schema) *graphql.Result {
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
	})
	if len(result.Errors) > 0 {
		fmt.Printf("wrong result, unexpected errors: %v", result.Errors)
	}
	return result
}

type requestBody struct {
	Query string `json:"query"`
}

func main() {
	dbClient, err := db.NewDb("127.0.0.1")
	if err != nil {
		fmt.Println("Unable to make DB connection")
		return
	}

	s, err := schema.BuildSchema("store", dbClient)

	if err != nil {
		fmt.Printf("Unable to build schema: %s", err)
		return
	}

	router := httprouter.New()
	router.GET("/graphql", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		result := executeQuery(r.URL.Query().Get("query"), s)
		json.NewEncoder(w).Encode(result)
	})
	router.POST("/graphql", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		if r.Body == nil {
			http.Error(w, "No request body", 400)
			return
		}

		var body requestBody
		err := json.NewDecoder(r.Body).Decode(&body)
		if err != nil {
			http.Error(w, "Request body is invalid", 400)
			return
		}

		result := executeQuery(body.Query, s)
		json.NewEncoder(w).Encode(result)
	})

	fmt.Println("Now server is running on port 8080")
	http.ListenAndServe(":8080", router)
}
