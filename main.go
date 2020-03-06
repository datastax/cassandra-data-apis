package main

import (
	"encoding/json"
	"fmt"
	"github.com/riptano/data-endpoints/db"
	"github.com/riptano/data-endpoints/schema"
	"log"
	"net/http"

	"github.com/graphql-go/graphql"
	"github.com/julienschmidt/httprouter"
)

// TODO: Make this a configuration setting
var singleKsName = "store";

var excludedKeyspaces = []string{
	"system", "system_auth", "system_distributed", "system_schema", "system_traces", "system_views", "system_virtual_schema",
	"dse_insights", "dse_insights_local", "dse_leases", "dse_perf", "dse_security", "dse_system", "dse_system_local",
	"solr_admin",
}

func isKeyspaceExcluded(ksName string) bool {
	for _, excluded := range excludedKeyspaces {
		if ksName == excluded {
			return true
		}
	}
	return false
}

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

func getHandler(graphqlSchema graphql.Schema) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		result := executeQuery(r.URL.Query().Get("query"), graphqlSchema)
		json.NewEncoder(w).Encode(result)
	}
}

func postHandler(graphqlSchema graphql.Schema) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
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

		result := executeQuery(body.Query, graphqlSchema)
		json.NewEncoder(w).Encode(result)
	}
}

type requestBody struct {
	Query string `json:"query"`
}

func main() {
	dbClient, err := db.NewDb("127.0.0.1")
	if err != nil {
		log.Fatalf("unable to make DB connection: %s", err)
	}

	router := httprouter.New()
	if len(singleKsName) > 0 { // Single keyspace mode (useful for cloud)
		graphqlSchema, err := schema.BuildSchema(singleKsName, dbClient)
		if err != nil {
			log.Fatalf("unable to get keyspace '%s' metadata: %s", singleKsName, err)
		}

		router.GET("/graphql", getHandler(graphqlSchema))
		router.POST("/graphql", postHandler(graphqlSchema))
	} else { // Otherwise, allow all user created keyspaces
		ksNames, err := dbClient.Keyspaces()
		if err != nil {
			log.Fatalf("unable to retrieve keyspace names: %s", err)
		}

		ksSchema, err := schema.BuildKeyspaceSchema(dbClient)
		if err != nil {
			log.Fatalf("unable to build keyspace management schema: %s", err)
		}

		router.GET("/graphql", getHandler(ksSchema))
		router.POST("/graphql", postHandler(ksSchema))

		for _, ksName := range ksNames {
			if isKeyspaceExcluded(ksName) {
				continue
			}

			graphqlSchema, err := schema.BuildSchema(ksName, dbClient)
			if err != nil {
				log.Fatalf("unable to build schema for keyspace '%s': %s", ksName, err)
			}
			router.GET("/graphql/" + ksName, getHandler(graphqlSchema))
			router.POST("/graphql/" + ksName, postHandler(graphqlSchema))
		}
	}

	fmt.Println("Now server is running on port 8080")
	http.ListenAndServe(":8080", router)
}
