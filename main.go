package main

import (
	"fmt"
	"github.com/riptano/data-endpoints/db"
	"github.com/riptano/data-endpoints/graphql"
	"github.com/riptano/data-endpoints/rest"
	"log"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

// TODO: Make this a configuration setting
var singleKsName = "store"

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

func main() {
	dbClient, err := db.NewDb("127.0.0.1")
	if err != nil {
		log.Fatalf("unable to make DB connection: %s", err)
	}

	router := httprouter.New()
	if len(singleKsName) > 0 { // Single keyspace mode (useful for cloud)
		schema, err := graphql.BuildSchema(singleKsName, dbClient)
		if err != nil {
			log.Fatalf("unable to get keyspace '%s' metadata: %s", singleKsName, err)
		}

		router.GET("/graphql", graphql.GetHandler(schema))
		router.POST("/graphql", graphql.PostHandler(schema))
	} else { // Otherwise, allow all user created keyspaces
		ksNames, err := dbClient.Keyspaces()
		if err != nil {
			log.Fatalf("unable to retrieve keyspace names: %s", err)
		}

		ksSchema, err := graphql.BuildKeyspaceSchema(dbClient)
		if err != nil {
			log.Fatalf("unable to build keyspace management graphql: %s", err)
		}

		router.GET("/graphql", graphql.GetHandler(ksSchema))
		router.POST("/graphql", graphql.PostHandler(ksSchema))

		for _, ksName := range ksNames {
			if isKeyspaceExcluded(ksName) {
				continue
			}

			graphqlSchema, err := graphql.BuildSchema(ksName, dbClient)
			if err != nil {
				log.Fatalf("unable to build graphql for keyspace '%s': %s", ksName, err)
			}
			router.GET("/graphql/"+ksName, graphql.GetHandler(graphqlSchema))
			router.POST("/graphql/"+ksName, graphql.PostHandler(graphqlSchema))
		}
	}

	finish := make(chan bool)
	go listenAndServe(router, 8080)
	go listenAndServe(rest.ApiRouter(dbClient), 8081)
	<-finish
}

func listenAndServe(router *httprouter.Router, port int) {
	fmt.Printf("Start listening on %d\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), router))
}
