package main

import (
	"fmt"
	"github.com/riptano/data-endpoints/datastax"
	"github.com/riptano/data-endpoints/graphql"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/julienschmidt/httprouter"
)

func getEnvOrDefault(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func main() {
	hosts := getEnvOrDefault("DB_HOSTS", "127.0.0.1")
	singleKsName := os.Getenv("SINGLE_KEYSPACE")

	cfg := datastax.NewEndpointConfig(strings.Split(hosts, ",")...)
	cfg.DbUsername = os.Getenv("DB_USERNAME");
	cfg.DbPassword = os.Getenv("DB_PASSWORD");

	endpoint, err := cfg.NewEndpoint()
	if err != nil {
		log.Fatalf("unable create new service: %s", err)
	}

	var routes []graphql.Route
	if singleKsName != "" { // Single keyspace mode (useful for cloud)
		routes, err = endpoint.RoutesKeyspaceGql("/graphql", singleKsName)
	} else {
		routes, err = endpoint.RoutesGql("/graphql")
	}

	if err != nil {
		log.Fatalf("unable to generate graphql routes: %s", err)
	}

	router := httprouter.New()
	for  _, route := range routes {
		router.HandlerFunc(route.Method, route.Pattern, route.HandlerFunc)
	}

	finish := make(chan bool)
	go listenAndServe(router, 8080)
	// go listenAndServe(rest.ApiRouter(dbClient), 8081)
	<-finish
}

func listenAndServe(router *httprouter.Router, port int) {
	fmt.Printf("Start listening on %d\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), router))
}
