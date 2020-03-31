package main

import (
	"fmt"
	"github.com/riptano/data-endpoints/config"
	"github.com/riptano/data-endpoints/endpoint"
	"github.com/riptano/data-endpoints/graphql"
	"github.com/riptano/data-endpoints/log"
	log2 "log"
	"net/http"
	"os"
	"strconv"
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

	cfg, err := endpoint.NewEndpointConfig(strings.Split(hosts, ",")...)
	if err != nil {
		log2.Fatalf("unable to initialize endpoint config: %s", err)
	}

	logger := cfg.Logger()

	cfg.SetDbUsername(os.Getenv("DB_USERNAME"))
	cfg.SetDbPassword(os.Getenv("DB_PASSWORD"))

	supportedOps := os.Getenv("SUPPORTED_OPERATIONS")
	if supportedOps == "" {
		cfg.SetSupportedOperations(config.TableCreate | config.KeyspaceCreate)
	} else {
		ops, err := config.Ops(strings.Split(supportedOps, ",")...)
		if err != nil {
			logger.Fatal("invalid supported operation", "operations", supportedOps, "error", err)
		}
		cfg.SetSupportedOperations(ops)
	}

	endpoint, err := cfg.NewEndpoint()
	if err != nil {
		logger.Fatal("unable create new endpoint",
			"error", err)
	}

	var routes []graphql.Route
	if singleKsName != "" { // Single keyspace mode (useful for cloud)
		routes, err = endpoint.RoutesKeyspaceGraphQL("/graphql", singleKsName)
	} else {
		routes, err = endpoint.RoutesGraphQL("/graphql")
	}

	if err != nil {
		logger.Fatal("unable to generate graphql routes",
			"error", err)
	}

	router := httprouter.New()
	for _, route := range routes {
		router.HandlerFunc(route.Method, route.Pattern, route.HandlerFunc)
	}

	handler := http.Handler(router)

	if doRequestLogging, _ := strconv.ParseBool(os.Getenv("REQUEST_LOGGING")); doRequestLogging {
		handler = log.NewLoggingHandler(handler, logger)
	}

	finish := make(chan bool)
	go listenAndServe(handler, 8080, logger)
	// go listenAndServe(rest.ApiRouter(dbClient), 8081)
	<-finish
}

func listenAndServe(handler http.Handler, port int, logger log.Logger) {
	logger.Info("server listening",
		"port", port)
	err := http.ListenAndServe(fmt.Sprintf(":%d", port), handler)
	if err != nil {
		logger.Fatal("unable to start server",
			"port", port,
			"error", err, )
	}
}
