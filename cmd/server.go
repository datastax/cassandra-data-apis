package cmd

import (
	"errors"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/riptano/data-endpoints/config"
	"github.com/riptano/data-endpoints/endpoint"
	"github.com/riptano/data-endpoints/graphql"
	"github.com/riptano/data-endpoints/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	log2 "log"
	"net/http"
	"os"
)

const defaultGraphQLPath = "/graphql"
const defaultGraphQLSchemaPath = "/graphql-schema"
const defaultRESTPath = "/todo"

// Environment variables prefixed with "ENDPOINT_" can override settings e.g. "ENDPOINT_HOSTS"
const envVarPrefix = "endpoint"

var cfgFile string
var logger log.Logger
var cfg *endpoint.DataEndpointConfig

var serverCmd = &cobra.Command{
	Use:   os.Args[0] + " --hosts [HOSTS] [--start-graph|--start-rest] [OPTIONS]",
	Short: "GraphQL and REST endpoints for Apache Cassandra",
	Args: func(cmd *cobra.Command, args []string) error {
		// TODO: Validate GraphQL/REST paths, should they be disjointed?

		hosts := viper.GetStringSlice("hosts")
		if  len(hosts) == 0 {
			return errors.New("hosts are required")
		}

		startGraphQL := viper.GetBool("start-graphql")
		startREST := viper.GetBool("start-rest")
		if !startGraphQL && !startREST {
			return errors.New("at least one endpoint type should be started")
		}

		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		endpoint := createEndpoint()

		graphqlPort := viper.GetInt("graphql-port")
		restPort := viper.GetInt("rest-port")

		startGraphQL := viper.GetBool("start-graphql")
		startREST := viper.GetBool("start-rest")

		if graphqlPort == restPort {
			router := httprouter.New()
			endpointNames := ""
			if startGraphQL {
				addGraphQLRoutes(router, endpoint)
				endpointNames += "GraphQL"
			}
			if startREST {
				addRESTRoutes(router, endpoint)
				if endpointNames != "" {
					endpointNames += "/"
				}
				endpointNames += "REST"
			}
			listenAndServe(maybeAddRequestLogging(router), graphqlPort, endpointNames)
		} else {
			finish := make(chan bool)
			if startGraphQL {
				router := httprouter.New()
				addGraphQLRoutes(router, endpoint)
				go listenAndServe(maybeAddRequestLogging(router), graphqlPort, "GraphQL")
			}
			if startREST {
				router := httprouter.New()
				addRESTRoutes(router, endpoint)
				go listenAndServe(maybeAddRequestLogging(router), restPort, "REST")
			}
			<-finish
		}
	},
}

// Execute start GraphQL/REST endpoints
func Execute() {
	zapLogger, err := zap.NewProduction()
	if err != nil {
		log2.Fatalf("unable to initialize logger: %v", err)
	}

	logger = log.NewZapLogger(zapLogger)

	flags := serverCmd.PersistentFlags()

	// General endpoint flags
	flags.StringVarP(&cfgFile, "config", "c", "", "config file")
	flags.StringSliceP("hosts", "t", nil, "hosts for connecting to the database")
	flags.StringP("username", "u", "", "connect with database username")
	flags.StringP("password", "p", "", "database user's password")

	flags.String("keyspace", "", "only allow access to a single keyspace")
	flags.Bool("request-logging", false, "enable request logging")
	flags.StringSlice("excluded-keyspaces", nil, "keyspaces to exclude from the endpoint")
	flags.Duration("schema-update-interval", endpoint.DefaultSchemaUpdateDuration, "interval in seconds used to update the graphql schema")
	flags.StringSlice("operations", []string{
		"TableCreate",
		"KeyspaceCreate",
	}, "list of supported table and keyspace management operations. options: TableCreate,TableDrop,TableAlterAdd,TableAlterDrop,KeyspaceCreate,KeyspaceDrop")

	// GraphQL specific flags
	flags.Bool("start-graphql", true, "start the GraphQL endpoint")
	flags.String("graphql-path", defaultGraphQLPath, "path for the GraphQL endpoint")
	flags.String("graphql-schema-path", defaultGraphQLSchemaPath, "path for the GraphQL schema management")
	flags.Int("graphql-port", 8080, "port for the GraphQL endpoint")

	// REST specific flags
	flags.Bool("start-rest", false, "start the REST endpoint")
	flags.String("rest-path", defaultRESTPath, "path for the REST endpoint")
	flags.Int("rest-port", 8080, "port for the REST endpoint")

	flags.VisitAll(func(flag *pflag.Flag) {
		if flag.Name != "config" {
			viper.BindPFlag(flag.Name, flags.Lookup(flag.Name))
		}
	})

	cobra.OnInitialize(initialize)

	viper.SetEnvPrefix(envVarPrefix)
	viper.AutomaticEnv()

	if err := serverCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func createEndpoint() *endpoint.DataEndpoint {
	cfg = endpoint.NewEndpointConfigWithLogger(logger, viper.GetStringSlice("hosts")...)

	updateInterval := viper.GetDuration("schema-update-interval")
	if updateInterval <= 0 {
		updateInterval = endpoint.DefaultSchemaUpdateDuration
	}

	cfg.
		WithDbUsername(viper.GetString("username")).
		WithDbPassword(viper.GetString("password")).
		WithExcludedKeyspaces(viper.GetStringSlice("excluded-keyspaces")).
		WithSchemaUpdateInterval(updateInterval)

	endpoint, err := cfg.NewEndpoint()
	if err != nil {
		logger.Fatal("unable create new endpoint",
			"error", err)
	}

	return endpoint
}

func addGraphQLRoutes(router *httprouter.Router, endpoint *endpoint.DataEndpoint) {
	var routes []graphql.Route
	var err error

	singleKeyspace := viper.GetString("keyspace")
	rootPath := viper.GetString("graphql-path")

	if singleKeyspace != "" {
		routes, err = endpoint.RoutesKeyspaceGraphQL(rootPath, singleKeyspace)
	} else {
		routes, err = endpoint.RoutesGraphQL(rootPath)
	}

	if err != nil {
		logger.Fatal("unable to generate graphql routes",
			"error", err)
	}

	for _, route := range routes {
		router.Handler(route.Method, route.Pattern, route.Handler)
	}

	supportedOps := viper.GetStringSlice("operations")
	ops, err := config.Ops(supportedOps...)
	if err != nil {
		logger.Fatal("invalid supported operation", "operations", supportedOps, "error", err)
	}

	routes, err = endpoint.RoutesSchemaManagementGraphQL(viper.GetString("graphql-schema-path"), ops)

	if err != nil {
		logger.Fatal("unable to generate graphql schema routes",
			"error", err)
	}

	for _, route := range routes {
		router.Handler(route.Method, route.Pattern, route.Handler)
	}
}

func addRESTRoutes(router *httprouter.Router, endpoint *endpoint.DataEndpoint) {
	// TODO: Implement
}

func maybeAddRequestLogging(handler http.Handler) http.Handler {
	if viper.GetBool("request-logging") {
		handler = log.NewLoggingHandler(handler, logger)
	}
	return handler
}

func initialize() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		if err := viper.ReadInConfig(); err == nil {
			logger.Info("using config file",
				"file", viper.ConfigFileUsed())
		}
	}
}

func listenAndServe(handler http.Handler, port int, endpointNames string) {
	logger.Info("server listening",
		"port", port,
		"type", endpointNames)
	err := http.ListenAndServe(fmt.Sprintf(":%d", port), handler)
	if err != nil {
		logger.Fatal("unable to start server",
			"port", port,
			"error", err, )
	}
}
