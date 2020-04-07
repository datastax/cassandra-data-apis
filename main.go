package main

import (
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

var cfgFile string
var logger log.Logger

var serverCmd = &cobra.Command{
	Use:   "data-endpoints",
	Short: "",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := endpoint.NewEndpointConfigWithLogger(logger, viper.GetStringSlice("hosts")...)

		updateInterval := viper.GetDuration("schema-update-interval")
		if updateInterval <= 0 {
			updateInterval = endpoint.DefaultSchemaUpdateDuration
		}

		cfg.
			WithDbUsername(viper.GetString("username")).
			WithDbPassword(viper.GetString("password")).
			WithExcludedKeyspaces(viper.GetStringSlice("excluded-keyspaces")).
			WithSchemaUpdateInterval(updateInterval)

		supportedOps := viper.GetStringSlice("operations")
		ops, err := config.Ops(supportedOps...)
		if err != nil {
			logger.Fatal("invalid supported operation", "operations", supportedOps, "error", err)
		}
		cfg.WithSupportedOperations(ops)

		endpoint, err := cfg.NewEndpoint()
		if err != nil {
			logger.Fatal("unable create new endpoint",
				"error", err)
		}

		var routes []graphql.Route
		singleKeyspace := viper.GetString("keyspace")
		if singleKeyspace != "" {
			routes, err = endpoint.RoutesKeyspaceGraphQL("/graphql", singleKeyspace)
		} else {
			routes, err = endpoint.RoutesGraphQL("/graphql")
		}

		if err != nil {
			logger.Fatal("unable to generate graphql routes",
				"error", err)
		}

		router := httprouter.New()
		for _, route := range routes {
			router.Handler(route.Method, route.Pattern, route.Handler)
		}

		handler := http.Handler(router)

		if viper.GetBool("request-logging") {
			handler = log.NewLoggingHandler(handler, logger)
		}

		finish := make(chan bool)
		go listenAndServe(handler, viper.GetInt("port"), logger)
		// go listenAndServe(rest.ApiRouter(dbClient), 8081)
		<-finish
	},
}

func main() {
	zapLogger, err := zap.NewProduction()
	if err != nil {
		log2.Fatalf("unable to initialize logger: %v", err)
	}

	logger = log.NewZapLogger(zapLogger)

	flags := serverCmd.PersistentFlags()

	flags.StringVarP(&cfgFile, "config", "c", "", "config file")
	flags.StringSliceP("hosts", "t", nil, "hosts for connecting to the database")
	flags.StringP("username", "u", "", "connect with database username")
	flags.StringP("password", "p", "", "database user's password")
	flags.Int( "port", 8080, "port to bind endpoint to")
	flags.String("keyspace","", "only allow access to a single keyspace")
	flags.StringSlice( "operations", []string{
		"TableCreate",
		"KeyspaceCreate",
	}, "list of supported table and keyspace management operations. options: TableCreate,TableDrop,TableAlterAdd,TableAlterDrop,KeyspaceCreate,KeyspaceDrop")
	flags.Bool("request-logging",false, "enable request logging")
	flags.StringSlice("excluded-keyspaces",nil, "keyspaces to exclude from the endpoint")
	flags.Duration("schema-update-interval", endpoint.DefaultSchemaUpdateDuration, "interval in seconds used to update the graphql schema")

	flags.VisitAll(func(flag *pflag.Flag) {
		if flag.Name != "config" {
			viper.BindPFlag(flag.Name, flags.Lookup(flag.Name))
		}
	})

	cobra.OnInitialize(initialize)

	viper.SetEnvPrefix("endpoint")
	viper.AutomaticEnv()

	if err := serverCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
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
