package main

import (
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/riptano/data-endpoints/config"
	"github.com/riptano/data-endpoints/endpoint"
	"github.com/riptano/data-endpoints/graphql"
	"github.com/riptano/data-endpoints/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	log2 "log"
	"net/http"
	"os"
)

func initConfig() {
}

func main() {
	var cfgFile string
	var dbHosts []string
	var dbUsername string
	var dbPassword string
	var singleKeyspace string
	var supportedOps []string
	var useRequestLogging bool

	var serverCmd = &cobra.Command{
		Use:   "data-endpoints",
		Short: "",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := endpoint.NewEndpointConfig(dbHosts...)
			if err != nil {
				log2.Fatalf("unable to initialize endpoint config: %s", err)
			}

			logger := cfg.Logger()

			cfg.SetDbUsername(dbUsername)
			cfg.SetDbPassword(dbPassword)

			ops, err := config.Ops(supportedOps...)
			if err != nil {
				logger.Fatal("invalid supported operation", "operations", supportedOps, "error", err)
			}
			cfg.SetSupportedOperations(ops)

			endpoint, err := cfg.NewEndpoint()
			if err != nil {
				logger.Fatal("unable create new endpoint",
					"error", err)
			}

			var routes []graphql.Route
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
				router.HandlerFunc(route.Method, route.Pattern, route.HandlerFunc)
			}

			handler := http.Handler(router)

			if useRequestLogging {
				handler = log.NewLoggingHandler(handler, logger)
			}

			finish := make(chan bool)
			go listenAndServe(handler, 8080, logger)
			// go listenAndServe(rest.ApiRouter(dbClient), 8081)
			<-finish
		},
	}

	flags := serverCmd.PersistentFlags()

	// TODO:
	// Log level
	// Configuration file
	// Separate graphql from rest?

	flags.StringVarP(&cfgFile, "config", "c", "", "config file")
	flags.StringSliceVarP(&dbHosts, "hosts", "t", nil, "hosts for connecting to the database")
	flags.StringVarP(&dbUsername, "username", "u", "", "connect with database username")
	flags.StringVarP(&dbPassword, "password", "p", "", "database user's password")
	flags.StringVarP(&singleKeyspace, "keyspace", "k", "", "only allow access to a single keyspace")
	flags.StringSliceVarP(&supportedOps, "operations", "o", []string{
		"TableCreate",
		"KeyspaceCreate",
	}, "list of supported table and keyspace management operations. options: TableCreate,TableDrop,TableAlterAdd,TableAlterDrop,KeyspaceCreate,KeyspaceDrop")
	flags.BoolVarP(&useRequestLogging, "request-logging", "r", false, "enable request logging")

	viper.BindPFlag("hosts", serverCmd.PersistentFlags().Lookup("hosts"))
	viper.BindPFlag("username", serverCmd.PersistentFlags().Lookup("username"))
	viper.BindPFlag("password", serverCmd.PersistentFlags().Lookup("password"))
	viper.BindPFlag("keyspace", serverCmd.PersistentFlags().Lookup("keyspace"))
	viper.BindPFlag("operations", serverCmd.PersistentFlags().Lookup("operations"))

	cobra.MarkFlagRequired(flags, "hosts")

	if err := serverCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
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
