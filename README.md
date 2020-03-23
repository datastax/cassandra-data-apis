# DataStax GraphQL and REST endpoints

## Getting started

The GraphQL endpoints can be used as a standalone webserver or you could plugin the routes within your HTTP request router.

### Run as a standalone webserver

If you want to run this module as a standalone webserver, use:

```bash
# Define the keyspace you want to use
export SINGLE_KEYSPACE=store
# Start the webserver
go build -o run.exe && ./run.exe
```

Note that in the future the deployment model will be containerized.

### Plugin the routes within your HTTP request router

If you want to add the routes to your existing HTTP request router, use:

```go
cfg := endpoint.NewEndpointConfig("your.first.contact.point", "your.second.contact.point")
// Setup config here using your env variables
endpoint, err := cfg.NewEndpoint()
if err != nil {
	log.Fatalf("unable create new endpoint: %s", err)
}
keyspace := "store"
routes, err = endpoint.RoutesKeyspaceGraphQL("/graphql", keyspace)
// Setup routes on your http router
```