# DataStax GraphQL and REST endpoints

## Getting started

The GraphQL endpoints can be used as a standalone webserver or you could plugin
the routes within your HTTP request router.

### Run as a container (GraphQL only)

```bash
docker build -t data-endpoints .
docker run -p 8080:8080 -e "ENDPOINT_HOSTS=<cassandra_hosts_here>" data-endpoints
```

Or to use with a configuration file, create a file with the following contents:

```yaml
hosts:
  # Change to your cluster's hosts
  - 127.0.0.1

# Add your configuration here

```

Then start the endpoints with:

```bash
docker run -p 8080:8080 -v "${PWD}/<your_config_file>.yaml:/root/config.yaml" data-endpoints
```

#### Use with single node, local Cassandra cluster

```bash
docker build -t data-endpoints .

# On Linux (with a cluster started on the docker bridge: 172.17.0.1)
docker run -p 8080:8080 -e "ENDPOINT_HOSTS=172.17.0.1" data-endpoints

# Or (with a cluster bound to 0.0.0.0)
run --network host -e "ENDPOINT_HOSTS=127.0.0.1" data-endpoints

# On macOS (with a cluster bound to 0.0.0.0)
docker run -p 8080:8080 -e "ENDPOINT_HOSTS=host.docker.internal" data-endpoints
```

These host values can also be used in the configuration file approach used in
the previous section.

### Run as a standalone webserver (GraphQL only)

If you want to run this module as a standalone webserver, use:

```bash
# Define the keyspace you want to use
# Start the webserver
go build -o run.exe && ./run.exe --hosts 127.0.0.1 --keyspace store
```

Or your settings can be persisted using a configuration file:

```yaml
hosts:
  - 127.0.0.1
keyspace: store
operations:
  - TableCreate
  - KeyspaceCreate
port: 8080
schema-update-interval: 30s
```

To start the server using a configuration file, use:

```bash
./run.exe --config <your_config_file>.yaml
```

Settings can also be overridden using environment variables prefixed with
`ENDPOINT_`:

```bash
ENDPOINT_HOSTS=127.0.0.1 ENDPOINT_KEYSPACE=store ./run.exe --config <your_config_file>.yaml
```

Note `--start-rest` is not currently implemented.

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
