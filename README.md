# Data APIs for Apache Cassandra

Easy to use APIs for accessing data stored in Apache Cassandra. 

These APIs can be used as a standalone server using either Docker or manually
running a server. They can also be embedded in existing applications using HTTP
routes. 

Currently, this project provides a GraphQL API. Other API types are possible in
the future.

## Getting Started

### Installation

```sh
docker pull datastaxlabs/cassandra-data-apis
docker run --rm -d -p 8080:8080 -e DATA_API_HOSTS=<cassandra_hosts_here> datastaxlabs/cassandra-data-apis
```

You can also manually build the docker image and/or the server using the
[instructions](#building) below.


### Using GraphQL

By default, a GraphQL endpoint is started. Use the [GraphQL
documentation](/graphql/README.md) for getting started.

## Configuration

Configuration for Docker can be done using either environment variables, a
mounted configuration file, or both.

Add additional configuration using environment variables by adding them to the
`docker run` command.

```
docker run -e DATA_API_HOSTS=127.0.0.1 -e DATA_API_KEYSPACE=example ...
```

### Using a configuration file

To use a configuration file, create a file with the following contents:

```yaml
hosts:
  # Change to your cluster's hosts
  - 127.0.0.1
# keyspace: example
# username: cassandra
# password: cassandra

# Additional configuration here

```

Then start docker with:

```sh
docker run -p 8080:8080 -v "${PWD}/<your_config_file>.yaml:/root/config.yaml" cassandra-data-apis
```

### Settings

| Name | Type | Env. Variable | Description |
| --- | --- | --- | --- |
| hosts                  | strings  | DATA_API_HOSTS                  | Hosts for connecting to the database |
| username               | string   | DATA_API_USERNAME               | connect with database username |
| keyspace               | string   | DATA_API_KEYSPACE               | Only allow access to a single keyspace |
| excluded-keyspaces     | strings  | DATA_API_EXCLUDED_KEYSPACES     | Keyspaces to exclude from the endpoint |
| password               | string   | DATA_API_PASSWORD               | Database user's password |
| operations             | strings  | DATA_API_OPERATIONS             | List of supported table and keyspace management operations. options: TableCreate,TableDrop,TableAlterAdd,TableAlterDrop,KeyspaceCreate,KeyspaceDrop (default [TableCreate,KeyspaceCreate]) |
| request-logging        | bool     | DATA_API_REQUEST_LOGGING        | Enable request logging |
| schema-update-interval | duration | DATA_API_SCHEMA_UPDATE_INTERVAL | interval in seconds used to update the graphql schema (default 10s) |
| start-graphql          | bool     | DATA_API_START_GRAPHQL          | start the GraphQL endpoint (default true) |
| graphql-path           | string   | DATA_API_GRAPHQL_PATH           | Path for the GraphQL endpoint (default "/graphql") |
| graphql-port           | int      | DATA_API_GRAPHQL_PORT           | Port for the GraphQL endpoint (default 8080) |
| graphql-schema-path    | string   | DATA_API_GRAPHQL_SCHEMA_PATH    | Path for the GraphQL schema management (default "/graphql-schema") |

## Building 

This section is mostly for developers. It's recommended that you use the
pre-built docker image.

### Building the Docker Image

```bash
cd <path_to_data-apis>/cassandra-data-apis
docker build -t cassandra-data-apis .
```

### Run locally with single node, local Cassandra cluster

```bash
cd <path_to_data-apis>/cassandra-data-apis
docker build -t cassandra-data-apis .

# On Linux (with a cluster started on the docker bridge: 172.17.0.1)
docker run -p 8080:8080 -e "DATA_API_HOSTS=172.17.0.1" cassandra-data-apis

# Or (with a cluster bound to 0.0.0.0)
run --network host -e "DATA_API_HOSTS=127.0.0.1" cassandra-data-apis

# On macOS (with a cluster bound to 0.0.0.0)
docker run -p 8080:8080 -e "DATA_API_HOSTS=host.docker.internal" cassandra-data-apis
```

These host values can also be used in the configuration file approach used in
the previous section.

### Build and run as a standalone webserver

If you want to run this module as a standalone webserver, use:

```bash
# Define the keyspace you want to use
# Start the webserver
go build run.exe && ./run.exe --hosts 127.0.0.1 --keyspace store
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
`DATA_API_`:

```bash
DATA_API_HOSTS=127.0.0.1 DATA_API_KEYSPACE=store ./run.exe --config <your_config_file>.yaml
```

Note `--start-rest` is not currently implemented.

### Plugin the routes within your HTTP request router

#### Installation

```
go get github.com/datastax/cassandra-data-apis
```

#### Using the API

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
