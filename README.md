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
documentation](/docs/graphql/README.md) for getting started.

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

# See the "Settings" section for additional configuration

```

Then start docker with:

```sh
docker run -p 8080:8080 -v "${PWD}/<your_config_file>.yaml:/root/config.yaml" datastaxlabs/cassandra-data-apis
```

### Settings

| Name | Type | Env. Variable | Description |
| --- | --- | --- | --- |
| hosts                  | strings  | DATA_API_HOSTS                  | Hosts for connecting to the database |
| keyspace               | string   | DATA_API_KEYSPACE               | Only allow access to a single keyspace |
| excluded-keyspaces     | strings  | DATA_API_EXCLUDED_KEYSPACES     | Keyspaces to exclude from the endpoint |
| username               | string   | DATA_API_USERNAME               | Connect with database user |
| password               | string   | DATA_API_PASSWORD               | Database user's password |
| operations             | strings  | DATA_API_OPERATIONS             | A list of supported schema management operations. See below. (default `"TableCreate, KeyspaceCreate"`) |
| request-logging        | bool     | DATA_API_REQUEST_LOGGING        | Enable request logging |
| schema-update-interval | duration | DATA_API_SCHEMA_UPDATE_INTERVAL | Interval in seconds used to update the graphql schema (default `10s`) |
| start-graphql          | bool     | DATA_API_START_GRAPHQL          | Start the GraphQL endpoint (default `true`) |
| graphql-path           | string   | DATA_API_GRAPHQL_PATH           | GraphQL endpoint path (default `"/graphql"`) |
| graphql-port           | int      | DATA_API_GRAPHQL_PORT           | GraphQL endpoint port (default `8080`) |
| graphql-schema-path    | string   | DATA_API_GRAPHQL_SCHEMA_PATH    | GraphQL schema management path (default `"/graphql-schema"`) |

#### Configuration Types

The `strings` type expects a comma-delimited list e.g. `127.0.0.1, 127.0.0.2,
127.0.0.3` when using environment variables or a command flag, and it expects
an array type when using a configuration file.

YAML:

```yaml
--- 
host: 
  - "127.0.0.1"
  - "127.0.0.2"
  - "127.0.0.3"

```

JSON:
```json
{
	"hosts": ["127.0.0.1", "127.0.0.2", "127.0.0.3"]
}
```

#### Schema Management Operations

| Operation | Allows |
| --- | --- |
| `TableCreate`    | Creation of tables    |
| `TableDrop`      | Removal of tables     |
| `TableAlterAdd`  | Add new table columns |
| `TableAlterDrop` | Remove table columns  |
| `KeyspaceCreate` | Creation of keyspaces |
| `KeyspaceDrop`   | Removal of keyspaces  |

## Building 

This section is mostly for developers. Pre-built docker image recommended.

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

# With a cluster bound to 0.0.0.0
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

Your settings can be persisted using a configuration file:

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

### Plugin the routes within your HTTP request router

#### Installation

```
go get github.com/datastax/cassandra-data-apis
```

#### Using the API

To add the routes to your existing HTTP request router, use:

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
