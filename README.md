# Data APIs for Apache Cassandra (Deprecated)

(Project has been moved to Stargate)

Easy to use APIs for accessing data stored in Apache Cassandra. 

These APIs can be used as a standalone server using either Docker or manually
running a server. They can also be embedded in existing applications using HTTP
routes. 

Currently, this project provides GraphQL APIs. Other API types are possible in
the future.

## Getting Started

### Installation

```sh
docker pull datastaxlabs/cassandra-data-apis
docker run -p 8080:8080 -e DATA_API_HOSTS=<cassandra_hosts_here> datastaxlabs/cassandra-data-apis
```

You can also manually build the docker image and/or the server using the
[instructions](#building) below.

#### Running in the background

You can start the container in detached mode by using `-d` and `--rm` flags.

```sh
docker run --rm -d -p 8080:8080 -e DATA_API_HOSTS=<cassandra_hosts_here> datastaxlabs/cassandra-data-apis
```

#### Running on macOS or Windows for development purposes

When using Docker for Desktop, if your Cassandra instance is listening on the loopback address `127.0.0.1`,
you can use `host.docker.internal` name which resolves to the internal IP address used by the host.

```sh
docker run -p 8080:8080 -e DATA_API_HOSTS=host.docker.internal datastaxlabs/cassandra-data-apis
```

### Using GraphQL

By default, a GraphQL endpoint is started and will generate a GraphQL schema per keyspace. You need at least one
user-defined keyspace in your database to get started.  

Use the [GraphQL documentation](/docs/graphql/README.md) for getting started.

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
| ssl-enabled            | bool     | DATA_API_SSL_ENABLED            | Enable SSL (client-to-node encryption)? |
| ssl-ca-cert-path       | string   | DATA_API_SSL_CA_CERT_PATH       | SSL CA certificate path |
| ssl-client-cert-path   | string   | DATA_API_SSL_CLIENT_CERT_PATH   | SSL client certificate path |
| ssl-client-key-path    | string   | DATA_API_SSL_CLIENT_KEY_PATH    | SSL client private key path |
| ssl-host-verification  | string   | DATA_API_SSL_HOST_VERIFICATION  | Verify the peer certificate? It is highly insecure to disable host verification (default `true`) |
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

#### TLS/SSL

##### HTTPS

The API endpoint does not currently support HTTPS natively, but it can be handled by a gateway or
reverse proxy. More information about protecting the API endpoint can be found in this
[documentation][protecting].

##### Client-to-node Encryption

By default, traffic between the API endpoints and the database servers is not encrypted. To secure
this traffic you will need to generate SSL certificates and enable SSL on the database servers. More
information about enabling SSL (client-to-node encryption) on the database servers can be found in
this [documentation][client-to-node]. After SSL is enabled on the database servers use the
`ssl-enabled` option, along with `ssl-ca-cert-path` to enable secure connections. `ssl-ca-cert-path`
is a path to the chain of certificates used to generate the database server's certificates. The
certificate chain is used by API endpoints to verify the database server's certificates.  The
`ssl-client-cert-path` and `ssl-client-key-path` options are not required, but can be use to provide
client-side certificates that are used by the database servers to authenticate and verify the API
servers, this is known as mutual authentication. 

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
docker run --network host -e "DATA_API_HOSTS=127.0.0.1" cassandra-data-apis

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

## License

© DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

[protecting]: /docs/protecting/README.md
[client-to-node]: https://docs.datastax.com/en/cassandra-oss/3.x/cassandra/configuration/secureSSLClientToNode.html
