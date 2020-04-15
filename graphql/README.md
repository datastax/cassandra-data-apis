# GraphQL API for Apache Cassandra

### Using the Playground

The easiest way to get started is to use the built-in GraphQL playground. After
running the commands from the installation step the GraphQL playground can be
accessed by going to http://localhost:8080/graphql-playground. This will allow
you to create new schema and interact with your GraphQL APIs.

### Creating Schema

Before you can get started using your GraphQL APIs you'll need to create a
keyspace and at least one table. If your Cassandra database already has existing
schema then the server has already imported your schema the you might skip this
step. Otherwise, use the following steps to create new schema.

Inside the playground, navigate to http://localhost:8080/graphql-schema

First create a keyspace by executing:

```graphql
mutation {
  createKeyspace(
    name:"library", # The name of your keyspace
    dcs: {name:"dc1", replicas: 3} # Controls how your data is replicated
  )
}
```

After the keyspace is created you can create a table by executing:

```graphql
mutation {
  books: createTable(
    keyspaceName:"library", 
    tableName:"books", 
    partitionKeys: [ # The keys required to access your data
      { name: "title", type: {basic: TEXT} }
    ]
    values: [ # The values associated with the keys
      { name: "author", type: {basic: TEXT} }
    ]
  )
  authors: createTable(
    keyspaceName:"library", 
    tableName:"authors", 
    partitionKeys: [
      { name: "name", type: {basic: TEXT} }
    ]
    clusteringKeys: [ # TODO: explain this
      { name: "title", type: {basic:TEXT} }
  	]
  )
}
```

Or you can create the schema using `cqlsh` and the server will automatically
pick up your schema changes.

```cql
CREATE KEYSPACE library WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3'};

CREATE TABLE library.books (
    title text PRIMARY KEY,
    author text
);

CREATE TABLE library.authors (
    name text,
    title text,
    PRIMARY KEY (name, title)
) WITH CLUSTERING ORDER BY (title ASC);
```

### Path Layout

By default, this is how the server paths are structured:

* `/graphgl-playground`: Provides an interactive playground to explore your
GraphQL APIs.
* `/graphgl-schema`: Provides an API for exploring and creating schema, in
database terminology this is know as: Data Definition Language (DDL). In
Cassandra these are the queries that create, modify, drop keyspaces and
tables e.g. `CREATE KEYSPACE ...`, `CREATE TABLE ...`, `DROP TABLE ...`.
* `/graphql/<keyspace>`: Provides an API for querying and modifying your Cassandra
tables using GraphQL fields.

**Tip:** If your application wants to focus on a single keyspace then the
environment variable `DATA_API_KEYSPACE=<your keyspace>` can be added to the
`docker run -e DATA_API_KEYSPACE=<your keyspace> ...` command. In this mode, the
provided keyspace's GraphQL API will live under `/graphql` and other keyspaces
in your database will no longer be accessible via `/graphql/<keyspace>`.

### API Generation

For each table in your Cassandra schema, several fields are created for handling
queries and mutations.  For example, the `books` table's GraphQL schema looks
like this:

```graphql
schema {
  query: TableQuery
  mutation: TableMutation
}

type TableQuery {
  books(value: BooksInput, orderBy: [BooksOrder], options: QueryOptions): BooksResult
  booksFilter(filter: BooksFilterInput!, orderBy: [BooksOrder], options: QueryOptions): BooksResult
}

type TableMutation {
  insertBooks(value: BooksInput!, ifNotExists: Boolean, options: UpdateOptions): BooksMutationResult
  updateBooks(value: BooksInput!, ifExists: Boolean, ifCondition: BooksFilterInput, options: UpdateOptions): BooksMutationResult
  deleteBooks(value: BooksInput!, ifExists: Boolean, ifCondition: BooksFilterInput, options: UpdateOptions): BooksMutationResult
}
```

#### Queries:

* `books()`: Query book values by equality. If no `value` argument is provided
  then the first 100 (default pagesize) values are returned.

* `booksFilter`: Query book values by filtering the result with relational
  operators e.g.  greater than (`gt`), less than (`lt`) etc. `books()` should be
  prefer if your queries don't require the use of these more complex operators.

#### Mutations:
  
* `insertBooks()`: Insert a new book. This is an "upsert" operation that will
  update the value of exiting books if they already exists unless `ifNotExists`
  is set to `true`. Using `ifNotExists` uses a lightweight transaction (LWT)
  which adds significant overhead to the mutation.
* `updateBooks()`: Update an existing book. This is also an "upsert" and will
  create a new book if one doesn't exists unless `ifExists` is set to `true`.
  Using `ifExists` or `ifCondition` uses a lightweight transaction (LWT) which
  adds significant overhead to the mutation.
* `deleteBooks()`: Deletes a book. Using `ifExists` or `ifCondition` uses a
  lightweight transaction (LWT) which adds significant overhead to the mutation.

As more tables are added to a keyspace additional fields will be added to the
`TableQuery` and `TableMutation` types to handle queries and mutations for those
new tables.

### Using the API

Using the schema has been created in the previous you start adding and querying
values. Navigate to your keyspace inside the playground by going to
http://localhost:8080/graphql/library and start adding some entries.


#### Insert Books

```graphql
mutation {
  moby: insertBooks(value: {title:"Moby Dick", author:"Herman Melville"}) {
    value {
      title
    }
  }
  catch22: insertBooks(value: {title:"Catch-22", author:"Joseph Heller"}) {
    value {
      title
    }
  }
}
```


#### Query Books

To query those values you can run the following.

```graphql
query {
    books {
      values {
      	title
      	author
      }
    }
}
```

```json
{
  "data": {
    "books": {
      "values": [
        {
          "author": "Joseph Heller",
          "title": "Catch-22"
        },
        {
          "author": "Herman Melville",
          "title": "Moby Dick"
        }
      ]
    }
  }
}
```

#### Query a Single Book

A specific book can be queried by providing a key value.

```graphql
query {
    books (value: {title:"Moby Dick"}) {
      values {
      	title
      	author
      }
    }
}
```

```json
{
  "data": {
    "books": {
      "values": [
        {
          "author": "Herman Melville",
          "title": "Moby Dick"
        }
      ]
    }
  }
}
```

## Using Apollo Client

This is a basic guide to get started with Apollo Client 2.x in node. First
you'll need to install dependencies. These examples utilize the `books` schema
created in the previous schema section.

### Node

```sh
npm install apollo-client apollo-cache-inmemory apollo-link-http \
      apollo-link-error apollo-link graphql-tag --save
```

After the dependencies are installed you should be able to connect to you local
server.

```js
const { HttpLink } = require('apollo-link-http')
const { InMemoryCache } = require('apollo-cache-inmemory')
const { ApolloClient } = require('apollo-client')
const fetch = require('node-fetch')
const gql = require('graphql-tag')

const client = new ApolloClient({
  link: new HttpLink({
    uri: 'http://localhost:8080/graphql/library',
    fetch: fetch
  }),
  cache: new InMemoryCache()
})

const query = 
client.query({ 
  query: gql`
    {
       books {
         values {
           author
         }
       }
    }
  `
}).then(result => {
  console.log(result)
})
```

### In the Browser

The Apollo Client can also be used inside the browser:
https://jsfiddle.net/1n8f0cgt/, but [CORS] needs to be enabled. This can be
done by starting the Docker image with the environment variable `-e
DATA_API_ACCESS_CONTROL_ALLOW_ORIGIN=*`


## API Features

### Filtering

### Paging

### Conditional Inserts

### Conditional Updates

### Consistency

### Time To Live (TTL)

## Advance Configuration

[CORS]: https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS
