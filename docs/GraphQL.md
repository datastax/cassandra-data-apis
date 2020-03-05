# GraphQL API design

## GraphQL types:

`Int` (signed 32-bit integer)

`Float` (double precision)

`String` (UTF-8)

`Boolean`

`ID` (non-human-readable string identifier)

### Modifiers

`type!` Not null

`[type]` List of `type`

### Objects

```graphql
type SomeType {
 field1: Int!
 field2: String
}
```

## Cassandra types:

`ascii` (US-ASCII chars only)

`text` (UTF-8)

`varchar` (UTF-8)

`tinyint` (signed 8-bit integer)

`smallint` (signed 16-bit integer)

`int` (signed 32-bit integer)

`bigint` (signed 64-bit integer)

`varint` (arbitrary-precision integer, a signed array of bytes)

`decimal` (variable-precision decimal, a varint with a signed 32-bit integer scale)

`float` (single precision)

`double` (double precision)

`date` (unsigned 32-bit integer of days since the epoch)

`duration` (month (signed 32-bit integer), days (signed 32-bit iteger), nanoseconds (signed 64-bit integer))

`time` (signed 64-bit integer of nanoseconds since midnight)

`timestamp` (signed 64-bit integer of milliseconds since epoch)

`uuid` (v4 random UUID)

`timeuuid` (v1 time-based UUID)

`blob` (byte array)

`boolean`

`counter` (signed 64-bit integer)

`inet` (IPv4/IPv6 address)

`list` (array of single type)

`map` (mapping of key/value that have single key type and single value type)

`set` (unique array of single type)

`tuple` (array of multiple types)

`udt` (mapping of key/value that have a string key and multiple value types)

## Potential type mapping

### Simple types

| CQL type   |  GraphQL type                                                                                   |
|------------|-------------------------------------------------------------------------------------------------|
| `ascii`    | `String`                                                                                        |
| `text`     | `String`                                                                                        |
| `varchar`  | `String`                                                                                        |
| `tinyint`  | `Int`                                                                                           |
| `smallint` | `Int`                                                                                           |
| `int`      | `Int`                                                                                           |
| `bigint`   | `String`                                                                                        |
| `varint`   | `String`                                                                                        |
| `float`    | `Float`                                                                                         |
| `double`   | `Float`                                                                                         |
| `decimal`  | `String`                                                                                        |
| `date`     | `String` of the form `"yyyy-mm-dd"`                                                             |
| `duration` | `String` in ISO 8601 formats or digits and units, for example:  `P21Y5M`, `12h30m`, `33us1ns`   |
| `time`     | `String` of the form `"hh:mm:ss[.fff]"`                                                         |
| `timestamp`| `String` of the form `"yyyy-mm-dd [hh:MM:ss[.fff]][+/-NNNN]"`                                   |
| `uuid`     | `String` of the form `"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"` where x are hex digits `[a-f0-9]` |
| `timeuuid` | `String` of the form `"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"` where x are hex digits `[a-f0-9]` |
| `blob`     | `String` of base64 encoded data                                                                 |
| `boolean`  | `Boolean`                                                                                       |
| `counter`  | `String`                                                                                        |
| `inet`     | `String`                                                                                        |

One possible idea here is to "tag" types using objects. `bigint` and other
similar CQL specific types could be wrapped in a type instead of using just
`String`:

```graphql
type Bigint {
  value: String!
}
```

This would also apply to other types that don't fit neatly into GraphQL's scalar
types.

We can also use `scalar Bigint` to define a custom type.


### Complex types

| CQL type   |  GraphQL type                     |
|------------|-----------------------------------|
| `list`     | `[<type>]`                        |
| `set`      | `[<type>]`                        |
| `map`      | `[<defined_key_value_type>]`      |
| `tuple`    | `[<union_of_all_included_types>]` |
| `udt`      | Custom defined object type        |


#### Supporting `map`

Only maps of the form `map<varchar, ...>` are supported unless we marshal other
types into a string format.

```graphql
# map<varchar, varchar>

type MapPair { # This will need to have a unique name
  key: String!
  value: String!
}

[MapPair]
# ...

```

#### Supporting `tuple`

```graphql
# tuple<varchar, int, boolean, blob>

union TupleType = String | Int | Boolean # This will need to have a unique name

[TupleType]
```

#### Supporting UDTs

```graphql
# CREATE TYPE address (street varchar, zipcode int, state varchar)

type Address {
  street: String!
  zipcode: Int!
  state: String!
}
```

## API design

Mutations should only be done at the top-level according to the spec:
http://spec.graphql.org/June2018/#sec-Normal-and-Serial-Execution

To me this means we need to use a directory hierarchy to model the different
mutations to keyspaces, tables, and CRUD table operations.

### Root (Keyspace management)

Keyspaces live in directories under `/graphql`:  `graphql/keyspace1`,
`graphql/keyspace2`, etc.

Path: `/graphql`

```graphql
type DataCenter {
  name: String!
  replicas: Int!
}

type Keyspace {
  name: String!
  dcs: [DataCenter]!
}

schema {
  query: Query
  mutation: Mutation
}

type Query {
  keyspaces: [Keyspace]
  keyspace(name: String!): Keyspace
  # ...
}

type Mutation {
  createKeyspace(name: String!, dcs: [DataCenter]): Keyspace
  dropKeyspace(name: String!): Keyspace
}
```

### Table management and CRUD operations

Table management would live in under each keyspace directory along with the
tables CRUD operations.

I think it makes sense to have all the tables CRUD operations live in a combined
keyspace GraphQL schema so that result can be combined together in different
ways. Note: GraphQL queries run in parallel while mutations do not.

Path: `graphql/<keyspaceName>`

```graphql
enum BasicType {
  VARCHAR
  INT
  UUID
  # ...
}

type DataType {
  basic: BasicType!
  subTypes: [DataType]
}

type Column {
  name: String!
  tableName: String!
  type: DataType!
}

type Table {
  name: String!
  keyspaceName: String!
  primaryKey: [Column]!
  clusteringKey: [Column]
  values: [Column]
  # ...
}

schema {
  query: Query
  mutation: Mutation
}

type Query {
  tables: [Table]
  table(name: String): Table
  # ...
}

type Mutation {
  createTable(name: String!, primaryKey: [Column]!, clusteringKey: [Column], values: [Column]): Table
  dropTable(name: String!)

  # Do we put the CRUD operations here?
  # ...
}
```

#### Example

It would be cool to have some case mapping functions so that we can have both
idiomatic C*/DSE table names (`im_a_table`) and GraphQL names (proper and
camelcase) simultaneously.

Path: `/graphql/cycling`

```cql
CREATE KEYSPACE cycling
  WITH REPLICATION = {
   'class' : 'NetworkTopologyStrategy',
   'datacenter1' : 1
  } ;

CREATE TABLE cycling.cyclist_name (
   id UUID PRIMARY KEY,
   lastname text,
   firstname text );

CREATE TABLE cycling.cyclist_category (
   category text,
   points int,
   id UUID,
   lastname text,
   PRIMARY KEY (category, points))
```

```graphql
type CyclistName {
  id: String!
  lastname: String
  firstname: String
}

type CyclistCategory {
  category: String!
  points: Int!
  id: String
  lastname: String
}

schema {
  query: Query
  mutation: Mutation
}

# We can determine the select expression columns from the fields in the graphql
# query. Not sure how we're going to handle other types of expressions such as
# aggregates (Maybe the can be their own query: `countCyclistName()`?).

# We can potentially handle optional clustering filters by making them
# non-nullable parameters?

# Things to think about:
# * Expression
#   * Aggregates (built-in: count(), min(), max())
#   * DISTINCT (Do we need to support this?)
#   * Do we care about handling user define function/aggregates?
# * Filtering
#   * ORDER BY
#   * LIMIT
#   * Static filters

type Query {
  cyclistName(id: String!): CyclistName
  cyclistCategory(category: String!, points: Int!): CyclistCategory
}

# Do we need both insert and update could we just have an upsert?
type Mutation {
  # ...
  addCyclistName(id: String!, lastname: String, firstname: String): CyclistName
  updateCyclistName(id: String!, lastname: String, firstname: String): CyclistName
  deleteCyclistName(id: String!): CyclistName

  addCyclistCategory(category: String!, points: Int!, id: String, lastname: String): CyclistCategory
  updateCyclistCategory(category: String!, points: Int!, id: String, lastname: String): CyclistCategory
  deleteCyclistCategory(category: String!, points: Int!): CyclistCategory
}
```
