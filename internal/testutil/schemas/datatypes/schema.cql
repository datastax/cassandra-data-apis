// A sample schema containing data types that are supported

// Drop and recreate the keyspace
DROP KEYSPACE IF EXISTS datatypes;
CREATE KEYSPACE datatypes WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use datatypes;

CREATE TABLE scalars (
    id uuid PRIMARY KEY,
    text_col text,
    varchar_col varchar,
    ascii_col ascii,
    tinyint_col tinyint,
    smallint_col smallint,
    int_col int,
    bigint_col bigint,
    varint_col varint,
    decimal_col decimal,
    float_col float,
    double_col double,
    boolean_col boolean,
    uuid_col uuid,
    timeuuid_col timeuuid,
    blob_col blob,
    inet_col inet,
    timestamp_col timestamp,
    time_col time
);

CREATE TABLE collections (
    id uuid PRIMARY KEY ,
    list_text list<text>,
    list_float frozen<list<float>>,
    set_uuid set<uuid>,
    set_int frozen<set<int>>,
    map_bigint_blob map<bigint, blob>,
);

CREATE TABLE table_static (
  id1 uuid,
  id2 int,
  value int,
  value_static int static,
  PRIMARY KEY (id1, id2)
);

CREATE TABLE sample_table (id int PRIMARY KEY);