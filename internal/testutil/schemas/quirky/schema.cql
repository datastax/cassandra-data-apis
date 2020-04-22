// A sample schema containing data types that are not supported and edge cases

// Drop and recreate the keyspace
DROP KEYSPACE IF EXISTS quirky;
CREATE KEYSPACE quirky WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use quirky;

CREATE TABLE valid_sample (id uuid PRIMARY KEY, col_one text, col_two int);

CREATE TYPE address (line1 text, line2 text);

// UDTs are not supported
CREATE TABLE tbl_udt (id int PRIMARY KEY, address frozen<address>);

// Tuples are not supported yet
CREATE TABLE tbl_tuple (id int PRIMARY KEY, location tuple<float, float>);

// CQL type duration is not supported yet
CREATE TABLE tbl_duration (id int PRIMARY KEY, d duration);

// Reserved names
CREATE TABLE basic_type (id int PRIMARY KEY, value text);
CREATE TABLE column (id int PRIMARY KEY, value text);
CREATE TABLE column_custom (id int PRIMARY KEY, value text);
CREATE TABLE consistency (id int PRIMARY KEY, value text);
CREATE TABLE data_type (id int PRIMARY KEY, value text);

// Table names that generate the same type name
CREATE TABLE tester_abc (id int PRIMARY KEY, value text);
CREATE TABLE "testerAbc" (id int PRIMARY KEY, value text);

CREATE TABLE "WEIRD_CASE" ("ID" int PRIMARY KEY, "ABCdef" text, "QA_data" text, "abc_XYZ" text);

// Views shouldn't allow direct mutations
CREATE TABLE table_with_view (id int PRIMARY KEY, value text);
CREATE MATERIALIZED VIEW tables_view AS SELECT value FROM table_with_view WHERE value IS NOT NULL PRIMARY KEY(value, id);

