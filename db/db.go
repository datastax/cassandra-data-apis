package db

import (
	"fmt"
	"github.com/datastax/cassandra-data-apis/config"
	e "github.com/datastax/cassandra-data-apis/errors"
	"github.com/gocql/gocql"
	"time"
)

func ifNotExistsStr(ifNotExists bool) string {
	if ifNotExists {
		return "IF NOT EXISTS "
	}
	return ""
}

func ifExistsStr(ifExists bool) string {
	if ifExists {
		return "IF EXISTS "
	}
	return ""
}

// Db represents a connection to a db
type Db struct {
	session Session
}

type SslOptions struct {
	CaPath           string
	CertPath         string
	KeyPath          string
	HostVerification bool
}

type Config struct {
	Username   string
	Password   string
	SslOptions *SslOptions
}

// NewDb Gets a pointer to a db
func NewDb(config Config, hosts ...string) (*Db, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.PoolConfig = gocql.PoolConfig{
		HostSelectionPolicy: NewDefaultHostSelectionPolicy(),
	}

	// Match DataStax drivers settings
	cluster.ConnectTimeout = 5 * time.Second
	cluster.Timeout = 12 * time.Second

	if config.Username != "" && config.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: config.Username,
			Password: config.Password,
		}
	}

	if config.SslOptions != nil {
		cluster.SslOpts = &gocql.SslOptions{
			CertPath:               config.SslOptions.CertPath,
			KeyPath:                config.SslOptions.KeyPath,
			CaPath:                 config.SslOptions.CaPath,
			EnableHostVerification: config.SslOptions.HostVerification,
		}
	}

	var (
		session *gocql.Session
		err     error
	)

	if session, err = cluster.CreateSession(); err != nil {
		return nil, err
	}
	return NewDbWithSession(&GoCqlSession{ref: session}), nil
}

func NewDbWithSession(session Session) *Db {
	return &Db{
		session: session,
	}
}

func NewDbWithConnectedInstance(session *gocql.Session) *Db {
	return &Db{session: &GoCqlSession{ref: session}}
}

// Keyspace retrieves the keyspace metadata for all users
func (db *Db) Keyspace(keyspace string) (*gocql.KeyspaceMetadata, error) {
	// We expose gocql types for now, we should wrap them in the future instead
	ks, err := db.session.KeyspaceMetadata(keyspace)

	if err != nil && err.Error() == "keyspace does not exist" {
		return nil, &DbObjectNotFound{"keyspace", keyspace}
	}

	return ks, err
}

// Keyspace retrieves the table metadata for all users
func (db *Db) Table(keyspaceName string, tableName string) (*gocql.TableMetadata, error) {
	// We expose gocql types for now, we should wrap them in the future instead
	ks, err := db.Keyspace(keyspaceName)

	if err != nil {
		return nil, err
	}

	table, ok := ks.Tables[tableName]

	if !ok {
		return nil, &DbObjectNotFound{"table", tableName}
	}

	return table, nil
}

// KeyspaceNamingInfo Retrieves the keyspace naming information
func (db *Db) KeyspaceNamingInfo(ks *gocql.KeyspaceMetadata) config.KeyspaceNamingInfo {
	result := keyspaceNamingInfo{
		tables: make(map[string][]string, len(ks.Tables)),
	}

	for _, table := range ks.Tables {
		columns := make([]string, 0, len(table.Columns))
		for k := range table.Columns {
			columns = append(columns, k)
		}
		result.tables[table.Name] = columns
	}

	return &result
}

type keyspaceNamingInfo struct {
	tables map[string][]string
}

func (k *keyspaceNamingInfo) Tables() map[string][]string {
	return k.tables
}

// Keyspaces Retrieves all the keyspace names
func (db *Db) Keyspaces(userOrRole string) ([]string, error) {
	iter, err := db.session.ExecuteIter("SELECT keyspace_name FROM system_schema.keyspaces",
		NewQueryOptions().WithUserOrRole(userOrRole))
	if err != nil {
		return nil, err
	}

	var keyspaces []string
	for _, row := range iter.Values() {
		keyspaces = append(keyspaces, *row["keyspace_name"].(*string))
	}

	return keyspaces, nil
}

// Views Retrieves all the views for the given keyspace
func (db *Db) Views(ksName string) (map[string]bool, error) {
	iter, err := db.session.ExecuteIter("SELECT view_name FROM system_schema.views WHERE keyspace_name = ?", nil, ksName)
	if err != nil {
		return nil, err
	}

	views := make(map[string]bool, len(iter.Values()))
	for _, row := range iter.Values() {
		views[*row["view_name"].(*string)] = true
	}

	return views, nil
}

// DescribeTables returns the tables that the user is authorized to see
func (db *Db) DescribeTable(keyspace, table, username string) (*gocql.TableMetadata, error) {
	// Query system_schema first to make sure user is authorized
	stmt := "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?"

	result, retErr := db.Execute(stmt, NewQueryOptions().WithUserOrRole(username), keyspace, table)
	if retErr != nil {
		return nil, retErr
	}

	if len(result.Values()) == 0 {
		return nil, e.NewNotFoundError(fmt.Sprintf("table %s in keyspace %s not found", table, keyspace))
	}

	keyspaceMetadata, retErr := db.Keyspace(keyspace)
	if retErr != nil {
		return nil, retErr
	}

	tableMetadata, found := keyspaceMetadata.Tables[table]
	if found {
		return tableMetadata, nil
	}

	return nil, e.NewNotFoundError(fmt.Sprintf("table %s in keyspace %s not found", table, keyspace))
}

// DescribeTables returns the tables that the user is authorized to see
func (db *Db) DescribeTables(keyspace, username string) ([]string, error) {
	// Query system_schema to make sure user is authorized
	stmt := "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"
	result, retErr := db.Execute(stmt, NewQueryOptions().WithUserOrRole(username), keyspace)
	if retErr != nil {
		return nil, retErr
	}

	tables := make([]string, 0, len(result.Values()))
	for _, row := range result.Values() {
		value := row["table_name"].(*string)
		if value == nil {
			continue
		}
		tables = append(tables, *value)
	}

	return tables, nil
}

type DbObjectNotFound struct {
	objectType string
	keyspace   string
}

func (e *DbObjectNotFound) Error() string {
	return fmt.Sprintf("%s '%s' does not exist", e.objectType, e.keyspace)
}
