package db

import (
	"github.com/datastax/cassandra-data-apis/config"
	"github.com/gocql/gocql"
	"time"
)

// Db represents a connection to a db
type Db struct {
	session Session
}

// NewDb Gets a pointer to a db
func NewDb(username string, password string, hosts ...string) (*Db, error) {
	cluster := gocql.NewCluster(hosts...)

	// Match DataStax drivers settings
	cluster.ConnectTimeout = 5 * time.Second
	cluster.Timeout = 12 * time.Second

	if username != "" && password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
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

// Keyspace Retrieves a keyspace
func (db *Db) Keyspace(keyspace string) (*gocql.KeyspaceMetadata, error) {
	// We expose gocql types for now, we should wrap them in the future instead
	return db.session.KeyspaceMetadata(keyspace)
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
func (db *Db) Keyspaces() ([]string, error) {
	iter, err := db.session.ExecuteIter("SELECT keyspace_name FROM system_schema.keyspaces", nil)
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
