package db

import (
	"errors"
	"github.com/gocql/gocql"
)

// Db represents a connection to a db
type Db struct {
	session *gocql.Session
}

// NewDb Gets a pointer to a db
func NewDb(hosts ...string) (*Db, error) {
	cluster := gocql.NewCluster(hosts...)

	var (
		session *gocql.Session
		err     error
	)

	session, err = cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	if session == nil {
		return nil, errors.New("failed to create session")
	}

	return &Db{
		session: session,
	}, nil
}

// Keyspace Retrieves a keyspace
func (db *Db) Keyspace(keyspace string) (*gocql.KeyspaceMetadata, error) {
	// We expose gocql types for now, we should wrap them in the future instead
	return db.session.KeyspaceMetadata(keyspace)
}

// Keyspaces Retrieves all the keyspace names
func (db *Db) Keyspaces() ([]string, error) {
	iter := db.session.Query("SELECT keyspace_name FROM system_schema.keyspaces").Iter()

	var keyspaces []string

	var name string
	for iter.Scan(&name) {
		keyspaces = append(keyspaces, name)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	return keyspaces, nil
}

// Execute executes query and returns iterator to the result set
func (db *Db) Execute(query string, consistency gocql.Consistency, values ...interface{}) *gocql.Iter {
	return db.session.Query(query).Bind(values...).Consistency(consistency).Iter()
}

// ExecuteNoResult executes a prepared statement without returning row results
func (db *Db) ExecuteNoResult(query string, consistency gocql.Consistency, values ...interface{}) error {
	iter := db.session.Query(query).Bind(values...).Consistency(consistency).Iter()
	return iter.Close()
}
