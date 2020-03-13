package db

import "github.com/gocql/gocql"

func (db *Db) Execute(query string, consistency gocql.Consistency, values ...interface{}) *gocql.Iter {
	return db.session.ExecuteIter(query, consistency, values...)
}

func (db *Db) ExecuteNoResult(query string, consistency gocql.Consistency, values ...interface{}) error {
	return db.session.Execute(query, consistency, values)
}

type DbSession interface {
	// Execute executes a prepared statement without returning row results
	Execute(query string, consistency gocql.Consistency, values ...interface{}) error

	// Execute executes a simple statement without returning row results
	ExecuteSimple(query string, consistency gocql.Consistency, values ...interface{}) error

	// ExecuteIter executes a prepared statement and returns iterator to the result set
	ExecuteIter(query string, consistency gocql.Consistency, values ...interface{}) *gocql.Iter

	// ExecuteIterSimple executes a simple statement and returns iterator to the result set
	ExecuteIterSimple(query string, consistency gocql.Consistency, values ...interface{}) *gocql.Iter

	//TODO: Extract metadata methods from interface into another interface
	KeyspaceMetadata(keyspaceName string) (*gocql.KeyspaceMetadata, error)
}

type GoCqlSession struct {
	ref *gocql.Session
}

func (session *GoCqlSession) Execute(query string, consistency gocql.Consistency, values ...interface{}) error {
	return session.ref.Query(query).Bind(values...).Consistency(consistency).Exec()
}

func (session *GoCqlSession) ExecuteSimple(query string, consistency gocql.Consistency, values ...interface{}) error {
	return session.ref.Query(query, values...).Consistency(consistency).Exec()
}

func (session *GoCqlSession) ExecuteIter(query string, consistency gocql.Consistency, values ...interface{}) *gocql.Iter {
	return session.ref.Query(query).Bind(values...).Consistency(consistency).Iter()
}

func (session *GoCqlSession) ExecuteIterSimple(query string, consistency gocql.Consistency, values ...interface{}) *gocql.Iter {
	return session.ref.Query(query, values...).Consistency(consistency).Iter()
}
func (session *GoCqlSession) KeyspaceMetadata(keyspaceName string) (*gocql.KeyspaceMetadata, error) {
	return session.ref.KeyspaceMetadata(keyspaceName)
}
