package db

import "github.com/gocql/gocql"

type QueryOptions struct {
	UserOrRole string
	Consistency gocql.Consistency
}

func NewQueryOptions() *QueryOptions {
	return &QueryOptions{
		Consistency: gocql.LocalOne,
	}
}

func (q *QueryOptions) WithUserOrRole(userOrRole string) *QueryOptions {
	q.UserOrRole = userOrRole
	return q
}

func (q *QueryOptions) WithConsistency(userOrRole string) *QueryOptions {
	q.UserOrRole = userOrRole
	return q
}

type DbSession interface {
	// Execute executes a statement without returning row results
	Execute(query string, options *QueryOptions, values ...interface{}) error

	// ExecuteIterSimple executes a statement and returns iterator to the result set
	ExecuteIter(query string, options *QueryOptions, values ...interface{}) ResultIterator

	//TODO: Extract metadata methods from interface into another interface
	KeyspaceMetadata(keyspaceName string) (*gocql.KeyspaceMetadata, error)
}

type ResultIterator interface {
	Close() error
	Columns() []gocql.ColumnInfo
	Scanner() gocql.Scanner
	PageState() []byte
	Scan(dest ...interface{}) bool
	MapScan(m map[string]interface{}) bool
}

type GoCqlSession struct {
	ref *gocql.Session
}

func (db *Db) Execute(query string, options *QueryOptions, values ...interface{}) ResultIterator {
	return db.session.ExecuteIter(query, options, values...)
}

func (db *Db) ExecuteNoResult(query string, options* QueryOptions, values ...interface{}) error {
	return db.session.Execute(query, options, values)
}

func (session *GoCqlSession) Execute(query string, options *QueryOptions, values ...interface{}) error {
	return session.ExecuteIter(query, options, values...).Close()
}

func (session *GoCqlSession) ExecuteIter(query string, options *QueryOptions, values ...interface{}) ResultIterator {
	q := session.ref.Query(query, values)
	if options != nil {
		q.Consistency(options.Consistency)
		if options.UserOrRole != "" {
			q.CustomPayload(map[string][]byte {
				"ProxyExecute": []byte(options.UserOrRole),
			})
		}
	}
	return session.ref.Query(query, values).Iter()
}

func (session *GoCqlSession) KeyspaceMetadata(keyspaceName string) (*gocql.KeyspaceMetadata, error) {
	return session.ref.KeyspaceMetadata(keyspaceName)
}

