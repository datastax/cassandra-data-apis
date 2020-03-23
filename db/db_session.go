package db

import (
	"encoding/hex"
	"github.com/gocql/gocql"
)

type QueryOptions struct {
	UserOrRole  string
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
	ExecuteIter(query string, options *QueryOptions, values ...interface{}) (ResultSet, error)

	//TODO: Extract metadata methods from interface into another interface
	KeyspaceMetadata(keyspaceName string) (*gocql.KeyspaceMetadata, error)
}

type ResultSet interface {
	PageState() string
	Values() []map[string]interface{}
}

func (r *goCqlResultIterator) PageState() string {
	return hex.EncodeToString(r.pageState)
}

func (r *goCqlResultIterator) Values() []map[string]interface{} {
	return r.values
}

type goCqlResultIterator struct {
	pageState []byte
	values    []map[string]interface{}
}

func newResultIterator(iter *gocql.Iter) (*goCqlResultIterator, error) {
	columns := iter.Columns()
	scanner := iter.Scanner()

	items := make([]map[string]interface{}, 0)

	for scanner.Next() {
		row, err := mapScan(scanner, columns)
		if err != nil {
			return nil, err
		}
		items = append(items, row)
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return &goCqlResultIterator{
		pageState: iter.PageState(),
		values:    items,
	}, nil
}

type GoCqlSession struct {
	ref *gocql.Session
}

func (db *Db) Execute(query string, options *QueryOptions, values ...interface{}) (ResultSet, error) {
	return db.session.ExecuteIter(query, options, values...)
}

func (db *Db) ExecuteNoResult(query string, options *QueryOptions, values ...interface{}) error {
	return db.session.Execute(query, options, values)
}

func (session *GoCqlSession) Execute(query string, options *QueryOptions, values ...interface{}) error {
	_, err := session.ExecuteIter(query, options, values...)
	return err
}

func (session *GoCqlSession) ExecuteIter(query string, options *QueryOptions, values ...interface{}) (ResultSet, error) {
	q := session.ref.Query(query, values...)
	if options != nil {
		q.Consistency(options.Consistency)
		if options.UserOrRole != "" {
			q.CustomPayload(map[string][]byte{
				"ProxyExecute": []byte(options.UserOrRole),
			})
		}
	}
	return newResultIterator(q.Iter())
}

func (session *GoCqlSession) KeyspaceMetadata(keyspaceName string) (*gocql.KeyspaceMetadata, error) {
	return session.ref.KeyspaceMetadata(keyspaceName)
}
