package db

import (
	"encoding/hex"
	"errors"
	"github.com/gocql/gocql"
)

type QueryOptions struct {
	UserOrRole        string
	Consistency       gocql.Consistency
	SerialConsistency gocql.SerialConsistency
}

func NewQueryOptions() *QueryOptions {
	return &QueryOptions{
		// Set defaults for queries that are not affected by consistency
		// But still need the parameters, i.e, DDL queries.
		Consistency:       gocql.LocalOne,
		SerialConsistency: gocql.LocalSerial,
	}
}

func (q *QueryOptions) WithUserOrRole(userOrRole string) *QueryOptions {
	q.UserOrRole = userOrRole
	return q
}

func (q *QueryOptions) WithConsistency(consistency gocql.Consistency) *QueryOptions {
	q.Consistency = consistency
	return q
}

func (q *QueryOptions) WithSerialConsistency(serialConsistency gocql.SerialConsistency) *QueryOptions {
	q.SerialConsistency = serialConsistency
	return q
}

type Session interface {
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

	// Avoid reusing metadata from the prepared statement
	// Otherwise, we will not get the [applied] column (https://github.com/gocql/gocql/issues/612)
	// Or new columns for SELECT *
	q.NoSkipMetadata()

	if options != nil {
		q.Consistency(options.Consistency)

		if options.SerialConsistency != gocql.Serial && options.SerialConsistency != gocql.LocalSerial {
			return nil, errors.New("Invalid serial consistency")
		}

		q.SerialConsistency(options.SerialConsistency)

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
