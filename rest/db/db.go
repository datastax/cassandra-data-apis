package db

import (
	"sync"

	"github.com/gocql/gocql"
)

// DB exposes methods for interacting with the database running within the customer's cluster
type DB interface {
	GetSession() interface{}

	// AuthenticateUser will authenticate a user against the database by creating and immediately closing the session. If
	// the provided credentials are not valid an error will be returned.
	AuthenticateUser(username string, password string, hosts ...string) error

	// Select provides the necessary functionality to perform a SELECT statement against the database
	Select(stmt string, username string, values ...interface{}) ([]map[string]interface{}, error)

	// SelectWithPaging provides the necessary functionality to perform a SELECT statement against the database and includes
	// support for paging the resultset. Should paging of the resultset be required a string containing the pageStage will
	// be returned along with the rows.
	SelectWithPaging(stmt string, username string, pageState string, pageSize int, values ...interface{}) ([]map[string]interface{}, string, error)

	// Insert provides the necessary functionality to perform a INSERT statement against the database
	Insert(stmt string, username string, values ...interface{}) error

	// Update provides the necessary functionality to perform a UPDATE statement against the database
	Update(stmt string, username string, values ...interface{}) error

	// Delete provides the necessary functionality to perform a DELETE statement against the database
	Delete(stmt string, username string, values ...interface{}) error

	// Create provides the necessary functionality to perform a CREATE statement against the database
	Create(stmt string, username string) error

	// Alter provides the necessary functionality to perform a ALTER statement against the database
	Alter(stmt string, username string) error

	// Drop provides the necessary functionality to perform a DROP statement against the database
	Drop(stmt string, username string) error

	// DescribeKeyspace provides the necessary functionality to describe a keyspace
	DescribeKeyspace(keyspace, username string) (*gocql.KeyspaceMetadata, error)

	// DescribeKeyspaces provides the necessary functionality to list all keyspaces
	DescribeKeyspaces(username string) ([]string, error)

	// DescribeTable provides the necessary functionality to describe a table within a given keyspace
	DescribeTable(keyspace, table, username string) (*gocql.TableMetadata, error)

	// DescribeTables provides the necessary functionality to list all tables within a keyspace
	DescribeTables(keyspace, username string) ([]string, error)
}

type DatabaseConnection struct {
	Database DB
}

var doOnce sync.Once
var db DB

// GetDB will return an implementation of the DB interface depending on what is set for the environment variable IS_LOCAL
func GetDB(username string, password string, hosts ...string) (DB, error) {
	doOnce.Do(func() {
		var err error
		db, err = newDseDB(username, password, hosts...)
		if err != nil {
			//TODO: Log
			//log.Fatal(err)
		}
	})

	return db, nil
}
