package graphql

import (
	"github.com/datastax/cassandra-data-apis/config"
	"github.com/datastax/cassandra-data-apis/db"
	"github.com/datastax/cassandra-data-apis/log"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
	"time"
)

func TestSchemaUpdater_Update(t *testing.T) {
	sessionMock := db.NewSessionMock()
	schemaGen := NewSchemaGenerator(db.NewDbWithSession(sessionMock), config.NewConfigMock().Default())

	keyspace := "store"

	// Initial schema
	sessionMock.AddKeyspace(db.NewKeyspaceMock(
		keyspace, map[string][]*gocql.ColumnMetadata{
			"books": db.BooksColumnsMock,
		})).Once()

	sessionMock.AddViews(nil)

	updater, err := NewUpdater(schemaGen, "store", 10*time.Second, log.NewZapLogger(zap.NewExample()))
	assert.NoError(t, err, "unable to create updater")

	assert.Contains(t, updater.Schema(keyspace).QueryType().Fields(), "books")
	assert.NotContains(t, updater.Schema(keyspace).QueryType().Fields(), "newTable1")

	// Add new table
	sessionMock.AddKeyspace(db.NewKeyspaceMock(
		"store", map[string][]*gocql.ColumnMetadata{
			"books":     db.BooksColumnsMock,
			"newTable1": db.BooksColumnsMock,
		})).Once()

	updater.update()
	assert.Contains(t, updater.Schema(keyspace).QueryType().Fields(), "newTable1")
}
