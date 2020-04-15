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

	sessionMock.
		SetSchemaVersion("a78bc282-aff7-4c2a-8f23-4ce3584adbb0").
		Twice() // Called by `NewUpdater()` and the first `updater.update()`

	// Initial schema
	sessionMock.AddKeyspace(db.NewKeyspaceMock(
		"store", map[string][]*gocql.ColumnMetadata{
			"books": db.BooksColumnsMock,
		})).Once()

	updater, err := NewUpdater(schemaGen, "store", 10*time.Second, log.NewZapLogger(zap.NewExample()))
	assert.NoError(t, err, "unable to create updater")

	assert.Contains(t, updater.Schema().QueryType().Fields(), "books")
	assert.NotContains(t, updater.Schema().QueryType().Fields(), "newTable1")

	// Add new table
	sessionMock.AddKeyspace(db.NewKeyspaceMock(
		"store", map[string][]*gocql.ColumnMetadata{
			"books":     db.BooksColumnsMock,
			"newTable1": db.BooksColumnsMock,
		})).Once()

	updater.update() // Schema version is set
	// No change in the schema version, the updater will not read the new table
	assert.NotContains(t, updater.Schema().QueryType().Fields(), "newTable1")

	sessionMock.SetSchemaVersion("2ca627b7-f869-4f0c-b995-142f903a0367")
	updater.update() // Schema version changed
	assert.Contains(t, updater.Schema().QueryType().Fields(), "newTable1")
}
