package graphql

import (
	"github.com/gocql/gocql"
	"github.com/riptano/data-endpoints/config"
	"github.com/riptano/data-endpoints/db"
	"github.com/stretchr/testify/assert"
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

	updater, err := NewUpdater(schemaGen, "store", 10 * time.Second)
	assert.NoError(t, err, "unable to create updater")

	assert.Contains(t, updater.Schema().QueryType().Fields(), "books")
	assert.NotContains(t, updater.Schema().QueryType().Fields(), "newTable1")

	// Add new table
	sessionMock.AddKeyspace(db.NewKeyspaceMock(
		"store", map[string][]*gocql.ColumnMetadata{
			"books": db.BooksColumnsMock,
			"newTable1": db.BooksColumnsMock,
		})).Once()

	updater.update() // Schema version is not set
	assert.Contains(t, updater.Schema().QueryType().Fields(), "newTable1")
	assert.NotContains(t, updater.Schema().QueryType().Fields(), "newTable2")

	// Add new another table
	sessionMock.AddKeyspace(db.NewKeyspaceMock(
		"store", map[string][]*gocql.ColumnMetadata{
			"books": db.BooksColumnsMock,
			"newTable1": db.BooksColumnsMock,
			"newTable2": db.BooksColumnsMock,
		})).Once()

	updater.update() // Schema version is the same
	assert.NotContains(t, updater.Schema().QueryType().Fields(), "newTable2")

	sessionMock.SetSchemaVersion("2ca627b7-f869-4f0c-b995-142f903a0367")

	updater.update() // Schema version is different
	assert.Contains(t, updater.Schema().QueryType().Fields(), "newTable2")
}
