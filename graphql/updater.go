package graphql

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/riptano/data-endpoints/db"
	"os"
	"sync"
	"time"
)

type SchemaUpdater struct {
	ctx context.Context
	cancel context.CancelFunc
	mutex sync.Mutex
	updateInterval time.Duration
	schema *graphql.Schema
	ksName string
	db *db.Db
	schemaVersion gocql.UUID
}

func (su *SchemaUpdater) Schema() *graphql.Schema {
	// This should be pretty fast, but an atomic pointer swap wouldn't require a lock here
	su.mutex.Lock()
	defer su.mutex.Unlock()
	return su.schema
}

func NewUpdater(ksName string, db *db.Db, updateInterval time.Duration) (*SchemaUpdater, error) {
	schema, err := BuildSchema(ksName, db)
	if err != nil {
		return nil, err
	}
	updater := &SchemaUpdater{
		ctx:    nil,
		cancel: nil,
		mutex:  sync.Mutex{},
		updateInterval: updateInterval,
		schema: &schema,
		ksName: ksName,
		db: db,
	}
	return updater, nil
}

func (su *SchemaUpdater) Start() {
	su.ctx, su.cancel = context.WithCancel(context.Background())
	for {
		iter := su.db.Execute("SELECT schema_version FROM system.local", gocql.LocalOne)

		shouldUpdate := false
		row := make(map[string]interface{})
		for iter.MapScan(row) {
			if schemaVersion, ok := row["schema_version"].(gocql.UUID); ok {
				if schemaVersion != su.schemaVersion {
					shouldUpdate = true
					su.schemaVersion = schemaVersion
				}
			}
		}

		err := iter.Close()
		if  err != nil {
			// TODO: Log error
			fmt.Fprintf(os.Stderr, "error attempting to determine schema version: %s", err)
		}

		if shouldUpdate {
			schema, err := BuildSchema(su.ksName, su.db)
			if err != nil {
				// TODO: Log error
				fmt.Fprintf(os.Stderr, "error trying to build graphql schema for keyspace '%s': %s", su.ksName, err)
			} else {
				su.mutex.Lock()
				su.schema = &schema
				su.mutex.Unlock()
			}
		}

		if !su.sleep() {
			return
		}
	}
}

func (su *SchemaUpdater) Stop() {
	su.cancel()
}

func (su *SchemaUpdater) sleep() bool {
	select {
	case <-time.After(su.updateInterval):
		return true
	case <-su.ctx.Done():
		return false
	}
}