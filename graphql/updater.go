package graphql

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"os"
	"sync"
	"time"
)

type SchemaUpdater struct {
	ctx            context.Context
	cancel         context.CancelFunc
	mutex          sync.Mutex
	updateInterval time.Duration
	schema         *graphql.Schema
	schemaGen	   *SchemaGenerator
	ksName         string
	schemaVersion  gocql.UUID
}

func (su *SchemaUpdater) Schema() *graphql.Schema {
	// This should be pretty fast, but an atomic pointer swap wouldn't require a lock here
	su.mutex.Lock()
	defer su.mutex.Unlock()
	return su.schema
}

func NewUpdater(schemaGen *SchemaGenerator, ksName string, updateInterval time.Duration) (*SchemaUpdater, error) {
	schema, err := schemaGen.BuildSchema(ksName)
	if err != nil {
		return nil, err
	}
	updater := &SchemaUpdater{
		ctx:            nil,
		cancel:         nil,
		mutex:          sync.Mutex{},
		updateInterval: updateInterval,
		schema:         &schema,
		schemaGen:      schemaGen,
		ksName:         ksName,
	}
	return updater, nil
}

func (su *SchemaUpdater) Start() {
	su.ctx, su.cancel = context.WithCancel(context.Background())
	for {
		result, err := su.schemaGen.dbClient.Execute("SELECT schema_version FROM system.local", nil)

		if err != nil {
			// TODO: Log error
			fmt.Fprintf(os.Stderr, "error attempting to determine schema version: %s", err)
		}

		shouldUpdate := false
		for _, row := range result.Values() {
			if schemaVersion, ok := row["schema_version"].(*gocql.UUID); ok && schemaVersion != nil {
				if *schemaVersion != su.schemaVersion {
					shouldUpdate = true
					su.schemaVersion = *schemaVersion
				}
			} else {
				// TODO: Log error
				fmt.Fprintf(os.Stderr, "schema version value is invalid: %v", row)
			}
		}

		if shouldUpdate {
			schema, err := su.schemaGen.BuildSchema(su.ksName)
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
