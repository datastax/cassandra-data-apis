package graphql

import (
	"context"
	"github.com/graphql-go/graphql"
	"github.com/riptano/data-endpoints/log"
	"sync"
	"time"
)

type SchemaUpdater struct {
	ctx            context.Context
	cancel         context.CancelFunc
	mutex          sync.Mutex
	updateInterval time.Duration
	schema         *graphql.Schema
	schemaGen      *SchemaGenerator
	ksName         string
	schemaVersion  string
	logger 		   log.Logger
}

func (su *SchemaUpdater) Schema() *graphql.Schema {
	// This should be pretty fast, but an atomic pointer swap wouldn't require a lock here
	su.mutex.Lock()
	defer su.mutex.Unlock()
	return su.schema
}

func NewUpdater(schemaGen *SchemaGenerator, ksName string, updateInterval time.Duration, logger log.Logger) (*SchemaUpdater, error) {
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
		logger:			logger,
	}
	return updater, nil
}

func (su *SchemaUpdater) Start() {
	su.ctx, su.cancel = context.WithCancel(context.Background())
	for {
		su.update()
		if !su.sleep() {
			return
		}
	}
}

func (su *SchemaUpdater) Stop() {
	su.cancel()
}

func (su *SchemaUpdater) update() {
	result, err := su.schemaGen.dbClient.Execute("SELECT schema_version FROM system.local", nil)

	if err != nil {
		su.logger.Error("unable to query schema version",
			"error", err)
	}

	shouldUpdate := false
	for _, row := range result.Values() {
		if schemaVersion, ok := row["schema_version"].(*string); ok && schemaVersion != nil {
			if *schemaVersion != su.schemaVersion {
				shouldUpdate = true
				su.schemaVersion = *schemaVersion
			}
		} else {
			su.logger.Error("schema version value is invalid",
				"value", row)
		}
	}

	if shouldUpdate {
		schema, err := su.schemaGen.BuildSchema(su.ksName)
		if err != nil {
			su.logger.Error("unable to build graphql schema for keyspace",
				"keyspace", su.ksName, "error", err)
		} else {
			su.mutex.Lock()
			su.schema = &schema
			su.mutex.Unlock()
		}
	}
}

func (su *SchemaUpdater) sleep() bool {
	select {
	case <-time.After(su.updateInterval):
		return true
	case <-su.ctx.Done():
		return false
	}
}
