package graphql

import (
	"context"
	"errors"
	"github.com/datastax/cassandra-data-apis/log"
	"github.com/graphql-go/graphql"
	"sync"
	"time"
)

type SchemaUpdater struct {
	ctx            context.Context
	cancel         context.CancelFunc
	mutex          sync.Mutex
	updateInterval time.Duration
	schemas        *map[string]*graphql.Schema
	schemaGen      *SchemaGenerator
	singleKeyspace string
	schemaVersion  string
	logger         log.Logger
}

func (su *SchemaUpdater) Schema(keyspace string) *graphql.Schema {
	// This should be pretty fast, but an atomic pointer swap wouldn't require a lock here
	su.mutex.Lock()
	defer su.mutex.Unlock()
	schemas := *su.schemas
	return schemas[keyspace]
}

func NewUpdater(
	schemaGen *SchemaGenerator,
	singleKeyspace string,
	updateInterval time.Duration,
	logger log.Logger,
) (*SchemaUpdater, error) {
	schemas, err := schemaGen.BuildSchemas(singleKeyspace)
	if err != nil {
		return nil, err
	}

	updater := &SchemaUpdater{
		ctx:            nil,
		cancel:         nil,
		mutex:          sync.Mutex{},
		updateInterval: updateInterval,
		schemas:        &schemas,
		schemaGen:      schemaGen,
		singleKeyspace: singleKeyspace,
		logger:         logger,
	}

	version, err := updater.getSchemaVersion()
	if err != nil {
		logger.Error("unable to query schema version",
			"error", err)
	}
	updater.schemaVersion = version

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
	version, err := su.getSchemaVersion()

	if err != nil {
		su.logger.Error("unable to query schema version",
			"error", err)
		return
	}

	shouldUpdate := false
	if version != su.schemaVersion {
		shouldUpdate = true
		su.schemaVersion = version
	}

	if shouldUpdate {
		schemas, err := su.schemaGen.BuildSchemas(su.singleKeyspace)
		if err != nil {
			su.logger.Error("unable to build graphql schema for keyspace", "error", err)
		} else {
			su.mutex.Lock()
			su.schemas = &schemas
			su.mutex.Unlock()
		}
	}
}

func (su *SchemaUpdater) getSchemaVersion() (string, error) {
	result, err := su.schemaGen.dbClient.Execute("SELECT schema_version FROM system.local", nil)
	if err != nil {
		return "", err
	}
	row := result.Values()[0]
	version := row["schema_version"].(*string)
	if version == nil {
		return "", errors.New("schema version value is empty")
	}
	return *version, nil
}

func (su *SchemaUpdater) sleep() bool {
	select {
	case <-time.After(su.updateInterval):
		return true
	case <-su.ctx.Done():
		return false
	}
}
