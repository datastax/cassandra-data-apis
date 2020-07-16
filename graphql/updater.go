package graphql

import (
	"context"
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
	logger         log.Logger
}

func (su *SchemaUpdater) Schema(keyspace string) *graphql.Schema {
	// This should be pretty fast, but an atomic pointer swap wouldn't require a lock here
	su.mutex.Lock()
	schemas := *su.schemas
	su.mutex.Unlock()
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
	schemas, err := su.schemaGen.BuildSchemas(su.singleKeyspace)
	if err != nil {
		su.logger.Error("unable to build graphql schema for keyspace", "error", err)
	} else {
		su.mutex.Lock()
		su.schemas = &schemas
		su.mutex.Unlock()
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
