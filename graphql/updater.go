package graphql

import (
	"context"
	"fmt"
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
	schema *graphql.Schema
}

func (su *SchemaUpdater) Schema() *graphql.Schema {
	// This should be pretty fast, but an atomic pointer swap wouldn't require a lock here
	su.mutex.Lock()
	defer su.mutex.Unlock()
	return su.schema
}

func (su *SchemaUpdater) Start(ksName string, db *db.Db) error {
	su.ctx, su.cancel = context.WithCancel(context.Background())
	for {
		schema, err := BuildSchema(ksName, db)
		if err != nil {
			// TODO: Log error
			fmt.Fprintf(os.Stderr, "error trying to build graphql schema for keyspace '%s': %s", ksName, err)
		} else {
			su.mutex.Lock()
			su.schema = &schema
			su.mutex.Unlock()
		}
		// TODO: Make time configurable
		// We could signal this from a gocql schema listener
		if !su.sleep(10*time.Second) {
			return nil
		}
	}
}

func (su *SchemaUpdater) Stop() {
	su.cancel()
}

func (su *SchemaUpdater) sleep(duration time.Duration) bool {
	select {
	case <-time.After(duration):
		return true
	case <-su.ctx.Done():
		return false
	}
}