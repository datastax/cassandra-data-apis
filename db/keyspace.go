package db

import (
	"fmt"
)

func (db *Db) CreateKeyspace(name string, dcReplicas map[string]int, options *QueryOptions) (bool, error) {
	dcs := ""
	for name, replicas := range dcReplicas {
		dcs += fmt.Sprintf(", '%s': %d", name, replicas)
	}

	query := fmt.Sprintf(`CREATE KEYSPACE "%s" WITH REPLICATION  = { 'class': 'NetworkTopologyStrategy', %s }`, name, dcs[2:])

	err := db.session.Execute(query, options)

	return err == nil, err
}

func (db *Db) DropKeyspace(name string, options *QueryOptions) (bool, error) {
	query := fmt.Sprintf(`DROP KEYSPACE "%s"`, name)
	err := db.session.Execute(query, options)

	return err == nil, err
}
