package db

import (
	"fmt"
	"github.com/gocql/gocql"
)

func (db *Db) CreateKeyspace(name string, dcReplicas map[string]int) (bool, error) {
	// TODO: Escape keyspace datacenter names?
	dcs := ""
	for name, replicas := range dcReplicas {
		comma := ""
		if len(dcs) > 0 {
			comma = " ,"
		}
		dcs += fmt.Sprintf("%s'%s': %d", comma, name, replicas)
	}

	query := fmt.Sprintf("CREATE KEYSPACE %s WITH REPLICATION  = { 'class': 'NetworkTopologyStrategy', %s }", name, dcs)

	err := db.session.ExecuteSimple(query, gocql.Any)

	return err == nil, err
}

func (db *Db) DropKeyspace(name string) (bool, error) {
	// TODO: Escape keyspace name?
	query := fmt.Sprintf("DROP KEYSPACE %s", name)
	err := db.session.ExecuteSimple(query, gocql.Any)

	return err == nil, err
}
