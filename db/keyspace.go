package db

import (
	"fmt"
	"github.com/gocql/gocql"
)

func (db *Db) CreateKeyspace(ksName string, dcReplicas map[string]int) (bool, error) {
	// TODO: Escape keyspace datacenter names?
	dcs := ""
	for name, replicas := range dcReplicas {
		comma := ""
		if len(dcs) > 0 {
			comma = " ,"
		}
		dcs += fmt.Sprintf("%s'%s': %d", comma, name, replicas)
	}

	query := fmt.Sprintf("CREATE KEYSPACE %s WITH REPLICATION  = { 'class': 'NetworkTopologyStrategy', %s }", ksName, dcs)

	err := db.ExecuteNoResult(query, gocql.Any)

	return err == nil, err
}

func (db *Db) DropKeyspace(ksName string) (bool, error) {
	// TODO: Escape keyspace name?
	query := fmt.Sprintf("DROP KEYSPACE %s", ksName)
	err := db.ExecuteNoResult(query, gocql.Any)

	return err == nil, err

}
