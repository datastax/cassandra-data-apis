package db

import (
	"fmt"
)

type CreateKeyspaceInfo struct {
	Name        string
	DCReplicas  map[string]int
	IfNotExists bool
}

type DropKeyspaceInfo struct {
	Name     string
	IfExists bool
}

func (db *Db) CreateKeyspace(info *CreateKeyspaceInfo, options *QueryOptions) error {
	dcs := ""
	for name, replicas := range info.DCReplicas {
		dcs += fmt.Sprintf(", '%s': %d", name, replicas)
	}

	query := fmt.Sprintf(`CREATE KEYSPACE %s"%s" WITH REPLICATION  = { 'class': 'NetworkTopologyStrategy', %s }`,
		ifNotExistsStr(info.IfNotExists), info.Name, dcs[2:])

	return db.session.Execute(query, options)
}

func (db *Db) DropKeyspace(info *DropKeyspaceInfo, options *QueryOptions) error {
	query := fmt.Sprintf(`DROP KEYSPACE %s"%s"`, ifExistsStr(info.IfExists), info.Name)
	return db.session.Execute(query, options)
}
