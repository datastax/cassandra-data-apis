package config

import (
	"fmt"
)

type SchemaOperations int

const (
	TableCreate SchemaOperations = 1 << iota
	TableDrop
	TableAlterAdd
	TableAlterDrop
	KeyspaceCreate
	KeyspaceDrop
)

func Ops(ops ...string) (SchemaOperations, error) {
	var o SchemaOperations
	err := o.Add(ops...)
	return o, err
}

func (o *SchemaOperations) Set(ops SchemaOperations)             { *o |= ops; }
func (o *SchemaOperations) Clear(ops SchemaOperations)           { *o &= ^ops; }
func (o SchemaOperations) IsSupported(ops SchemaOperations) bool { return o & ops != 0; }

func (o *SchemaOperations) Add(ops ...string) error {
	for _, op := range ops {
		switch op {
		case "TableCreate":
			o.Set(TableCreate)
		case "TableDrop":
			o.Set(TableDrop)
		case "TableAlterAdd":
			o.Set(TableAlterAdd)
		case "TableAlterDrop":
			o.Set(TableAlterDrop)
		case "KeyspaceCreate":
			o.Set(KeyspaceCreate)
		case "KeyspaceDrop":
			o.Set(KeyspaceDrop)
		default:
			return fmt.Errorf("invalid operation: %s", op)
		}
	}
	return nil
}
