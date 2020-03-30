package config

import (
	"fmt"
)

type Operations int

const (
	TableCreate Operations = 1 << iota
	TableDrop
	TableAlterAdd
	TableAlterDrop
	KeyspaceCreate
	KeyspaceDrop
)

func Ops(ops ...string) (Operations, error) {
	var o Operations
	err := o.Add(ops...)
	return o, err
}

func (o *Operations) Set(ops Operations) { *o |= ops; }
func (o *Operations) Clear(ops Operations) { *o &= ^ops; }
func (o Operations) IsSupported(ops Operations) bool { return o & ops != 0; }

func (o *Operations) Add(ops ...string) error {
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
