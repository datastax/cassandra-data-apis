package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOperationsSetAndClear(t *testing.T) {
	var op Operations

	assert.Equal(t, op, Operations(0))
	assert.False(t, op.IsSupported(TableCreate))

	op.Set(TableCreate | TableDrop)
	assert.True(t, op.IsSupported(TableCreate))
	assert.True(t, op.IsSupported(TableDrop))

	op.Clear(TableCreate)
	assert.False(t, op.IsSupported(TableCreate))
	assert.True(t, op.IsSupported(TableDrop))
}

func TestOperationsAdd(t *testing.T) {
	var op Operations
	assert.Equal(t, op, Operations(0))

	op.Add("TableCreate", "TableDrop", "TableAlterAdd", "TableAlterDrop", "KeyspaceCreate", "KeyspaceDrop")
	assert.True(t, op.IsSupported(TableCreate))
	assert.True(t, op.IsSupported(TableDrop))
	assert.True(t, op.IsSupported(TableAlterAdd))
	assert.True(t, op.IsSupported(TableAlterDrop))
	assert.True(t, op.IsSupported(KeyspaceCreate))
	assert.True(t, op.IsSupported(KeyspaceDrop))
}
