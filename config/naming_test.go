package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNamingConventionToGraphQLField(t *testing.T) {
	nc := NewDefaultNaming(getKeyspaceNaming())
	assert.NotNil(t, nc)
	assert.Equal(t, "a", nc.ToGraphQLField("tbl_a", "a"))
	assert.Equal(t, "bB", nc.ToGraphQLField("tbl_a", "b_b"))
	assert.Equal(t, "bB2", nc.ToGraphQLField("tbl_a", "b__b"))
	assert.Equal(t, "aB", nc.ToGraphQLField("tbl_A", "aB"))
	assert.Equal(t, "aB2", nc.ToGraphQLField("tbl_A", "a_b"))
	assert.Equal(t, "email", nc.ToGraphQLField("tbl_b_c", "email"))
	assert.Equal(t, "addressStreet", nc.ToGraphQLField("tbl_b_c", "address_street"))

	// Columns not found should be converted to camelCase anyway
	assert.Equal(t, "notFound", nc.ToGraphQLField("tbl_b_c", "not_found"))
	assert.Equal(t, "aColumn", nc.ToGraphQLField("tbl_not_found", "a_column"))
}

func TestNamingConventionToCQLColumn(t *testing.T) {
	nc := NewDefaultNaming(getKeyspaceNaming())
	assert.NotNil(t, nc)
	assert.Equal(t, "a", nc.ToCQLColumn("tbl_a", "a"))
	assert.Equal(t, "b_b", nc.ToCQLColumn("tbl_a", "bB"))
	assert.Equal(t, "b__b", nc.ToCQLColumn("tbl_a", "bB2"))
	assert.Equal(t, "aB", nc.ToCQLColumn("tbl_A", "aB"))
	assert.Equal(t, "a_b", nc.ToCQLColumn("tbl_A", "aB2"))

	// Fields not found should be converted to snake_case anyway
	assert.Equal(t, "not_found", nc.ToCQLColumn("tbl_b_c", "notFound"))
	assert.Equal(t, "not_found", nc.ToCQLColumn("tbl_not_found", "notFound"))
}

func getKeyspaceNaming() KeyspaceNamingInfo {
	infoMock := NewKeyspaceNamingInfoMock()
	infoMock.On("Tables").Return(map[string][]string{
		"tbl_a":   {"a", "b_b", "b__b"},
		"tbl_A":   {"aB", "a_b", "first_name", "firstName"},
		"tbl_b_c": {"email", "address_street"},
	})
	return infoMock
}
