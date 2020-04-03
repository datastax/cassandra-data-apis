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
	assert.Equal(t, "email", nc.ToGraphQLField("tbl_b", "email"))
	assert.Equal(t, "addressStreet", nc.ToGraphQLField("tbl_b", "address_street"))

	// Columns not found should be converted to camelCase anyway
	assert.Equal(t, "notFound", nc.ToGraphQLField("tbl_b", "not_found"))
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
	assert.Equal(t, "not_found", nc.ToCQLColumn("tbl_b", "notFound"))
	assert.Equal(t, "not_found", nc.ToCQLColumn("tbl_not_found", "notFound"))
}

func TestNamingConventionToGraphType(t *testing.T) {
	nc := NewDefaultNaming(getKeyspaceNaming())
	assert.NotNil(t, nc)
	assert.Equal(t, "TblA2", nc.ToGraphQLType("tbl_a"))
	assert.Equal(t, "TblA", nc.ToGraphQLType("tbl_A"))
	assert.Equal(t, "TblB", nc.ToGraphQLType("tbl_b"))
	assert.Equal(t, "TblBFilter", nc.ToGraphQLType("tbl_b_filter"))

	// Fields not found should be converted to snake_case anyway
	assert.Equal(t, "TblNotFound", nc.ToGraphQLType("tbl_not_found"))
}

func TestNamingConventionToGraphQLTypeUnique(t *testing.T) {
	nc := NewDefaultNaming(getKeyspaceNaming())
	assert.NotNil(t, nc)
	assert.Equal(t, "TblAFilter", nc.ToGraphQLTypeUnique("tbl_A", "filter"))
	assert.Equal(t, "TblAInput", nc.ToGraphQLTypeUnique("tbl_A", "input"))
	assert.Equal(t, "TblA2Filter", nc.ToGraphQLTypeUnique("tbl_a", "filter"))
	assert.Equal(t, "TblA2Input", nc.ToGraphQLTypeUnique("tbl_a", "input"))
	// There's a table that is represented as GraphQL Type "TblBFilter"
	assert.Equal(t, "TblBFilter2", nc.ToGraphQLTypeUnique("tbl_b", "filter"))
	assert.Equal(t, "TblBFilterFilter", nc.ToGraphQLTypeUnique("tbl_b_filter", "filter"))
	assert.Equal(t, "TblNotFoundSuffix", nc.ToGraphQLTypeUnique("tbl_not_found", "suffix"))
}

func getKeyspaceNaming() KeyspaceNamingInfo {
	infoMock := NewKeyspaceNamingInfoMock()
	infoMock.On("Tables").Return(map[string][]string{
		"tbl_A":        {"aB", "a_b", "first_name", "firstName"},
		"tbl_a":        {"a", "b_b", "b__b"},
		"tbl_b":        {"email", "address_street"},
		"tbl_b_filter": {"col1", "col2"},
	})
	return infoMock
}
