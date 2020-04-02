package config

import (
	"fmt"
	"github.com/iancoleman/strcase"
)

type NamingConvention interface {
	// ToCQLColumn converts a GraphQL/REST name to a CQL column name.
	ToCQLColumn(tableName string, fieldName string) string

	// ToCQLColumn converts a GraphQL/REST name to a CQL table name.
	ToCQLTable(name string) string

	// ToGraphQLField converts a CQL name (typically a column name) to a GraphQL field name.
	ToGraphQLField(tableName string, columnName string) string

	// ToGraphQLOperation converts a CQL name (typically a table name) to a GraphQL operation name.
	ToGraphQLOperation(prefix string, name string) string

	// ToGraphQLType converts a CQL name (typically a table name) to a GraphQL type name.
	ToGraphQLType(name string) string

	// ToGraphQLEnumValue converts a CQL name to a GraphQL enumeration value name.
	ToGraphQLEnumValue(name string) string
}

type NamingConventionFn func(KeyspaceNamingInfo) NamingConvention

type KeyspaceNamingInfo interface {
	// A map containing the table names as keys and the column names as values
	Tables() map[string][]string
}

type defaultNaming struct{}

// Default naming implementation.
var DefaultNaming = &defaultNaming{}

func (n *defaultNaming) ToCQLColumn(tableName string, fieldName string) string {
	// TODO: Fix numbers: "Table2" or "table2" --> "table_2"
	return strcase.ToSnake(fieldName)
}

func (n *defaultNaming) ToCQLTable(name string) string {
	// TODO: Fix numbers: "Table2" or "table2" --> "table_2"
	return strcase.ToSnake(name)
}

func (n *defaultNaming) ToGraphQLField(tableName string, columnName string) string {
	return strcase.ToLowerCamel(columnName)
}

func (n *defaultNaming) ToGraphQLOperation(prefix string, name string) string {
	if prefix == "" {
		return strcase.ToLowerCamel(name)
	} else {
		return strcase.ToLowerCamel(prefix) + strcase.ToCamel(name)
	}
}

func (n *defaultNaming) ToGraphQLType(name string) string {
	return strcase.ToCamel(name)
}

func (n *defaultNaming) ToGraphQLEnumValue(name string) string {
	return strcase.ToLowerCamel(name)
}

func NewDefaultNaming(info KeyspaceNamingInfo) NamingConvention {
	dbTables := info.Tables()
	entitiesByTables := make(map[string]string, len(dbTables))
	tablesByEntities := make(map[string]string, len(dbTables))
	fieldsByColumns := make(map[string]map[string]string, len(dbTables))
	columnsByFields := make(map[string]map[string]string, len(dbTables))

	for tableName, columns := range dbTables {
		fieldByColumnName := make(map[string]string, len(columns))
		columnNameByField := make(map[string]string, len(columns))

		for _, columnName := range columns {
			fieldName := generateAvailableName(strcase.ToLowerCamel(columnName), columnNameByField)
			fieldByColumnName[columnName] = fieldName
			columnNameByField[fieldName] = columnName
		}

		entityName := generateAvailableName(strcase.ToCamel(tableName), tablesByEntities)
		entitiesByTables[tableName] = entityName
		fieldsByColumns[tableName] = fieldByColumnName
		columnsByFields[tableName] = columnNameByField
		tablesByEntities[entityName] = tableName
	}

	result := snakeCaseToCamelNaming{
		entitiesByTables: entitiesByTables,
		tablesByEntities: tablesByEntities,
		fieldsByColumns:  fieldsByColumns,
		columnsByFields:  columnsByFields,
	}
	return &result
}

func generateAvailableName(baseName string, nameMap map[string]string) string {
	if _, found := nameMap[baseName]; !found {
		return baseName
	}
	for i := 2; i < 1000; i++ {
		name := fmt.Sprintf("%s%d", baseName, i)
		_, found := nameMap[name]
		if !found {
			return name
		}
	}

	panic("Name was repeated more than 1000 times")
}

type snakeCaseToCamelNaming struct {
	entitiesByTables map[string]string
	tablesByEntities map[string]string
	columnsByFields  map[string]map[string]string
	fieldsByColumns  map[string]map[string]string
}

func (n *snakeCaseToCamelNaming) ToCQLColumn(tableName string, fieldName string) string {
	// lookup column by fields
	columnName, found := n.columnsByFields[tableName][fieldName]
	if !found {
		return strcase.ToSnake(fieldName)
	}
	return columnName
}

func (n *snakeCaseToCamelNaming) ToCQLTable(name string) string {
	// lookup table name by entity name
	tableName := n.tablesByEntities[name]
	if tableName == "" {
		// Default to snake_case for tables that doesn't exist yet (DDL)
		return strcase.ToSnake(name)
	}
	return tableName
}

func (n *snakeCaseToCamelNaming) ToGraphQLField(tableName string, columnName string) string {
	// lookup fields by columns
	fieldName, found := n.fieldsByColumns[tableName][columnName]
	if !found {
		return strcase.ToLowerCamel(columnName)
	}
	return fieldName
}

func (n *snakeCaseToCamelNaming) ToGraphQLOperation(prefix string, name string) string {
	if prefix == "" {
		return strcase.ToLowerCamel(name)
	} else {
		return strcase.ToLowerCamel(prefix) + strcase.ToCamel(name)
	}
}

func (n *snakeCaseToCamelNaming) ToGraphQLType(name string) string {
	entityName := n.entitiesByTables[name]
	if entityName == "" {
		// Default to Camel for entities that doesn't exist yet (DDL)
		return strcase.ToCamel(name)
	}
	return entityName
}

func (n *snakeCaseToCamelNaming) ToGraphQLEnumValue(name string) string {
	return strcase.ToLowerCamel(name)
}
