package ddl

import (
	"fmt"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas"
	"github.com/datastax/cassandra-data-apis/types"
	. "github.com/onsi/gomega"
	"sort"
	"strings"
	"time"
)

var ColumnTypes = []string{
	"{ basic: TEXT }",
	"{ basic: ASCII }",
	"{ basic: VARCHAR }",
	"{ basic: TEXT }",
	"{ basic: BOOLEAN }",
	"{ basic: FLOAT }",
	"{ basic: DOUBLE }",
	"{ basic: TINYINT }",
	// "{ basic: SMALLINT }",
	"{ basic: INT }",
	"{ basic: BIGINT }",
	"{ basic: VARINT }",
	"{ basic: DECIMAL }",
	"{ basic: UUID }",
	"{ basic: TIMEUUID }",
	"{ basic: TIME }",
	// "{ basic: DATE }",
	// "{ basic: DURATION }",
	"{ basic: TIMESTAMP }",
	"{ basic: BLOB }",
	"{ basic: INET }",
	"{ basic: LIST, info: { subTypes: [ { basic: TEXT } ] } }",
	"{ basic: SET, info: { subTypes: [ { basic: TEXT } ] } }",
	"{ basic: MAP, info: { subTypes: [ { basic: TEXT }, { basic: INT } ] } }",
}

var ColumnTypesResult = []map[string]interface{}{
	{"basic": "TEXT"},
	{"basic": "ASCII"},
	{"basic": "TEXT"}, // VARCHAR reports as TEXT
	{"basic": "TEXT"},
	{"basic": "BOOLEAN"},
	{"basic": "FLOAT"},
	{"basic": "DOUBLE"},
	{"basic": "TINYINT"},
	// {"basic": "SMALLINT"},
	{"basic": "INT"},
	{"basic": "BIGINT"},
	{"basic": "VARINT"},
	{"basic": "DECIMAL"},
	{"basic": "UUID"},
	{"basic": "TIMEUUID"},
	{"basic": "TIME"},
	// {"basic": "DATE"},
	// {"basic": "DURATION"},
	{"basic": "TIMESTAMP"},
	{"basic": "BLOB"},
	{"basic": "INET"},
	{"basic": "LIST", "info": map[string]interface{}{"subTypes": []interface{}{map[string]interface{}{"basic": "TEXT"}}}},
	{"basic": "SET", "info": map[string]interface{}{"subTypes": []interface{}{map[string]interface{}{"basic": "TEXT"}}}},
	{"basic": "MAP", "info": map[string]interface{}{"subTypes": []interface{}{map[string]interface{}{"basic": "TEXT"}, map[string]interface{}{"basic": "INT"}}}},
}

var DCsResult = []interface{}{
	map[string]interface{}{
		"name":     "dc1",
		"replicas": float64(3),
	},
}

func CreateKeyspace(routes []types.Route, name string) {
	response := CreateKeyspaceIfNotExists(routes, name, false)
	Expect(response.Errors).To(HaveLen(0))
}

func CreateKeyspaceIfNotExists(routes []types.Route, name string, ifNotExists bool) schemas.ResponseBody {
	mutation := `mutation { createKeyspace(name:"%s", dcs: [{name:"dc1", replicas:3}], ifNotExists: %t) }`
	buffer := schemas.ExecutePost(routes, "/graphql-schema", fmt.Sprintf(mutation, name, ifNotExists))
	return schemas.DecodeResponse(buffer)
}

func DropKeyspace(routes []types.Route, name string) {
	mutation := `mutation { dropKeyspace(name:"%s") }`
	buffer := schemas.ExecutePost(routes, "/graphql-schema", fmt.Sprintf(mutation, name))
	response := schemas.DecodeResponse(buffer)
	Expect(response.Errors).To(HaveLen(0))
}

func CreateTable(routes []types.Route, ksName string, tableName string, columnTypes []string) {
	response := CreateTableIfNotExists(routes, ksName, tableName, columnTypes, false)
	Expect(response.Errors).To(HaveLen(0))
}

func CreateTableIfNotExists(routes []types.Route, ksName string, tableName string,
	columnTypes []string, ifNotExists bool) schemas.ResponseBody {
	mutation := `mutation { 
  createTable(
   	keyspaceName:"%s", 
   	tableName:"%s", 
   	partitionKeys: [ { name: "pk1", type: { basic: TEXT } } ],
   	clusteringKeys: [ { name: "ck1", type: { basic: TEXT } } ],
   	values: [ %s ],
	ifNotExists: %t)
}`
	buffer := schemas.ExecutePost(routes, "/graphql-schema",
		fmt.Sprintf(mutation, ksName, tableName, buildColumnList("value", columnTypes), ifNotExists))
	return schemas.DecodeResponse(buffer)
}

func AlterTableAdd(routes []types.Route, ksName string, tableName string, columnTypes []string) {
	response := AlterTableAddResponse(routes, ksName, tableName, columnTypes)
	Expect(response.Errors).To(HaveLen(0))
}

func AlterTableAddResponse(routes []types.Route, ksName string,
	tableName string, columnTypes []string) schemas.ResponseBody {
	mutation := `mutation {
  alterTableAdd(keyspaceName:"%s", tableName:"%s", toAdd: [ %s ])
}`
	buffer := schemas.ExecutePost(routes, "/graphql-schema",
		fmt.Sprintf(mutation, ksName, tableName, buildColumnList("addedValue", columnTypes)))
	return schemas.DecodeResponse(buffer)
}

func AlterTableDrop(routes []types.Route, ksName string, tableName string, columns []string) {
	response := AlterTableDropResponse(routes, ksName, tableName, columns)
	Expect(response.Errors).To(HaveLen(0))
}

func AlterTableDropResponse(routes []types.Route, ksName string, tableName string, columns []string) schemas.ResponseBody {
	mutation := `mutation {
  alterTableDrop(keyspaceName:"%s", tableName:"%s", toDrop: [ %s ])
}`
	var b strings.Builder
	for _, column := range columns {
		fmt.Fprintf(&b, `, "%s"`, column)
	}
	buffer := schemas.ExecutePost(routes, "/graphql-schema",
		fmt.Sprintf(mutation, ksName, tableName, b.String()[2:]))
	return schemas.DecodeResponse(buffer)
}

func DropTable(routes []types.Route, ksName string, tableName string) {
	response := DropTableResponse(routes, ksName, tableName)
	Expect(response.Errors).To(HaveLen(0))
}

func DropTableResponse(routes []types.Route, ksName string, tableName string) schemas.ResponseBody {
	mutation := `mutation { dropTable(keyspaceName:"%s", tableName:"%s") }`
	buffer := schemas.ExecutePost(routes, "/graphql-schema", fmt.Sprintf(mutation, ksName, tableName))
	return schemas.DecodeResponse(buffer)
}

func Keyspaces(routes []types.Route) schemas.ResponseBody {
	query := `query {
  keyspaces {
    name
    dcs {
      name
      replicas
    }
  }
}
`
	buffer := schemas.ExecutePost(routes, "/graphql-schema", query)
	response := schemas.DecodeResponse(buffer)
	Expect(response.Errors).To(HaveLen(0))
	return response
}

func Keyspace(routes []types.Route, ksName string) schemas.ResponseBody {
	query := `query {
  keyspace(name:"%s") {
    name
    dcs {
      name
      replicas
    }
  }
}
`
	buffer := schemas.ExecutePost(routes, "/graphql-schema", fmt.Sprintf(query, ksName))
	return schemas.DecodeResponse(buffer)
}

func Tables(routes []types.Route, ksName string) schemas.ResponseBody {
	query := `query {
  keyspace(name: "%s") {
    tables {
      name
      columns {
        name
        type {
          basic
          info {
            subTypes {
              basic
            }
          }
        }
      }
    }
  }
}
`
	buffer := schemas.ExecutePost(routes, "/graphql-schema", fmt.Sprintf(query, ksName))
	response := schemas.DecodeResponse(buffer)
	Expect(response.Errors).To(HaveLen(0))
	return response
}

func Table(routes []types.Route, ksName string, tableName string) schemas.ResponseBody {
	query := `query {
  keyspace(name: "%s") {
    table(name:"%s") {
      name
      columns {
        name
        type {
          basic
          info {
            subTypes {
              basic
            }
          }
        }
      }
    }
  }
}
`
	buffer := schemas.ExecutePost(routes, "/graphql-schema", fmt.Sprintf(query, ksName, tableName))
	response := schemas.DecodeResponse(buffer)
	return response
}

func ExpectInvalidKeyspace(routes []types.Route, ksName string, tableName string) {
	expectedMessage := fmt.Sprintf("keyspace does not exist '%s'", ksName)
	response := CreateTableIfNotExists(routes, ksName, tableName, []string{"{ basic: TEXT }"}, false)
	schemas.ExpectError(response, expectedMessage)
	response = AlterTableAddResponse(routes, ksName, tableName, ColumnTypes)
	schemas.ExpectError(response, expectedMessage)
	response = AlterTableDropResponse(routes, ksName, tableName, []string{"addedValue01"})
	schemas.ExpectError(response, expectedMessage)
	response = DropTableResponse(routes, ksName, tableName)
	schemas.ExpectError(response, expectedMessage)
	response = Keyspace(routes, ksName)
	schemas.ExpectError(response, expectedMessage)
}

func SortColumns(response schemas.ResponseBody) {
	sortColumns(getColumns(response))
}

func WaitUntilColumnExists(columnName string, queryFunc func() schemas.ResponseBody) schemas.ResponseBody {
	return waitUntil(func(response schemas.ResponseBody) bool {
		Expect(response.Errors).To(HaveLen(0))
		for _, column := range getColumns(response) {
			c := column.(map[string]interface{})
			if c["name"] == columnName {
				return true
			}
		}
		return false
	}, queryFunc)
}

func WaitUntilColumnIsGone(columnName string, queryFunc func() schemas.ResponseBody) schemas.ResponseBody {
	return waitUntil(func(response schemas.ResponseBody) bool {
		for _, column := range getColumns(response) {
			c := column.(map[string]interface{})
			if c["name"] == columnName {
				return false
			}
		}
		return true
	}, queryFunc)
}

func WaitUntilExists(queryFunc func() schemas.ResponseBody) schemas.ResponseBody {
	return waitUntil(func(response schemas.ResponseBody) bool {
		return len(response.Errors) == 0 ||
			!strings.Contains(response.Errors[0].Message, "does not exist")
	}, queryFunc)
}

func WaitUntilGone(queryFunc func() schemas.ResponseBody) {
	waitUntil(func(response schemas.ResponseBody) bool {
		return len(response.Errors) > 0 &&
			strings.Contains(response.Errors[0].Message, "does not exist")
	}, queryFunc)
}

func TextColumn(columnName string) map[string]interface{} {
	return map[string]interface{}{
		"name": columnName,
		"type": map[string]interface{}{
			"basic": "TEXT",
			"info":  nil,
		},
	}
}

func BuildColumnResult(columnName string, columnTypes []map[string]interface{}, columns ...map[string]interface{}) []interface{} {
	result := make([]interface{}, 0, len(columnTypes)+len(columns)+2)
	result = append(result, TextColumn("ck1"))
	result = append(result, TextColumn("pk1"))
	for _, column := range columns {
		result = append(result, column)
	}
	for i, columnType := range columnTypes {
		c := map[string]interface{}{
			"info": nil,
		}
		for k, v := range columnType {
			c[k] = v
		}
		result = append(result, map[string]interface{}{
			"name": fmt.Sprintf("%s%02d", columnName, i+1),
			"type": c,
		})
	}
	return sortColumns(result)
}

func buildColumnList(columnName string, columnTypes []string) string {
	var b strings.Builder
	for i, columnType := range columnTypes {
		fmt.Fprintf(&b, `, { name: "%s%02d", type: %s }`, columnName, i+1, columnType)
	}
	return b.String()[2:]
}

func waitUntil(checkFunc func(response schemas.ResponseBody) bool,
	queryFunc func() schemas.ResponseBody) schemas.ResponseBody {
	var response schemas.ResponseBody
	i := 0
	const max = 20
	for ; i < max; i++ {
		response = queryFunc()
		if checkFunc(response) {
			break
		}
		time.Sleep(600 * time.Millisecond)
	}
	Expect(i).To(Not(Equal(max)))
	return response
}

func sortColumns(columns []interface{}) []interface{} {
	sort.SliceStable(columns, func(i, j int) bool {
		a := columns[i].(map[string]interface{})
		b := columns[j].(map[string]interface{})
		return a["name"].(string) < b["name"].(string)
	})
	return columns
}

func getColumns(response schemas.ResponseBody) []interface{} {
	keyspace := response.Data["keyspace"].(map[string]interface{})
	table := keyspace["table"].(map[string]interface{})
	return table["columns"].([]interface{})
}
