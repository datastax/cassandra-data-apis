package ddl

import (
	"fmt"
	"github.com/datastax/cassandra-data-apis/graphql"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas"
	. "github.com/onsi/gomega"
	"strings"
)

func buildColumnList(columnName string, columnTypes []string) string {
	var b strings.Builder
	for i, columnType := range columnTypes {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, `{ name: "%s%d", type: %s }`, columnName, i + 1, columnType)
	}
	return b.String()
}

func CreateKeyspace(routes []graphql.Route, name string) schemas.ResponseBody {
	mutation := `mutation { createKeyspace(name:"%s", dcs: [{name:"dc1", replicas:3}]) { name } }`
	buffer := schemas.ExecutePost(routes, "/graphql-schema", fmt.Sprintf(mutation, name))
	response := schemas.DecodeResponse(buffer)
	Expect(response.Errors).To(HaveLen(0))
	return response
}

func DropKeyspace(routes []graphql.Route, name string) schemas.ResponseBody {
	mutation := `mutation { dropKeyspace(name:"%s") }`
	buffer := schemas.ExecutePost(routes, "/graphql-schema", fmt.Sprintf(mutation, name))
	response := schemas.DecodeResponse(buffer)
	Expect(response.Errors).To(HaveLen(0))
	return response
}

func CreateTable(routes []graphql.Route, ksName string, tableName string, columnTypes []string) schemas.ResponseBody {
	mutation := `mutation { 
	createTable(
	    	keyspaceName:"%s", 
	    	tableName:"%s", 
	    	partitionKeys: [ { name: "pk1", type: { basic: TEXT } } ]
	    	clusteringKeys: [ { name: "ck1", type: { basic: TEXT } } ]
	    	values: [ %s ]
	  	) {
		name
	}
}`
	buffer := schemas.ExecutePost(routes, "/graphql-schema",
		fmt.Sprintf(mutation, ksName, tableName, buildColumnList("value", columnTypes)))
	response := schemas.DecodeResponse(buffer)
	Expect(response.Errors).To(HaveLen(0))
	return response
}

func AlterTableAdd(routes []graphql.Route, ksName string, tableName string, columnTypes []string) schemas.ResponseBody {
	mutation := `
mutation {
  alterTableAdd(keyspaceName:"%s", tableName:"%s", toAdd: [ %s ]) {
    name
  }
}`
	buffer := schemas.ExecutePost(routes, "/graphql-schema",
		fmt.Sprintf(mutation, ksName, tableName, buildColumnList("addedValue", columnTypes)))
	response := schemas.DecodeResponse(buffer)
	Expect(response.Errors).To(HaveLen(0))
	return response
}

func AlterTableDrop(routes []graphql.Route) {
}

func DropTable(routes []graphql.Route, ksName string, tableName string) {
}
