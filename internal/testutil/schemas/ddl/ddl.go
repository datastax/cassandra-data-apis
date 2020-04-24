package ddl

import (
	"fmt"
	"github.com/datastax/cassandra-data-apis/graphql"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas"
	. "github.com/onsi/gomega"
)

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

func CreateTable(routes []graphql.Route, ksName string, tableName string, valType string) schemas.ResponseBody {
	mutation := `mutation { 
	createTable(
	    	keyspaceName:"%s", 
	    	tableName:"%s", 
	    	partitionKeys: [ { name: "pk1", type: { basic: TEXT } } ]
	    	clusteringKeys: [ { name: "ck1", type: { basic: TEXT } } ]
	    	values: [ { name: "value1", type: %s } ]
	  	) {
		name
	}
}`
	buffer := schemas.ExecutePost(routes, "/graphql-schema",
		fmt.Sprintf(mutation, ksName, tableName, valType))
	response := schemas.DecodeResponse(buffer)
	Expect(response.Errors).To(HaveLen(0))
	return response
}

func AlterTableAdd(routes []graphql.Route, ksName string, tableName string, columnType string) schemas.ResponseBody {
	mutation := `
mutation {
  alterTableAdd(keyspaceName:"%s", tableName:"%s", toAdd: {name: "value2", type: %s}) {
    name
  }
}`
	buffer := schemas.ExecutePost(routes, "/graphql-schema",
		fmt.Sprintf(mutation, ksName, tableName, columnType))
	response := schemas.DecodeResponse(buffer)
	Expect(response.Errors).To(HaveLen(0))
	return response
}

func AlterTableDrop(routes []graphql.Route) {
}

func DropTable(routes []graphql.Route, ksName string, tableName string) {
}
