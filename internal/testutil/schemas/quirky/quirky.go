package quirky

import (
	"fmt"
	"github.com/datastax/cassandra-data-apis/graphql"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas"
	"github.com/iancoleman/strcase"
	. "github.com/onsi/gomega"
)

func InsertAndSelect(routes []graphql.Route, name string) {
	insertQuery := `mutation {
	  insert%s(value:{id:%d, value:"%s"}) {
		applied
	  }
	}`
	selectQuery := `query {
	  %s(value:{id:%d}) {
		values {
		  id
		  value
		}
	  }
	}`

	value := schemas.NewUuid()
	schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(insertQuery, name, 1, value))
	buffer := schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(selectQuery, strcase.ToLowerCamel(name), 1))
	values := schemas.DecodeDataAsSliceOfMaps(buffer, strcase.ToLowerCamel(name), "values")
	Expect(values[0]["value"]).To(Equal(value))
}

func InsertWeirdCase(routes []graphql.Route, id int) {
	query := `mutation { 
	  insertWEIRDCASE(value: { id: %d, aBCdef: "one", qAData: "two", abcXYZ: "three" }) {
		applied
	  }
	}`
	buffer := schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(query, id))
	data := schemas.DecodeData(buffer, "insertWEIRDCASE")
	Expect(data["applied"]).To(Equal(true))
}

func SelectWeirdCase(routes []graphql.Route, id int) {
	query := `{
	  wEIRDCASE(value: {id: %d }) { 
		values { aBCdef, abcXYZ, qAData }
	  }
	}`
	buffer := schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(query, id))
	data := schemas.DecodeData(buffer, "wEIRDCASE")
	Expect(data["values"]).To(ConsistOf(map[string]interface{}{"aBCdef": "one", "abcXYZ": "three", "qAData": "two"}))
}
