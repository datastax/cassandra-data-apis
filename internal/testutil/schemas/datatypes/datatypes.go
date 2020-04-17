package datatypes

import (
	"bytes"
	"fmt"
	"github.com/datastax/cassandra-data-apis/graphql"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas"
	. "github.com/onsi/gomega"
)

type ConvertFn func(value interface{}) interface{}

func MutateAndQueryScalar(
	routes []graphql.Route,
	datatype string,
	value interface{},
	format string,
	convert ConvertFn,
) {
	insertQuery := `mutation {
	  insertScalars(data:{id:"%s", %sCol:%s}) {
		applied
	  }
	}`
	selectQuery := `query {
	  scalars(data:{id:"%s"}) {
		values {
		  id
		  %sCol
		}
	  }
	}`
	deleteQuery := `mutation {
	  deleteScalars(data:{id:"%s"}) {
		applied
	  }
	}`
	updateQuery := `mutation {
	  updateScalars(data:{id:"%s", %sCol:%s}) {
		applied
	  }
	}`

	valueStr := fmt.Sprintf(format, value)
	id := schemas.NewUuid()
	var buffer *bytes.Buffer
	var data []map[string]interface{}
	if convert == nil {
		convert = func(value interface{}) interface{} {
			return value
		}
	}

	// Insert
	buffer = schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(insertQuery, id, datatype, valueStr))
	Expect(schemas.DecodeData(buffer, "insertScalars")["applied"]).To(Equal(true))

	// Select
	buffer = schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(selectQuery, id, datatype))
	data = schemas.DecodeDataAsSliceOfMaps(buffer, "scalars", "values")
	Expect(convert(data[0][datatype+"Col"])).To(Equal(value))

	// Delete
	buffer = schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(deleteQuery, id))
	Expect(schemas.DecodeData(buffer, "deleteScalars")["applied"]).To(Equal(true))

	// Verify deleted
	buffer = schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(selectQuery, id, datatype))
	Expect(schemas.DecodeDataAsSliceOfMaps(buffer, "scalars", "values")).To(HaveLen(0))

	// Update
	buffer = schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(updateQuery, id, datatype, valueStr))
	Expect(schemas.DecodeData(buffer, "updateScalars")["applied"]).To(Equal(true))

	// Verify updated
	buffer = schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(selectQuery, id, datatype))
	data = schemas.DecodeDataAsSliceOfMaps(buffer, "scalars", "values")
	Expect(convert(data[0][datatype+"Col"])).To(Equal(value))
}

func InsertScalarErrors(routes []graphql.Route, datatype string, value string) {
	insertQuery := `mutation {
	  insertScalars(data:{id:"%s", %sCol:%s}) {
		applied
	  }
	}`

	buffer := schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(insertQuery, schemas.NewUuid(), datatype, value))
	response := schemas.DecodeResponse(buffer)
	Expect(response.Errors).To(HaveLen(1))
	Expect(response.Errors[0].Message).To(ContainSubstring("invalid"))
}
