package datatypes

import (
	"bytes"
	"fmt"
	"github.com/datastax/cassandra-data-apis/graphql"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas"
	. "github.com/onsi/gomega"
	"sync/atomic"
)

var index int32 = 0

func MutateAndQueryScalar(routes []graphql.Route, datatype string, value interface{}, format string) {
	insertQuery := `mutation {
	  insertScalars(data:{id:%d, %sCol:%s}) {
		applied
	  }
	}`
	selectQuery := `query {
	  scalars(data:{id:%d}) {
		values {
		  id
		  %sCol
		}
	  }
	}`
	deleteQuery := `mutation {
	  deleteScalars(data:{id:%d}) {
		applied
	  }
	}`
	updateQuery := `mutation {
	  updateScalars(data:{id:%d, %sCol:%s}) {
		applied
	  }
	}`

	valueStr := fmt.Sprintf(format, value)
	id := atomic.AddInt32(&index, 1)
	var buffer *bytes.Buffer
	var data []map[string]interface{}

	// Insert
	buffer = schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(insertQuery, id, datatype, valueStr))
	Expect(schemas.DecodeData(buffer, "insertScalars")["applied"]).To(Equal(true))

	// Select
	buffer = schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(selectQuery, id, datatype))
	data = schemas.DecodeDataAsSliceOfMaps(buffer, "scalars", "values")
	Expect(data[0][datatype+"Col"]).To(Equal(value))

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
	Expect(data[0][datatype+"Col"]).To(Equal(value))
}
