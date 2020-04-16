package datatypes

import (
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

	valueStr := fmt.Sprintf(format, value)

	id := atomic.AddInt32(&index, 1)
	schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(insertQuery, id, datatype, valueStr))
	buffer := schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(selectQuery, id, datatype))
	data := schemas.DecodeDataAsSliceOfMaps(buffer, "scalars", "values")
	actual := data[0][datatype+"Col"]
	Expect(actual).To(Equal(value))
}
