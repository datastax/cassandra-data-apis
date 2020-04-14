package quirky

import (
	"fmt"
	"github.com/iancoleman/strcase"
	. "github.com/onsi/gomega"
	"github.com/riptano/data-endpoints/graphql"
	"github.com/riptano/data-endpoints/internal/testutil/schemas"
)

func InsertAndSelect(routes []graphql.Route, name string) {
	insertQuery := `mutation {
	  insert%s(data:{id:%d, value:"%s"}) {
		applied
	  }
	}`
	selectQuery := `query {
	  %s(data:{id:%d}) {
		values {
		  id
		  value
		}
	  }
	}`

	value := schemas.NewUuid()
	buffer, err := schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(insertQuery, name, 1, value))
	Expect(err).ToNot(HaveOccurred())

	buffer, err = schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(selectQuery, strcase.ToLowerCamel(name), 1))
	Expect(err).ToNot(HaveOccurred())

	data := schemas.DecodeDataAsSliceOfMaps(buffer, strcase.ToLowerCamel(name), "values")
	Expect(data[0]["value"]).To(Equal(value))
}
