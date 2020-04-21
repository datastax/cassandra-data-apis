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
	  insertScalars(value:{id:"%s", %sCol:%s}) {
		applied
	  }
	}`
	selectQuery := `query {
	  scalars(value:{id:"%s"}) {
		values {
		  id
		  %sCol
		}
	  }
	}`
	deleteQuery := `mutation {
	  deleteScalars(value:{id:"%s"}) {
		applied
	  }
	}`
	updateQuery := `mutation {
	  updateScalars(value:{id:"%s", %sCol:%s}) {
		applied
	  }
	}`

	valueStr := fmt.Sprintf(format, value)
	id := schemas.NewUuid()
	var buffer *bytes.Buffer
	var values []map[string]interface{}
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
	values = schemas.DecodeDataAsSliceOfMaps(buffer, "scalars", "values")
	Expect(convert(values[0][datatype+"Col"])).To(Equal(value))

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
	values = schemas.DecodeDataAsSliceOfMaps(buffer, "scalars", "values")
	Expect(convert(values[0][datatype+"Col"])).To(Equal(value))
}

func InsertScalarErrors(routes []graphql.Route, datatype string, value string) {
	insertQuery := `mutation {
	  insertScalars(value:{id:"%s", %sCol:%s}) {
		applied
	  }
	}`

	buffer := schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(insertQuery, schemas.NewUuid(), datatype, value))
	response := schemas.DecodeResponse(buffer)
	Expect(response.Errors).To(HaveLen(1))
	Expect(response.Errors[0].Message).To(ContainSubstring("invalid"))
}

func InsertAndUpdateNulls(routes []graphql.Route, datatype string, jsonValue interface{}) {
	insertQuery := `mutation {
	  insertScalars(value:{id:"%s", %sCol:%s}) {
		applied
	  }
	}`
	selectQuery := `query {
	  scalars(value:{id:"%s"}) {
		values {
		  id
		  %sCol
		}
	  }
	}`
	updateQuery := `mutation {
	  updateScalars(value:{id:"%s", %sCol:null}) {
		applied
	  }
	}`

	valueStr := fmt.Sprintf("%v", jsonValue)
	if _, ok := jsonValue.(string); ok {
		valueStr = fmt.Sprintf(`"%s"`, jsonValue)
	}
	id := schemas.NewUuid()
	var buffer *bytes.Buffer
	var values []map[string]interface{}

	// Insert
	buffer = schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(insertQuery, id, datatype, valueStr))
	Expect(schemas.DecodeData(buffer, "insertScalars")["applied"]).To(Equal(true))

	// Select
	buffer = schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(selectQuery, id, datatype))
	values = schemas.DecodeDataAsSliceOfMaps(buffer, "scalars", "values")
	Expect(values[0][datatype+"Col"]).To(Equal(jsonValue))

	// Update
	buffer = schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(updateQuery, id, datatype))
	Expect(schemas.DecodeData(buffer, "updateScalars")["applied"]).To(Equal(true))

	// Select
	buffer = schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(selectQuery, id, datatype))
	values = schemas.DecodeDataAsSliceOfMaps(buffer, "scalars", "values")
	Expect(values[0][datatype+"Col"]).To(BeNil())
}

func MutateAndQueryCollection(
	routes []graphql.Route,
	fieldName string,
	stringValue string,
	jsonValue []interface{},
	isMap bool,
) {
	updateQuery := `mutation {
	  updateCollections(value:{id: "%s", %s: %s}) {
		applied
	  }
	}`
	selectQuery := `query {
	  collections(value:{id:"%s"}) {
		values {
		  id
		  %s
		}
	  }
	}`

	id := schemas.NewUuid()
	buffer := schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(updateQuery, id, fieldName, stringValue))
	Expect(schemas.DecodeData(buffer, "updateCollections")["applied"]).To(Equal(true))

	selectFieldName := fieldName
	if isMap {
		selectFieldName = fmt.Sprintf("%s {key, value}", fieldName)
	}

	buffer = schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(selectQuery, id, selectFieldName))
	values := schemas.DecodeDataAsSliceOfMaps(buffer, "collections", "values")
	value := values[0][fieldName]
	if !isMap {
		Expect(value).To(Equal(jsonValue))
	} else {
		Expect(value).To(ContainElements(jsonValue))
	}
}
