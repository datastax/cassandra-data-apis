package datatypes

import (
	"bytes"
	"fmt"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas"
	"github.com/datastax/cassandra-data-apis/types"
	"github.com/gocql/gocql"
	. "github.com/onsi/gomega"
	"math"
)

type ConvertFn func(value interface{}) interface{}

func MutateAndQueryScalar(
	routes []types.Route,
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

func InsertScalarErrors(routes []types.Route, datatype string, value string) {
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

func InsertAndUpdateNulls(routes []types.Route, datatype string, jsonValue interface{}) {
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
	routes []types.Route,
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

func MutateAndQueryStatic(routes []types.Route) {
	insertQueryWithStatic := `mutation {
	  insertTableStatic(value:{id1: "%s", id2: %d, value: %d, valueStatic: %v}) {
		applied
	  }
	}`
	insertQuery := `mutation {
	  insertTableStatic(value:{id1: "%s", id2: %d, value: %d}) {
		applied
	  }
	}`
	selectQuery := `query {
	  tableStatic(value:{id1:"%s"}) {
		values {
		  id1
		  id2
		  value
		  valueStatic
		}
	  }
	}`

	id := schemas.NewUuid()
	jsonValue := float64(100)

	// Insert 2 rows in the same partition, one including the static value
	buffer := schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(insertQueryWithStatic, id, 1, 1, jsonValue))
	Expect(schemas.DecodeData(buffer, "insertTableStatic")["applied"]).To(Equal(true))
	buffer = schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(insertQuery, id, 2, 2))
	Expect(schemas.DecodeData(buffer, "insertTableStatic")["applied"]).To(Equal(true))

	// Select
	buffer = schemas.ExecutePost(routes, "/graphql", fmt.Sprintf(selectQuery, id))
	values := schemas.DecodeDataAsSliceOfMaps(buffer, "tableStatic", "values")
	Expect(values).To(HaveLen(2))
	// The static value should be present in all rows for the partition
	for _, value := range values {
		Expect(value["valueStatic"]).To(Equal(jsonValue))
	}
}

// ScalarJsonValues gets a slice containing one slice per scalar data type with name in first position and json values in
// the following positions.
func ScalarJsonValues() [][]interface{} {
	return [][]interface{}{
		{"float", float64(0), float64(-1), 1.25, 3.40282},
		{"double", float64(1), float64(0), -1.25, math.MaxFloat64},
		{"boolean", true, false},
		{"tinyint", float64(1)},
		{"int", float64(2)},
		{"bigint", "123"},
		{"varint", "123"},
		{"decimal", "123.080000"},
		{"timeuuid", gocql.TimeUUID().String()},
		{"uuid", schemas.NewUuid()},
		{"inet", "10.11.150.201"},
		{"blob", "ABEi"},
		{"timestamp", "2005-08-05T13:20:21.52Z"},
		{"time", "08:45:02"},
	}
}
