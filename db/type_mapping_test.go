package db

import (
	"github.com/stretchr/testify/assert"
	"gopkg.in/inf.v0"
	"reflect"
	"testing"
)

func (suite *IntegrationTestSuite) TestNumericCqlTypeMapping() {
	queries := []string{
		"CREATE TABLE ks1.tbl_numerics (id int PRIMARY KEY, bigint_value bigint, float_value float," +
			" double_value double, smallint_value smallint, tinyint_value tinyint, decimal_value decimal)",
		"INSERT INTO ks1.tbl_numerics (id, bigint_value, float_value, double_value, smallint_value, tinyint_value" +
			", decimal_value) VALUES (1, 1, 1.1, 1.1, 1, 1, 1.25)",
		"INSERT INTO ks1.tbl_numerics (id) VALUES (100)",
	}

	for _, query := range queries {
		err := suite.db.session.Execute(query, nil)
		assert.Nil(suite.T(), err)
	}

	var (
		rs  ResultSet
		err error
		row map[string]interface{}
	)

	rs, err = suite.db.session.ExecuteIter("SELECT * FROM ks1.tbl_numerics WHERE id = ?", nil, 1)
	assert.Nil(suite.T(), err)
	row = rs.Values()[0]
	assertPointerValue(suite.T(), new(string), "1", row["bigint_value"])
	assertPointerValue(suite.T(), new(float32), float32(1.1), row["float_value"])
	assertPointerValue(suite.T(), new(float64), 1.1, row["double_value"])
	assertPointerValue(suite.T(), new(int16), int16(1), row["smallint_value"])
	assertPointerValue(suite.T(), new(int8), int8(1), row["tinyint_value"])
	assertPointerValue(suite.T(), new(inf.Dec), *inf.NewDec(125, 2), row["decimal_value"])

	// Assert nil values
	rs, err = suite.db.session.ExecuteIter("SELECT * FROM ks1.tbl_numerics WHERE id = ?", nil, 100)
	assert.Nil(suite.T(), err)
	row = rs.Values()[0]
	assertNilPointer(suite.T(), new(string), row["bigint_value"])
	assertNilPointer(suite.T(), new(float32), row["float_value"])
	assertNilPointer(suite.T(), new(float64), row["double_value"])
	assertNilPointer(suite.T(), new(int16), row["smallint_value"])
	assertNilPointer(suite.T(), new(int8), row["tinyint_value"])
	assertNilPointer(suite.T(), new(inf.Dec), row["decimal_value"])
}

func (suite *IntegrationTestSuite) TestCollectionCqlTypeMapping() {
	queries := []string{
		"CREATE TABLE ks1.tbl_lists (id int PRIMARY KEY, int_value list<int>, bigint_value list<bigint>," +
			" float_value list<float>, double_value list<double>, bool_value list<boolean>, text_value list<text>)",
		"INSERT INTO ks1.tbl_lists (id, int_value, bigint_value, float_value, double_value" +
			", bool_value, text_value) VALUES (1, [1], [1], [1.1], [2.1], [true], ['hello'])",
		"INSERT INTO ks1.tbl_lists (id) VALUES (100)",
	}

	for _, query := range queries {
		err := suite.db.session.Execute(query, nil)
		assert.Nil(suite.T(), err)
	}

	var (
		rs  ResultSet
		err error
		row map[string]interface{}
	)

	//TODO: Test nulls and sets
	rs, err = suite.db.session.ExecuteIter("SELECT * FROM ks1.tbl_lists WHERE id = ?", nil, 1)
	assert.Nil(suite.T(), err)
	row = rs.Values()[0]
	assertPointerValue(suite.T(), new([]int), []int{1}, row["int_value"])
	assertPointerValue(suite.T(), new([]string), []string{"1"}, row["bigint_value"])
	assertPointerValue(suite.T(), new([]float32), []float32{1.1}, row["float_value"])
	assertPointerValue(suite.T(), new([]float64), []float64{2.1}, row["double_value"])
	assertPointerValue(suite.T(), new([]bool), []bool{true}, row["bool_value"])
	assertPointerValue(suite.T(), new([]string), []string{"hello"}, row["text_value"])
}

func (suite *IntegrationTestSuite) TestMapCqlTypeMapping() {
	queries := []string{
		"CREATE TABLE ks1.tbl_maps (id int PRIMARY KEY, m1 map<text, int>, m2 map<bigint, double>," +
			" m3 map<uuid, frozen<list<int>>>,  m4 map<smallint, varchar>)",
		"INSERT INTO ks1.tbl_maps (id, m1, m2, m3, m4) VALUES (1, {'a': 1}, {1: 1.1}" +
			", {e639af03-7851-49d7-a711-5ba81a0ff9c5: [1, 2]}, {4: 'four'})",
		"INSERT INTO ks1.tbl_maps (id) VALUES (100)",
	}

	for _, query := range queries {
		err := suite.db.session.Execute(query, nil)
		assert.Nil(suite.T(), err)
	}

	var (
		rs  ResultSet
		err error
		row map[string]interface{}
	)

	rs, err = suite.db.session.ExecuteIter("SELECT * FROM ks1.tbl_maps WHERE id = ?", nil, 1)
	assert.Nil(suite.T(), err)
	row = rs.Values()[0]
	assertPointerValue(suite.T(), new(map[string]int), map[string]int{"a": 1}, row["m1"])
	assertPointerValue(suite.T(), new(map[string]float64), map[string]float64{"1": 1.1}, row["m2"])
	assertPointerValue(suite.T(),
		new(map[string][]int), map[string][]int{"e639af03-7851-49d7-a711-5ba81a0ff9c5": {1, 2}}, row["m3"])
	assertPointerValue(suite.T(),
		new(map[int16]string), map[int16]string{int16(4): "four"}, row["m4"])
}

func assertPointerValue(t *testing.T, expectedType interface{}, expected interface{}, actual interface{}) {
	assert.IsType(t, expectedType, actual)
	assert.Equal(t, expected, reflect.ValueOf(actual).Elem().Interface())
}

func assertNilPointer(t *testing.T, expectedType interface{}, actual interface{}) {
	assert.IsType(t, expectedType, actual)
	assert.Nil(t, actual)
}
