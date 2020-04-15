// +build integration

package db

import (
	"fmt"
	. "github.com/datastax/cassandra-data-apis/internal/testutil"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"gopkg.in/inf.v0"
	"math/big"
	"reflect"
	"strings"
	"time"
)

var db *Db

var _ = Describe("Session", func() {
	Describe("ExecuteIter()", func() {
		Context("With numerical values", func() {
			It("Should provide the expected representation", func() {
				queries := []string{
					"CREATE TABLE ks1.tbl_numerics (id int PRIMARY KEY, bigint_value bigint, float_value float," +
						" double_value double, smallint_value smallint, tinyint_value tinyint, decimal_value decimal," +
						" varint_value varint)",
					"INSERT INTO ks1.tbl_numerics (id, bigint_value, float_value, double_value, smallint_value, tinyint_value" +
						", decimal_value, varint_value) VALUES (1, 1, 1.1, 1.1, 1, 1, 1.25, 1)",
					"INSERT INTO ks1.tbl_numerics (id) VALUES (100)",
				}

				for _, query := range queries {
					err := db.session.Execute(query, nil)
					Expect(err).To(BeNil())
				}

				rs, err := db.session.ExecuteIter("SELECT * FROM ks1.tbl_numerics WHERE id = ?", nil, 1)
				assert.Nil(GinkgoT(), err)
				row := rs.Values()[0]
				assertPointer(new(string), "1", row["bigint_value"])
				assertPointer(new(float32), float32(1.1), row["float_value"])
				assertPointer(new(float64), 1.1, row["double_value"])
				assertPointer(new(int16), int16(1), row["smallint_value"])
				assertPointer(new(int8), int8(1), row["tinyint_value"])
				assertPointer(new(inf.Dec), *inf.NewDec(125, 2), row["decimal_value"])
				assertPointer(new(big.Int), *big.NewInt(1), row["varint_value"])

				// Assert nil values
				rs, err = db.session.ExecuteIter("SELECT * FROM ks1.tbl_numerics WHERE id = ?", nil, 100)
				assert.Nil(GinkgoT(), err)
				row = rs.Values()[0]
				assertNilPointer(new(string), row["bigint_value"])
				assertNilPointer(new(float32), row["float_value"])
				assertNilPointer(new(float64), row["double_value"])
				assertNilPointer(new(int16), row["smallint_value"])
				assertNilPointer(new(int8), row["tinyint_value"])
				assertNilPointer(new(inf.Dec), row["decimal_value"])
				assertNilPointer(new(big.Int), row["varint_value"])
			})
		})

		Context("With lists and sets", func() {
			It("Should provide the expected representation", func() {
				queries := []string{
					"CREATE TABLE ks1.tbl_lists (id int PRIMARY KEY, int_value list<int>, bigint_value list<bigint>," +
						" float_value list<float>, double_value list<double>, bool_value list<boolean>, text_value list<text>)",
					"INSERT INTO ks1.tbl_lists (id, int_value, bigint_value, float_value, double_value" +
						", bool_value, text_value) VALUES (1, [1], [1], [1.1], [2.1], [true], ['hello'])",
					"INSERT INTO ks1.tbl_lists (id) VALUES (100)",
				}

				for _, query := range queries {
					err := db.session.Execute(query, nil)
					assert.Nil(GinkgoT(), err)
				}

				var (
					rs  ResultSet
					err error
					row map[string]interface{}
				)

				//TODO: Test nulls and sets
				rs, err = db.session.ExecuteIter("SELECT * FROM ks1.tbl_lists WHERE id = ?", nil, 1)
				assert.Nil(GinkgoT(), err)
				row = rs.Values()[0]
				assertPointer(new([]int), []int{1}, row["int_value"])
				assertPointer(new([]string), []string{"1"}, row["bigint_value"])
				assertPointer(new([]float32), []float32{1.1}, row["float_value"])
				assertPointer(new([]float64), []float64{2.1}, row["double_value"])
				assertPointer(new([]bool), []bool{true}, row["bool_value"])
				assertPointer(new([]string), []string{"hello"}, row["text_value"])
			})
		})

		Context("With maps", func() {
			It("Should provide the expected representation", func() {
				queries := []string{
					"CREATE TABLE ks1.tbl_maps (id int PRIMARY KEY, m1 map<text, int>, m2 map<bigint, double>," +
						" m3 map<uuid, frozen<list<int>>>,  m4 map<smallint, varchar>)",
					"INSERT INTO ks1.tbl_maps (id, m1, m2, m3, m4) VALUES (1, {'a': 1}, {1: 1.1}" +
						", {e639af03-7851-49d7-a711-5ba81a0ff9c5: [1, 2]}, {4: 'four'})",
					"INSERT INTO ks1.tbl_maps (id) VALUES (100)",
				}

				for _, query := range queries {
					err := db.session.Execute(query, nil)
					assert.Nil(GinkgoT(), err)
				}

				var (
					rs  ResultSet
					err error
					row map[string]interface{}
				)

				rs, err = db.session.ExecuteIter("SELECT * FROM ks1.tbl_maps WHERE id = ?", nil, 1)
				assert.Nil(GinkgoT(), err)
				row = rs.Values()[0]
				assertPointer(new(map[string]int), map[string]int{"a": 1}, row["m1"])
				assertPointer(new(map[string]float64), map[string]float64{"1": 1.1}, row["m2"])
				assertPointer(new(map[string][]int), map[string][]int{"e639af03-7851-49d7-a711-5ba81a0ff9c5": {1, 2}}, row["m3"])
				assertPointer(new(map[int16]string), map[int16]string{int16(4): "four"}, row["m4"])
			})
		})

		Context("With scalars", func() {
			It("Should provide the expected representation", func() {

				queries := []string{
					"CREATE TABLE ks1.tbl_scalars (id int PRIMARY KEY, inet_value inet, uuid_value uuid, timeuuid_value timeuuid," +
						" timestamp_value timestamp, blob_value blob)",
					"INSERT INTO ks1.tbl_scalars (id) VALUES (100)",
				}

				for _, query := range queries {
					err := db.session.Execute(query, nil)
					assert.Nil(GinkgoT(), err)
				}

				id := 1
				timeValue := time.Time{}
				_ = timeValue.UnmarshalText([]byte("2019-12-31T23:59:59.999Z"))
				values := map[string]interface{}{
					"id":              id,
					"inet_value":      "10.10.150.1",
					"uuid_value":      "d2b99a72-4482-4064-8f96-ca7aba39a1ca",
					"timeuuid_value":  "308f185c-7272-11ea-bc55-0242ac130003",
					"timestamp_value": timeValue,
					"blob_value":      []byte{1, 2, 3, 4},
				}
				columns := make([]string, 0)
				parameters := make([]interface{}, 0)
				for k, v := range values {
					columns = append(columns, k)
					parameters = append(parameters, v)
				}

				insertQuery := fmt.Sprintf("INSERT INTO ks1.tbl_scalars (%s) VALUES (?%s)",
					strings.Join(columns, ", "), strings.Repeat(", ?", len(columns)-1))
				_, err := db.session.ExecuteIter(insertQuery, nil, parameters...)
				assert.Nil(GinkgoT(), err)

				selectQuery := fmt.Sprintf("SELECT %s FROM ks1.tbl_scalars WHERE id = ?", strings.Join(columns, ", "))
				rs, err := db.session.ExecuteIter(selectQuery, nil, id)
				assert.Nil(GinkgoT(), err)
				row := rs.Values()[0]
				for key, value := range values {
					assertPointerValue(value, row[key])
				}
			})
		})
	})
})

var _ = BeforeSuite(func() {
	session := SetupIntegrationTestFixture("CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}")

	db = NewDbWithConnectedInstance(session)
})

var _ = AfterSuite(func() {
	TearDownIntegrationTestFixture()
})

func assertPointer(expectedType interface{}, expected interface{}, actual interface{}) {
	assert.IsType(GinkgoT(), expectedType, actual)
	assertPointerValue(expected, actual)
}

func assertPointerValue(expected interface{}, actual interface{}) {
	assert.Equal(GinkgoT(), expected, reflect.ValueOf(actual).Elem().Interface())
}

func assertNilPointer(expectedType interface{}, actual interface{}) {
	assert.IsType(GinkgoT(), expectedType, actual)
	assert.Nil(GinkgoT(), actual)
}
