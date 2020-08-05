// +build integration

package endpoint

import (
	"bytes"
	"encoding/json"
	"fmt"
	c "github.com/datastax/cassandra-data-apis/config"
	"github.com/datastax/cassandra-data-apis/db"
	"github.com/datastax/cassandra-data-apis/graphql"
	. "github.com/datastax/cassandra-data-apis/internal/testutil"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas/datatypes"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas/ddl"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas/killrvideo"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas/quirky"
	"github.com/datastax/cassandra-data-apis/types"
	"github.com/gocql/gocql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/inf.v0"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

var _ = Describe("DataEndpoint", func() {
	EnsureCcmCluster(func() {
		CreateSchema("killrvideo")
		CreateSchema("quirky")
		CreateSchema("datatypes")
	})

	Describe("RoutesKeyspaceGraphQL()", func() {
		var session *gocql.Session

		BeforeEach(func() {
			session = GetSession()
		})

		Context("With killrvideo schema", func() {
			config := NewEndpointConfigWithLogger(TestLogger(), host)
			keyspace := "killrvideo"
			It("Should insert and select users", func() {
				routes := getRoutes(config, keyspace)
				Expect(routes).To(HaveLen(2))

				id := schemas.NewUuid()

				buffer, err := executePost(routes, "/graphql", graphql.RequestBody{
					Query: killrvideo.InsertUserMutation(id, "John", "john@email.com", false),
				}, nil)
				Expect(err).ToNot(HaveOccurred())
				expected := schemas.NewResponseBody("insertUsers", map[string]interface{}{
					"applied": true,
					"value": map[string]interface{}{
						"createdDate": nil,
						"email":       "john@email.com",
						"firstname":   "John",
						"lastname":    nil,
						"userid":      id,
					},
				})
				Expect(schemas.DecodeResponse(buffer)).To(Equal(expected))

				buffer, err = executePost(routes, "/graphql", graphql.RequestBody{
					Query: killrvideo.SelectUserQuery(id),
				}, nil)
				Expect(err).ToNot(HaveOccurred())

				values := []map[string]interface{}{{
					"createdDate": nil,
					"email":       "john@email.com",
					"firstname":   "John",
					"lastname":    nil,
					"userid":      id,
				}}

				data := schemas.DecodeData(buffer, "users")
				Expect(data["values"]).To(ConsistOf(values))
				Expect(data["pageState"]).To(BeEmpty())
			})

			It("Should support page size and state", func() {
				// Insert some data
				length := 5
				for i := 1; i <= length; i++ {
					killrvideo.CqlInsertTagByLetter(session, fmt.Sprintf("a%d", i))
				}

				routes := getRoutes(config, keyspace)

				queryTags := func(pageState string, expectedValues []map[string]interface{}) string {
					buffer, err := executePost(routes, "/graphql", graphql.RequestBody{
						Query: killrvideo.SelectTagsByLetter("a", 2, pageState),
					}, nil)
					Expect(err).ToNot(HaveOccurred())
					data := schemas.DecodeData(buffer, "tagsByLetter")
					Expect(data["values"]).To(ConsistOf(expectedValues))
					return data["pageState"].(string)
				}

				// Use an empty page state
				pageState := queryTags("", []map[string]interface{}{{"tag": "a1"}, {"tag": "a2"}})
				Expect(pageState).NotTo(HaveLen(0))

				// Use the previous page state
				pageState = queryTags(pageState, []map[string]interface{}{{"tag": "a3"}, {"tag": "a4"}})
				Expect(pageState).NotTo(HaveLen(0))

				// Last page
				pageState = queryTags(pageState, []map[string]interface{}{{"tag": "a5"}})
				// No more pages
				Expect(pageState).To(HaveLen(0))
			})

			It("Should support querying without where clause", func() {
				// Insert some data
				length := c.DefaultPageSize + 2
				for i := 1; i <= length; i++ {
					killrvideo.CqlInsertTagByLetter(session, fmt.Sprintf("b%d", i))
				}

				routes := getRoutes(config, keyspace)
				query := killrvideo.SelectTagsByLetterNoWhereClause("")
				buffer := schemas.ExecutePost(routes, "/graphql", query)
				data := schemas.DecodeData(buffer, "tagsByLetter")
				Expect(data["values"]).To(HaveLen(c.DefaultPageSize))
				pageState := data["pageState"]
				// Further pages should be signaled
				Expect(pageState).NotTo(BeEmpty())

				// query the following page
				query = killrvideo.SelectTagsByLetterNoWhereClause(pageState.(string))
				buffer = schemas.ExecutePost(routes, "/graphql", query)
				data = schemas.DecodeData(buffer, "tagsByLetter")
				Expect(data["values"]).NotTo(BeEmpty())
				Expect(data["pageState"]).NotTo(Equal(pageState))
			})

			It("Should support normal and conditional updates", func() {
				routes := getRoutes(config, keyspace)
				id := schemas.NewUuid()
				firstEmail := "email1@email.com"
				buffer, err := executePost(routes, "/graphql", graphql.RequestBody{
					Query: killrvideo.UpdateUserMutation(id, "John", firstEmail, ""),
				}, nil)
				Expect(err).ToNot(HaveOccurred())
				data := schemas.DecodeData(buffer, "updateUsers")
				Expect(data["applied"]).To(BeTrue())
				Expect(data["value"]).To(Equal(map[string]interface{}{
					"userid":    id,
					"firstname": "John",
					"email":     firstEmail,
				}))

				// This should not be applied
				buffer, _ = executePost(routes, "/graphql", graphql.RequestBody{
					Query: killrvideo.UpdateUserMutation(id, "John", "new_email@email.com", "email_old@email.com"),
				}, nil)
				data = schemas.DecodeData(buffer, "updateUsers")
				// Verify that the mutation was not applied on C* side
				Expect(data["applied"]).To(BeFalse())
				Expect(data["value"]).To(Equal(map[string]interface{}{
					"userid":    id,
					"firstname": nil,
					"email":     firstEmail,
				}))

				// This should be applied
				buffer, _ = executePost(routes, "/graphql", graphql.RequestBody{
					Query: killrvideo.UpdateUserMutation(id, "John", "new_email@email.com", firstEmail),
				}, nil)
				data = schemas.DecodeData(buffer, "updateUsers")
				// Verify that the mutation was applied on C* side
				Expect(data["applied"]).To(BeTrue())
			})

			It("Should support conditional inserts", func() {
				routes := getRoutes(config, keyspace)
				value := map[string]interface{}{
					"userid":      schemas.NewUuid(),
					"firstname":   "John",
					"email":       "john@bonham.com",
					"createdDate": nil,
					"lastname":    nil,
				}

				buffer, err := executePost(routes, "/graphql", graphql.RequestBody{
					Query: killrvideo.InsertUserMutation(value["userid"], value["firstname"], value["email"], true),
				}, nil)
				Expect(err).ToNot(HaveOccurred())
				data := schemas.DecodeData(buffer, "insertUsers")
				Expect(data["applied"]).To(BeTrue())
				Expect(data["value"]).To(Equal(value))

				buffer, err = executePost(routes, "/graphql", graphql.RequestBody{
					Query: killrvideo.InsertUserMutation(value["userid"], value["firstname"], value["email"], true),
				}, nil)
				Expect(err).ToNot(HaveOccurred())
				data = schemas.DecodeData(buffer, "insertUsers")
				Expect(data["applied"]).To(BeFalse())
				Expect(data["value"]).To(Equal(value))
			})

			It("Should support normal and conditional deletes", func() {
				routes := getRoutes(config, keyspace)
				id1 := schemas.NewUuid()
				id2 := schemas.NewUuid()
				name := "John"

				insertQuery := "INSERT INTO killrvideo.users (userid, firstname) VALUES (?, ?)"
				selectQuery := "SELECT firstname FROM killrvideo.users WHERE userid = ?"
				Expect(session.Query(insertQuery, id1, name).Exec()).NotTo(HaveOccurred())
				Expect(session.Query(insertQuery, id2, name).Exec()).NotTo(HaveOccurred())

				// Normal delete
				buffer, err := executePost(routes, "/graphql", graphql.RequestBody{
					Query: killrvideo.DeleteUserMutation(id1, ""),
				}, nil)
				Expect(err).ToNot(HaveOccurred())
				data := schemas.DecodeData(buffer, "deleteUsers")
				Expect(data["applied"]).To(BeTrue())
				Expect(data["value"]).To(Equal(map[string]interface{}{
					"userid":    id1,
					"firstname": nil,
					"email":     nil,
				}))
				iter := session.Query(selectQuery, id1).Iter()
				Expect(iter.NumRows()).To(BeZero())
				Expect(iter.Close()).ToNot(HaveOccurred())

				// Conditional delete
				buffer, err = executePost(routes, "/graphql", graphql.RequestBody{
					Query: killrvideo.DeleteUserMutation(id2, name),
				}, nil)
				Expect(err).ToNot(HaveOccurred())
				data = schemas.DecodeData(buffer, "deleteUsers")
				Expect(data["applied"]).To(BeFalse())
				Expect(data["value"]).To(Equal(map[string]interface{}{
					"userid":    id2,
					"firstname": "John",
					"email":     nil,
				}))
				iter = session.Query(selectQuery, id2).Iter()
				Expect(iter.NumRows()).To(Equal(1))
				Expect(iter.Close()).ToNot(HaveOccurred())
			})

			It("Should support query filters", func() {
				// Insert some data
				videoId, _ := gocql.RandomUUID()
				t0 := gocql.UUIDFromTime(time.Date(2010, 4, 29, 0, 0, 0, 0, time.Local))
				insertQuery := "INSERT INTO killrvideo.comments_by_video (videoid, commentid, comment) VALUES (?, ?, ?)"
				err := session.Query(insertQuery, videoId, t0, "comment 0").Exec()
				Expect(err).ToNot(HaveOccurred())
				length := 5
				// Insert more data with timeuuid greater than t0
				for i := 1; i <= length; i++ {
					err := session.Query(insertQuery, videoId, gocql.TimeUUID(), fmt.Sprintf("comment %d", i)).Exec()
					Expect(err).ToNot(HaveOccurred())
				}

				routes := getRoutes(config, keyspace)
				buffer, err := executePost(routes, "/graphql", graphql.RequestBody{
					Query: killrvideo.SelectCommentsByVideoGreaterThan(videoId.String(), t0.String()),
				}, nil)
				Expect(err).ToNot(HaveOccurred())

				actual := schemas.DecodeDataAsSliceOfMaps(buffer, "commentsByVideoFilter", "values")
				Expect(actual).To(HaveLen(length))
				sort.SliceStable(actual, func(i, j int) bool {
					return actual[i]["comment"].(string) < actual[j]["comment"].(string)
				})
				for i := 0; i < length; i++ {
					Expect(actual[i]["comment"]).To(Equal(fmt.Sprintf("comment %d", i+1)))
				}
			})

			It("Should create types per table", func() {
				routes := getRoutes(config, keyspace)
				buffer, err := executePost(routes, "/graphql", graphql.RequestBody{
					Query: schemas.GraphQLTypesQuery,
				}, nil)

				Expect(err).ToNot(HaveOccurred())
				result := schemas.DecodeDataAsSliceOfMaps(buffer, "__schema", "types")
				typeNames := make([]string, 0, len(result))
				for _, item := range result {
					typeNames = append(typeNames, item["name"].(string))
				}

				Expect(typeNames).To(ContainElements(schemas.GetTypeNamesByTable("videos_by_tag")))
				Expect(typeNames).To(ContainElements(schemas.GetTypeNamesByTable("user_videos")))
				Expect(typeNames).To(ContainElements(schemas.GetTypeNamesByTable("comments_by_video")))
				Expect(typeNames).To(ContainElements(schemas.GetTypeNamesByTable("video_event")))
				Expect(typeNames).To(ContainElements("BigInt", "Counter", "Uuid", "TimeUuid"))
			})

			It("Should return an error when query is not found", func() {
				query := `query {
				 insertNotFound {
					values {
					  name
					}
				 }
				}`
				schemas.ExpectQueryToReturnError(getRoutes(config, keyspace), query, "Cannot query field")
			})

			It("Should return an error when selection field is not found", func() {
				query := `query {
				 videosByTag (value: { tag: "a"}) {
					values {
					  tag
					  fieldNotFound
					}
				 }
				}`
				schemas.ExpectQueryToReturnError(
					getRoutes(config, keyspace), query, `Cannot query field "fieldNotFound" on type`)
			})

			It("Should return an error when condition field is not found", func() {
				query := `query {
				 videosByTag (value: { fieldNotFound: "a"}) {
					values {
					  tag
					}
				 }
				}`
				schemas.ExpectQueryToReturnError(
					getRoutes(config, keyspace), query, `Argument "value" has invalid value`)
			})

			It("Should return an error when parameter is not found", func() {
				query := `query {
				 videosByTag (value: { tag: "a"}, paramNotFound: true) {
					values {
					  tag
					}
				 }
				}`
				schemas.ExpectQueryToReturnError(
					getRoutes(config, keyspace), query, `Unknown argument "paramNotFound" on field "videosByTag"`)
			})
		})

		Context("With quirky schema", func() {
			config := NewEndpointConfigWithLogger(TestLogger(), host)
			keyspace := "quirky"

			It("Should build tables with supported types", func() {
				routes := getRoutes(config, keyspace)
				buffer, err := executePost(routes, "/graphql", graphql.RequestBody{
					Query: schemas.GraphQLTypesQuery,
				}, nil)

				Expect(err).ToNot(HaveOccurred())
				result := schemas.DecodeDataAsSliceOfMaps(buffer, "__schema", "types")
				typeNames := make([]string, 0, len(result))
				for _, item := range result {
					typeNames = append(typeNames, item["name"].(string))
				}

				Expect(typeNames).To(ContainElements(schemas.GetTypeNamesByTable("valid_sample")))
			})

			It("Should support reserved names", func() {
				routes := getRoutes(config, keyspace)
				names := []string{
					"ColumnCustom", "ColumnCustom2", "ConsistencyCustom", "DataTypeCustom", "BasicTypeCustom",
				}
				for _, name := range names {
					quirky.InsertAndSelect(routes, name)
				}
			})

			It("Should support conflicting names", func() {
				routes := getRoutes(config, keyspace)
				names := []string{"TesterAbc", "TesterAbc2"}
				for _, name := range names {
					quirky.InsertAndSelect(routes, name)
				}
			})

			It("Should support case sensitive column names", func() {
				routes := getRoutes(config, keyspace)
				quirky.InsertWeirdCase(routes, 1)
				quirky.SelectWeirdCase(routes, 1)
			})

			It("Should not allow direct mutations on materialized views", func() {
				routes := getRoutes(config, keyspace)

				// Insert value into the view's table
				id := quirky.InsertAndSelect(routes, "TableWithView")

				// Queries should still work
				query := `query {
				  tablesView {
				    values {
				      id
				    }
				  }
				}`

				response := schemas.DecodeResponse(schemas.ExecutePost(routes, "/grqphql", query))
				expected := schemas.NewResponseBody("tablesView", map[string]interface{}{
					"values": []interface{}{
						map[string]interface{}{
							"id": float64(id),
						},
					},
				})
				Expect(response).To(Equal(expected))

				// Mutations for the view should not be present
				mutation := `mutation {
				  insertTablesView(value: {id:1, value:"test"}) {
				    applied
				  }
				}`

				schemas.ExpectQueryToReturnError(routes, mutation, `Cannot query field "insertTablesView"`)
			})
		})

		Context("With datatypes schema", func() {
			var routes []types.Route
			config := NewEndpointConfigWithLogger(TestLogger(), host)

			BeforeEach(func() {
				routes = getRoutes(config, "datatypes")
			})

			It("Should support text and varchar data types", func() {
				values := []string{"Привет мир", "नमस्ते दुनिया", "Jürgen"}
				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "text", "String!", value, `"%s"`, nil, nil)
					datatypes.MutateAndQueryScalar(routes, "varchar", "String!", value, `"%s"`, nil, nil)
				}
			})

			It("Should support ascii data type", func() {
				values := []string{"ABC", "><=;#{}[]", "abc"}
				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "ascii", "Ascii!", value, `"%s"`, nil, nil)
				}
			})

			It("Should support inet data type", func() {
				values := []string{"127.0.0.1", "::1", "10.1.2.250", "8.8.8.8", "fe80::aede:48ff:fe00:1122"}
				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "inet", "Inet!", value, `"%s"`, nil, nil)
				}
			})

			It("Should support blob data type", func() {
				values := []string{"VGhl", "ABEi", "AA==", "ESIira0="}
				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "blob", "Blob!", value, `"%s"`, nil, nil)
				}
			})

			It("Should support boolean data type", func() {
				values := []bool{true, false}
				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "boolean", "Boolean!", value, `%t`, nil, nil)
				}
			})

			It("Should support int data type", func() {
				values := []int{1, -2, 0, math.MaxInt32, math.MinInt32}
				toInt := jsonNumberTo(func(v float64) interface{} {
					return int(v)
				})
				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "int", "Int!", value, "%d", toInt, nil)
				}
			})

			It("Should support tinyint data type", func() {
				values := []int8{1, -2, 0, math.MaxInt8, math.MinInt8}
				toInt8 := jsonNumberTo(func(v float64) interface{} {
					return int8(v)
				})
				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "int", "Int!", value, "%d", toInt8, nil)
				}
			})

			It("Should support smallint data type", func() {
				values := []int16{1, -2, 0, math.MaxInt16, math.MinInt16}
				toInt16 := jsonNumberTo(func(v float64) interface{} {
					return int16(v)
				})
				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "int", "Int!", value, "%d", toInt16, nil)
				}
			})

			It("Should support float data type", func() {
				values := []float32{1, -2, 0, 1.123, -1.31}
				toFloat32 := jsonNumberTo(func(v float64) interface{} {
					return float32(v)
				})
				toString := jsonNumberTo(func(v float64) interface{} {
					return fmt.Sprintf("%g", v)
				})

				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "float", "Float32!", value, "%f", toFloat32, nil)
				}

				stringValues := []string{"1", "0", "-1", "123.46"}
				for _, value := range stringValues {
					datatypes.MutateAndQueryScalar(routes, "float", "Float32!", value, "%s", toString, nil)
				}
			})

			It("Should support double data type", func() {
				toString := jsonNumberTo(func(v float64) interface{} {
					return fmt.Sprintf("%g", v)
				})
				values := []float64{1, -2, 0, 1.123, -1.31}

				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "double", "Float!", value, "%f", nil, nil)
				}

				stringValues := []string{"1", "0", "-1", "123.46"}
				for _, value := range stringValues {
					datatypes.MutateAndQueryScalar(routes, "double", "Float!", value, "%s", toString, nil)
				}
			})

			It("Should support bigint data type", func() {
				values := []int64{1, -2, 0, math.MaxInt64, math.MinInt64}
				toBigInt := jsonStringTo(func(v string) interface{} {
					i, _ := strconv.ParseInt(v, 10, 64)
					return i
				})
				toJson := func(v interface{}) interface{} {
					switch v := v.(type) {
					case int64:
						return fmt.Sprintf("%d", v)
					}
					panic("unexpected type")
				}
				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "bigint", "BigInt!", value, `"%d"`, toBigInt, toJson)
				}
			})

			It("Should support varint data type", func() {
				values := []*big.Int{
					big.NewInt(0), big.NewInt(-1), big.NewInt(0).Mul(big.NewInt(math.MaxInt64), big.NewInt(123)),
				}
				toInt := jsonStringTo(func(v string) interface{} {
					i := new(big.Int)
					i.SetString(v, 10)
					return i
				})
				toJson := func(v interface{}) interface{} {
					switch v := v.(type) {
					case *big.Int:
						return v.Text(10)
					}
					panic("unexpected type")
				}
				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "varint", "Varint!", value, `"%s"`, toInt, toJson)
				}
			})

			It("Should support decimal data type", func() {
				values := []*inf.Dec{inf.NewDec(123, 2), inf.NewDec(0, 0), inf.NewDec(-1, 0)}
				toDec := jsonStringTo(func(v string) interface{} {
					i := new(inf.Dec)
					i.SetString(v)
					return i
				})
				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "decimal", "Decimal!", value, `"%s"`, toDec, nil)
				}
			})

			It("Should support time data type", func() {
				values := []string{"00:00:01.000000001", "14:29:31.800600000", "08:00:00", "21:59:32.800000000"}
				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "time", "Time!", value, `"%s"`, nil, nil)
				}
			})

			It("Should timestamp data type", func() {
				values := []string{"1983-02-23T00:00:50Z", "2010-04-29T23:20:21.52Z"}
				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "timestamp", "Timestamp!", value, `"%s"`, nil, nil)
				}
			})

			It("Should uuid data type", func() {
				values := []string{schemas.NewUuid(), schemas.NewUuid(), schemas.NewUuid()}
				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "uuid", "Uuid!", value, `"%s"`, nil, nil)
				}
			})

			It("Should timeuuid data type", func() {
				values := []string{
					gocql.TimeUUID().String(),
					gocql.UUIDFromTime(time.Date(2005, 8, 5, 0, 0, 0, 0, time.UTC)).String(),
				}
				for _, value := range values {
					datatypes.MutateAndQueryScalar(routes, "timeuuid", "TimeUuid!", value, `"%s"`, nil, nil)
				}
			})

			Context("With invalid values", func() {
				items := [][]string{
					{"float", "abc", `"abc"`},
					{"double", "abc", `"abc"`},
					{"boolean", "abc", `"abc"`, "1", "0"},
					{"tinyint", "abc", `"abc"`},
					{"int", "abc", `"abc"`},
					{"bigint", "123", `"abc"`},
					{"varint", "123", `"abc"`},
					{"decimal", "123", `"abc"`},
					{"timeuuid", `"1234-"`, "123"},
					{"uuid", `"1234-"`, "123"},
					{"inet", `"a.b.c.d"`, "123"},
					{"blob", `"ZZZ!"`},
					{"timestamp", `"ZZZ!"`, "123"},
					{"time", `"ZZZ!"`, "123"},
				}

				for _, itemEach := range items {
					// Capture item
					item := itemEach
					It("Should return an error for "+item[0], func() {
						for i := 1; i < len(item); i++ {
							datatypes.InsertScalarErrors(routes, item[0], item[i])
						}
					})
				}
			})

			Context("With null values", func() {
				for _, itemEach := range datatypes.ScalarJsonValues() {
					// Capture item
					item := itemEach
					datatype := item[0].(string)
					It("Should set the tombstones for values of type "+datatype, func() {
						datatypes.InsertAndUpdateNulls(routes, datatype, item[1])
					})
				}
			})

			It("should support list and sets", func() {
				items := [][]interface{}{
					{"listText", `["a", "b"]`, []interface{}{"a", "b"}},
					{"listFloat", `[1.25, 1]`, []interface{}{1.25, float64(1)}},
					{"setUuid", `["85414228-7a6d-4992-915d-0171f18de601"]`,
						[]interface{}{"85414228-7a6d-4992-915d-0171f18de601"}},
					{"setInt", `[2, 1]`, []interface{}{float64(1), float64(2)}},
				}

				for _, item := range items {
					datatypes.MutateAndQueryCollection(
						routes,
						item[0].(string),
						item[1].(string),
						item[2].([]interface{}),
						false)
				}
			})

			It("should support maps", func() {
				datatypes.MutateAndQueryCollection(
					routes,
					"mapBigintBlob",
					`[{key: "123", value: "VGhl"}, {key: "4", value: "asfR"}]`,
					[]interface{}{
						map[string]interface{}{"key": "123", "value": "VGhl"},
						map[string]interface{}{"key": "4", "value": "asfR"},
					},
					true)
			})

			It("Should support static columns", func() {
				datatypes.MutateAndQueryStatic(routes)
			})
		})

		Context("With empty keyspace", func() {
			const keyspace = "ks_empty"
			config := NewEndpointConfigWithLogger(TestLogger(), host)

			BeforeEach(func() {
				query := fmt.Sprintf(
					"CREATE KEYSPACE IF NOT EXISTS %s "+
						"WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
					keyspace)
				err := session.Query(query).Exec()
				PanicIfError(err)
			})

			It("Should build an empty schema", func() {
				// getRoutes() validates that there wasn't an error
				getRoutes(config, keyspace)
			})
		})
	})

	Describe("RoutesGraphQL()", func() {
		var routes []types.Route
		config := NewEndpointConfigWithLogger(TestLogger(), host)

		BeforeEach(func() {
			var err error
			endpoint := config.newEndpointWithDb(db.NewDbWithConnectedInstance(GetSession()))
			routes, err = endpoint.RoutesGraphQL("/graphql_root")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Should provide a url path per keyspace", func() {
			buffer := schemas.ExecutePost(routes, "/graphql_root/killrvideo", `{ videos { values { name }} }`)
			schemas.DecodeData(buffer, "videos")

			buffer = schemas.ExecutePost(routes, "/graphql_root/quirky?z=1&", `{ validSample { values { id }} }`)
			schemas.DecodeData(buffer, "validSample")

			buffer = schemas.ExecutePost(routes, "/graphql_root/datatypes/?", `{ sampleTable { values { id }} }`)
			schemas.DecodeData(buffer, "sampleTable")
		})

		It("Should return not found when keyspace is not found or invalid", func() {
			targets := []string{
				"/graphql_root/zzz", "/graphql_root/killrvideo/malformed", "/graphql_root/killrvideo%20",
			}

			for _, target := range targets {
				b, err := json.Marshal(graphql.RequestBody{Query: `{__schema{queryType{name}}}`})
				Expect(err).ToNot(HaveOccurred())
				targetUrl := fmt.Sprintf("http://%s", path.Join(host, target))
				r := httptest.NewRequest(http.MethodPost, targetUrl, bytes.NewReader(b))
				w := httptest.NewRecorder()
				routes[postIndex].Handler.ServeHTTP(w, r)
				Expect(w.Code).To(Equal(http.StatusNotFound))
			}
		})
	})
	Describe("RoutesSchemaManagement()", func() {
		cfg := NewEndpointConfigWithLogger(TestLogger(), host)
		Context("With keyspace schema mutations", func() {
			It("Should create keyspace", func() {
				routes := getSchemaRoutes(cfg)
				ksName := randomName()
				ddl.CreateKeyspace(routes, ksName)
				response := ddl.WaitUntilExists(func() schemas.ResponseBody {
					return ddl.Keyspace(routes, ksName)
				})
				Expect(response).To(Equal(schemas.NewResponseBody("keyspace", map[string]interface{}{
					"name": ksName,
					"dcs":  ddl.DCsResult,
				})))
			})
			It("Should drop keyspace", func() {
				routes := getSchemaRoutes(cfg)
				ksName := randomName()
				ddl.CreateKeyspace(routes, ksName)
				ddl.WaitUntilExists(func() schemas.ResponseBody {
					return ddl.Keyspace(routes, ksName)
				})
				ddl.DropKeyspace(routes, ksName)
				ddl.WaitUntilGone(func() schemas.ResponseBody {
					return ddl.Keyspace(routes, ksName)
				})
				response := ddl.Keyspace(routes, ksName)
				Expect(response.Errors).To(HaveLen(1))
				Expect(response.Errors[0].Message).To(ContainSubstring("does not exist"))
			})
			It("Should create keyspace if not exists", func() {
				routes := getSchemaRoutes(cfg)
				ksName := randomName()
				ddl.CreateKeyspace(routes, ksName)
				ddl.WaitUntilExists(func() schemas.ResponseBody {
					return ddl.Keyspace(routes, ksName)
				})
				response := ddl.CreateKeyspaceIfNotExists(routes, ksName, false)
				Expect(response.Errors).To(HaveLen(1))
				Expect(response.Errors[0].Message).To(ContainSubstring("Cannot add existing keyspace"))
				response = ddl.CreateKeyspaceIfNotExists(routes, ksName, true)
				Expect(response.Errors).To(HaveLen(0))
			})
		})
		Context("With table schema mutations", func() {
			It("Should create table", func() {
				routes := getSchemaRoutes(cfg)
				ksName := randomName()
				tableName := "table1"
				ddl.CreateKeyspace(routes, ksName)
				ddl.CreateTable(routes, ksName, tableName, ddl.ColumnTypes)
				response := ddl.WaitUntilExists(func() schemas.ResponseBody {
					return ddl.Table(routes, ksName, tableName)
				})
				ddl.SortColumns(response)
				expected := schemas.NewResponseBody("keyspace", map[string]interface{}{
					"table": map[string]interface{}{
						"name":    tableName,
						"columns": ddl.BuildColumnResult("value", ddl.ColumnTypesResult),
					},
				})
				Expect(response).To(Equal(expected))
			})
			It("Should create table if not exists", func() {
				routes := getSchemaRoutes(cfg)
				ksName := randomName()
				tableName := "table1"
				ddl.CreateKeyspace(routes, ksName)
				ddl.CreateTable(routes, ksName, tableName, ddl.ColumnTypes)
				ddl.WaitUntilExists(func() schemas.ResponseBody {
					return ddl.Table(routes, ksName, tableName)
				})
				response := ddl.CreateTableIfNotExists(routes, ksName, tableName, ddl.ColumnTypes, false)
				Expect(response.Errors).To(HaveLen(1))
				Expect(response.Errors[0].Message).To(ContainSubstring("Cannot add already existing table"))
				response = ddl.CreateTableIfNotExists(routes, ksName, tableName, ddl.ColumnTypes, true)
				Expect(response.Errors).To(HaveLen(0))
			})
			It("Should create counter table", func() {
				routes := getSchemaRoutes(cfg)
				ksName := randomName()
				tableName := "table1"
				ddl.CreateKeyspace(routes, ksName)
				ddl.CreateTable(routes, ksName, tableName, []string{"{ basic: COUNTER }"})
				response := ddl.WaitUntilExists(func() schemas.ResponseBody {
					return ddl.Table(routes, ksName, tableName)
				})
				ddl.SortColumns(response)
				expected := schemas.NewResponseBody("keyspace", map[string]interface{}{
					"table": map[string]interface{}{
						"name": tableName,
						"columns": ddl.BuildColumnResult("value", []map[string]interface{}{
							{"basic": "COUNTER"},
						}),
					},
				})
				Expect(response).To(Equal(expected))
			})
			It("Should alter table add column", func() {
				routes := getSchemaRoutes(cfg)
				ksName := randomName()
				tableName := "table1"
				ddl.CreateKeyspace(routes, ksName)
				ddl.CreateTable(routes, ksName, tableName, []string{"{ basic: TEXT }"})
				ddl.AlterTableAdd(routes, ksName, tableName, ddl.ColumnTypes)
				response := ddl.WaitUntilColumnExists("addedValue01", func() schemas.ResponseBody {
					return ddl.Table(routes, ksName, tableName)
				})
				ddl.SortColumns(response)
				expected := schemas.NewResponseBody("keyspace", map[string]interface{}{
					"table": map[string]interface{}{
						"name": tableName,
						"columns": ddl.BuildColumnResult("addedValue", ddl.ColumnTypesResult,
							ddl.TextColumn("value01")),
					},
				})
				Expect(response).To(Equal(expected))
			})
			It("Should alter table drop column", func() {
				routes := getSchemaRoutes(cfg)
				ksName := randomName()
				tableName := "table1"
				ddl.CreateKeyspace(routes, ksName)
				ddl.CreateTable(routes, ksName, tableName, []string{"{ basic: TEXT }"})
				ddl.WaitUntilExists(func() schemas.ResponseBody {
					return ddl.Table(routes, ksName, tableName)
				})
				ddl.AlterTableDrop(routes, ksName, tableName, []string{"value01"})
				response := ddl.WaitUntilColumnIsGone("value01", func() schemas.ResponseBody {
					return ddl.Table(routes, ksName, tableName)
				})
				ddl.SortColumns(response)
				expected := schemas.NewResponseBody("keyspace", map[string]interface{}{
					"table": map[string]interface{}{
						"name":    tableName,
						"columns": ddl.BuildColumnResult("", nil),
					},
				})
				Expect(response).To(Equal(expected))
			})
			It("Should drop table", func() {
				routes := getSchemaRoutes(cfg)
				ksName := randomName()
				tableName := "table1"
				ddl.CreateKeyspace(routes, ksName)
				ddl.CreateTable(routes, ksName, tableName, []string{"{ basic: TEXT }"})
				ddl.WaitUntilExists(func() schemas.ResponseBody {
					return ddl.Table(routes, ksName, tableName)
				})
				ddl.DropTable(routes, ksName, tableName)
				ddl.WaitUntilGone(func() schemas.ResponseBody {
					return ddl.Table(routes, ksName, tableName)
				})
				response := ddl.Table(routes, ksName, tableName)
				Expect(response.Errors).To(HaveLen(1))
				Expect(response.Errors[0].Message).To(ContainSubstring("table does not exist"))
			})
			It("Should only be able to modify tables inside single keyspace", func() {
				ksName := randomName()
				otherKsName := randomName()
				tableName := "table1"
				{ // Create keyspace
					routes := getSchemaRoutes(cfg)
					ddl.CreateKeyspace(routes, ksName)
					ddl.CreateKeyspace(routes, otherKsName)
				}
				routes := getSchemaRoutesKeyspace(cfg, ksName)

				// Valid cases
				ddl.CreateTable(routes, ksName, tableName, []string{"{ basic: TEXT }"})
				ddl.WaitUntilExists(func() schemas.ResponseBody {
					return ddl.Table(routes, ksName, tableName)
				})
				ddl.AlterTableAdd(routes, ksName, tableName, ddl.ColumnTypes)
				ddl.WaitUntilColumnExists("addedValue01", func() schemas.ResponseBody {
					return ddl.Table(routes, ksName, tableName)
				})
				ddl.AlterTableDrop(routes, ksName, tableName, []string{"addedValue01"})
				ddl.WaitUntilColumnIsGone("addedValue01", func() schemas.ResponseBody {
					return ddl.Table(routes, ksName, tableName)
				})
				ddl.DropTable(routes, ksName, tableName)
				ddl.WaitUntilGone(func() schemas.ResponseBody {
					return ddl.Table(routes, ksName, tableName)
				})

				// Invalid cases
				ddl.ExpectInvalidKeyspace(routes, otherKsName, tableName)
				keyspaces := ddl.Keyspaces(routes)
				Expect(keyspaces.Data["keyspaces"]).To(HaveLen(1))
			})
			It("Should not be able to modify tables inside excluded keyspace", func() {
				ksName := randomName()
				tableName := "table1"

				{ // Setup and valid case
					routes := getSchemaRoutes(cfg)
					ddl.CreateKeyspace(routes, ksName)
					ddl.CreateTable(routes, ksName, tableName, []string{"{ basic: TEXT }"})
				}

				{ // Invalid cases
					cfg.WithExcludedKeyspaces([]string{ksName})
					routes := getSchemaRoutes(cfg)
					ddl.ExpectInvalidKeyspace(routes, ksName, tableName)
				}
			})
		})
	})
})

func getRoutes(config *DataEndpointConfig, keyspace string) []types.Route {
	var endpoint = config.newEndpointWithDb(db.NewDbWithConnectedInstance(GetSession()))
	routes, err := endpoint.RoutesKeyspaceGraphQL("/graphql", keyspace)
	Expect(err).ToNot(HaveOccurred())
	return routes
}

func getSchemaRoutes(cfg *DataEndpointConfig) []types.Route {
	var endpoint = cfg.newEndpointWithDb(db.NewDbWithConnectedInstance(GetSession()))
	routes, err := endpoint.RoutesSchemaManagementGraphQL("/graphql-schema", c.AllSchemaOperations)
	Expect(err).ToNot(HaveOccurred())
	return routes
}

func getSchemaRoutesKeyspace(cfg *DataEndpointConfig, singleKeyspace string) []types.Route {
	var endpoint = cfg.newEndpointWithDb(db.NewDbWithConnectedInstance(GetSession()))
	routes, err := endpoint.RoutesSchemaManagementKeyspaceGraphQL("/graphql-schema", singleKeyspace, c.AllSchemaOperations)
	Expect(err).ToNot(HaveOccurred())
	return routes
}

func jsonStringTo(f func(string) interface{}) func(interface{}) interface{} {
	return func(value interface{}) interface{} {
		switch value := value.(type) {
		case string:
			return f(value)
		}
		panic("unexpected type")
	}
}

func jsonNumberTo(f func(float64) interface{}) func(interface{}) interface{} {
	return func(value interface{}) interface{} {
		switch value := value.(type) {
		case float64:
			return f(value)
		}
		panic("unexpected type")
	}
}

func randomName() string {
	return strings.Replace(gocql.TimeUUID().String(), "-", "", -1)
}
