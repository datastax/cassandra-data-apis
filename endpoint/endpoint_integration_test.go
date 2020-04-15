// +build integration

package endpoint

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/datastax/cassandra-data-apis/db"
	"github.com/datastax/cassandra-data-apis/graphql"
	. "github.com/datastax/cassandra-data-apis/internal/testutil"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas/killrvideo"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas/quirky"
	"github.com/gocql/gocql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"path"
	"sort"
	"testing"
	"time"
)

var session *gocql.Session

var _ = Describe("DataEndpoint", func() {
	Describe("RoutesKeyspaceGraphQL()", func() {
		Context("With killrvideo schema", func() {
			config, _ := NewEndpointConfig(host)
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
						"email":       nil,
						"firstname":   nil,
						"lastname":    nil,
						"userid":      nil,
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
				insertQuery := "INSERT INTO killrvideo.tags_by_letter (first_letter, tag) VALUES (?, ?)"
				length := 5
				for i := 1; i <= length; i++ {
					err := session.Query(insertQuery, "a", fmt.Sprintf("a%d", i)).Exec()
					Expect(err).ToNot(HaveOccurred())
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

			It("Should support normal and conditional updates", func() {
				routes := getRoutes(config, keyspace)
				id := schemas.NewUuid()
				firstEmail := "email1@email.com"
				buffer, err := executePost(routes, "/graphql", graphql.RequestBody{
					Query: killrvideo.UpdateUserMutation(id, "John", firstEmail, ""),
				}, nil)
				Expect(err).ToNot(HaveOccurred())
				Expect(schemas.DecodeData(buffer, "updateUsers")["applied"]).To(BeTrue())

				// This should not be applied
				buffer, _ = executePost(routes, "/graphql", graphql.RequestBody{
					Query: killrvideo.UpdateUserMutation(id, "John", "new_email@email.com", "email_old@email.com"),
				}, nil)
				data := schemas.DecodeData(buffer, "updateUsers")
				// Verify that the mutation was not applied on C* side
				Expect(data["applied"]).To(BeFalse())
				Expect(data["value"]).To(Equal(map[string]interface{}{
					"userid":    nil,
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
				Expect(schemas.DecodeData(buffer, "deleteUsers")["applied"]).To(BeTrue())
				iter := session.Query(selectQuery, id1).Iter()
				Expect(iter.NumRows()).To(BeZero())
				Expect(iter.Close()).ToNot(HaveOccurred())

				// Conditional delete
				buffer, err = executePost(routes, "/graphql", graphql.RequestBody{
					Query: killrvideo.DeleteUserMutation(id2, name),
				}, nil)
				Expect(err).ToNot(HaveOccurred())
				data := schemas.DecodeData(buffer, "deleteUsers")
				Expect(data["applied"]).To(BeFalse())
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
				routes := getRoutes(config, keyspace)
				query := `query {
				 insertNotFound {
					values {
					  name
					  description
					}
				 }
				}`
				b, err := json.Marshal(graphql.RequestBody{Query: query})
				Expect(err).ToNot(HaveOccurred())
				r := httptest.NewRequest(http.MethodPost, path.Join(fmt.Sprintf("http://%s", schemas.Host), "/graphql"), bytes.NewReader(b))
				w := httptest.NewRecorder()
				routes[postIndex].Handler.ServeHTTP(w, r)
				// GraphQL spec defines the error as a field and HTTP status code should still be 200
				// http://spec.graphql.org/June2018/#sec-Errors
				Expect(w.Code).To(Equal(http.StatusOK))
				response := schemas.DecodeResponse(w.Body)
				Expect(response.Data).To(HaveLen(0))
				Expect(response.Errors).To(HaveLen(1))
				Expect(response.Errors[0].Message).To(ContainSubstring("Cannot query field"))
			})
		})

		Context("With quirky schema", func() {
			config, _ := NewEndpointConfig(host)
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
		})
	})
})

var _ = BeforeSuite(func() {
	session = SetupIntegrationTestFixture()
	CreateSchema("killrvideo")
	CreateSchema("quirky")
})

var _ = AfterSuite(func() {
	TearDownIntegrationTestFixture()
})

func TestEndpoint(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Endpoint integration test suite")
}

func getRoutes(config *DataEndpointConfig, keyspace string) []graphql.Route {
	var endpoint = config.newEndpointWithDb(db.NewDbWithConnectedInstance(session))
	routes, err := endpoint.RoutesKeyspaceGraphQL("/graphql", keyspace)
	Expect(err).ToNot(HaveOccurred())
	return routes
}
