package endpoint

import (
	"fmt"
	"github.com/gocql/gocql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/riptano/data-endpoints/db"
	"github.com/riptano/data-endpoints/graphql"
	. "github.com/riptano/data-endpoints/internal/testutil"
	"github.com/riptano/data-endpoints/internal/testutil/schemas"
	"github.com/riptano/data-endpoints/internal/testutil/schemas/killrvideo"
	"sort"
	"testing"
	"time"
)

var session *gocql.Session

var _ = Describe("DataEndpoint", func() {
	BeforeEach(func() {
		if !IntegrationTestsEnabled() {
			Skip("Integration tests are not enabled")
		}
	})

	Describe("RoutesKeyspaceGraphQL()", func() {
		Context("With killrvideo schema", func() {
			var config, _ = NewEndpointConfig(host)
			It("Should insert and select users", func() {
				routes := getRoutes(config)
				Expect(routes).To(HaveLen(2))

				id := killrvideo.NewUuid()

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

				var endpoint = config.newEndpointWithDb(db.NewDbWithConnectedInstance(session))
				routes, _ := endpoint.RoutesKeyspaceGraphQL("/graphql", "killrvideo")

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
				routes := getRoutes(config)
				id := killrvideo.NewUuid()
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
				routes := getRoutes(config)
				value := map[string]interface{}{
					"userid":      killrvideo.NewUuid(),
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
				routes := getRoutes(config)
				id1 := killrvideo.NewUuid()
				id2 := killrvideo.NewUuid()
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

				routes := getRoutes(config)
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

			It("Should types per table", func() {
				routes := getRoutes(config)
				buffer, err := executePost(routes, "/graphql", graphql.RequestBody{
					Query: `{
					  __schema {
						types {
						  name
						  description
						}
					  }
					}`,
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
		})
	})
})

var _ = BeforeSuite(func() {
	if !IntegrationTestsEnabled() {
		return
	}

	session = SetupIntegrationTestFixture()
	CreateSchema("killrvideo")
})

var _ = AfterSuite(func() {
	TearDownIntegrationTestFixture()
})

func TestEndpoint(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Endpoint integration test suite")
}

func getRoutes(config *DataEndpointConfig) []graphql.Route {
	var endpoint = config.newEndpointWithDb(db.NewDbWithConnectedInstance(session))
	routes, err := endpoint.RoutesKeyspaceGraphQL("/graphql", "killrvideo")
	Expect(err).ToNot(HaveOccurred())
	return routes
}
