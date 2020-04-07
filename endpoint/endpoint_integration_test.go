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
				var endpoint = config.newEndpointWithDb(db.NewDbWithConnectedInstance(session))
				routes, err := endpoint.RoutesKeyspaceGraphQL("/graphql", "killrvideo")
				Expect(err).ToNot(HaveOccurred())
				Expect(routes).To(HaveLen(2))

				id := killrvideo.NewUuid()

				buffer, err := executePost(routes, "/graphql", graphql.RequestBody{
					Query: killrvideo.InsertUserMutation(id, "John", "john@email.com"),
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

				Expect(schemas.DecodeData(buffer, "users")["values"]).To(ConsistOf(values))
			})

			XIt("Should support normal and conditional updates", func() {
				//TODO: Implement
			})

			XIt("Should support conditional inserts", func() {
				//TODO: Implement
			})

			XIt("Should support normal and conditional deletes", func() {
				//TODO: Implement
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

				var endpoint = config.newEndpointWithDb(db.NewDbWithConnectedInstance(session))
				routes, err := endpoint.RoutesKeyspaceGraphQL("/graphql", "killrvideo")
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
				var endpoint = config.newEndpointWithDb(db.NewDbWithConnectedInstance(session))
				routes, _ := endpoint.RoutesKeyspaceGraphQL("/graphql", "killrvideo")

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
				result := schemas.DecodeData(buffer, "__schema")["types"].([]interface{})

				typeNames := make([]string, 0, len(result))
				for _, item := range result {
					mapItem := item.(map[string]interface{})
					typeNames = append(typeNames, mapItem["name"].(string))
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
