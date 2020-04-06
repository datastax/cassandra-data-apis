package endpoint

import (
	"github.com/gocql/gocql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/riptano/data-endpoints/db"
	"github.com/riptano/data-endpoints/graphql"
	. "github.com/riptano/data-endpoints/internal/testutil"
	"github.com/riptano/data-endpoints/internal/testutil/schemas"
	"github.com/riptano/data-endpoints/internal/testutil/schemas/killrvideo"
	"testing"
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
				})
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
				})
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
