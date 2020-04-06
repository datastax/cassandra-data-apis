package endpoint

import (
	"encoding/json"
	"github.com/gocql/gocql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/riptano/data-endpoints/db"
	"github.com/riptano/data-endpoints/graphql"
	. "github.com/riptano/data-endpoints/internal/testutil"
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
		var config, _ = NewEndpointConfig(host)
		It("Should handle the killrvideo schema", func() {
			var endpoint = config.newEndpointWithDb(db.NewDbWithConnectedInstance(session))
			routes, err := endpoint.RoutesKeyspaceGraphQL("/graphql", "killrvideo")
			Expect(err).ToNot(HaveOccurred())
			Expect(routes).To(HaveLen(2))

			body := graphql.RequestBody{
				Query: `query {
				  userCredentials(data:{email:"abc@email.com"}) {
					values {
					  email
					  userid
					}
				  }
				}`,
			}

			expected := responseBody{
				Data: map[string]interface{}{
					"userCredentials": map[string]interface{}{
						"values": []interface{}{},
					},
				},
			}

			buffer, err := executePost(routes, "/graphql", body)
			Expect(err).ToNot(HaveOccurred())

			var resp responseBody
			err = json.NewDecoder(buffer).Decode(&resp)
			Expect(err).ToNot(HaveOccurred())
			Expect(resp).To(Equal(expected))
		})
	})
})

var _ = BeforeSuite(func() {
	if !IntegrationTestsEnabled() {
		return
	}

	session = SetupIntegrationTestFixture()
	CreateSchema("killrvideo-schema.cql")
})

var _ = AfterSuite(func() {
	TearDownIntegrationTestFixture()
})

func TestEndpoint(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Endpoint integration test suite")
}
