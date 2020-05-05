// +build integration

package endpoint

import (
	"fmt"
	"github.com/datastax/cassandra-data-apis/config"
	"github.com/datastax/cassandra-data-apis/db"
	. "github.com/datastax/cassandra-data-apis/internal/testutil"
	"github.com/datastax/cassandra-data-apis/internal/testutil/rest"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas"
	"github.com/datastax/cassandra-data-apis/internal/testutil/schemas/datatypes"
	e "github.com/datastax/cassandra-data-apis/rest/endpoint/v1"
	"github.com/datastax/cassandra-data-apis/rest/models"
	"github.com/datastax/cassandra-data-apis/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	"net/http"
)

var _ = Describe("DataEndpoint", func() {
	EnsureCcmCluster(func() {
		CreateSchema("killrvideo")
		CreateSchema("quirky")
		CreateSchema("datatypes")
	})

	Describe("RoutesRest()", func() {
		cfg := NewEndpointConfigWithLogger(TestLogger(), host)
		var routes []types.Route
		var dbClient *db.Db

		BeforeEach(func() {
			routes, dbClient = getRestRoutes(cfg, "")
		})

		Describe("GET /keyspaces", func() {

			It("Should list the keyspaces", func() {
				var response []string
				rest.ExecuteGet(routes, e.KeyspacesPathFormat, &response)
				Expect(len(response)).To(BeNumerically(">", 0))
				Expect(response).To(ContainElements("killrvideo", "quirky", "datatypes"))
			})

			It("Should not return excluded keyspaces", func() {
				localCfg := NewEndpointConfigWithLogger(TestLogger(), host)
				localCfg.ksExcluded = []string{"killrvideo", "quirky"}
				routes, dbClient = getRestRoutes(localCfg, "")
				var response []string
				rest.ExecuteGet(routes, e.KeyspacesPathFormat, &response)
				Expect(len(response)).To(BeNumerically(">", 0))
				Expect(response).To(ContainElements("datatypes"))
				Expect(response).NotTo(ContainElements(localCfg.ksExcluded))
			})

			It("Should return a single keyspace when configured", func() {
				singleKs := "killrvideo"
				routes, dbClient = getRestRoutes(cfg, singleKs)
				var response []string
				rest.ExecuteGet(routes, e.KeyspacesPathFormat, &response)
				Expect(response).To(Equal([]string{singleKs}))
			})
		})

		Describe("GET /keyspaces/{keyspaceName}/tables", func() {
			It("Should return 404 when keyspace is not found", func() {
				var response models.ModelError
				code := rest.ExecuteGet(routes, e.TablesPathFormat, &response, "ks_not_found")
				Expect(code).To(Equal(http.StatusNotFound))
				Expect(response.Description).To(Equal("Keyspace 'ks_not_found' not found"))
			})

			It("Should list the tables", func() {
				var response []string
				rest.ExecuteGet(routes, e.TablesPathFormat, &response, "killrvideo")
				Expect(response).To(ContainElements("user_videos", "videos", "video_ratings_by_user"))
			})
		})

		Describe("POST /keyspaces/{keyspaceName}/tables/{tableName}/rows", func() {
			pathFormat := e.RowsPathFormat

			It("Should insert a basic row", func() {
				id := schemas.NewUuid()
				body := fmt.Sprintf(`{ "columns": [
				  { "name": "videoid", "value": "%s"},
				  { "name": "userid", "value": "%s"},
				  { "name": "name", "value": "%s"},
				  { "name": "location_type", "value": %d}
				]}`, id, id, "video "+id, 123)

				var response models.RowsResponse
				code := rest.ExecutePost(routes, pathFormat, body, &response, "killrvideo", "videos")
				Expect(code).To(Equal(http.StatusCreated))
				Expect(response.Success).To(BeTrue())

				rs, err := dbClient.Execute("SELECT * FROM killrvideo.videos WHERE videoid = ?", nil, id)
				Expect(err).NotTo(HaveOccurred())
				Expect(rs.Values()).To(HaveLen(1))
				Expect(rs.Values()[0]).To(MatchKeys(IgnoreExtras, Keys{
					"name":          PointTo(Equal("video " + id)),
					"userid":        PointTo(Equal(id)),
					"location_type": PointTo(Equal(123)),
				}))
			})

			It("Should return 404 when keyspace is not found", func() {
				body := `{ "columns": [{ "name": "a", "value": 1}]}`
				code := rest.ExecutePost(routes, pathFormat, body, nil, "ks_not_found", "videos")
				Expect(code).To(Equal(http.StatusNotFound))
			})

			It("Should return 404 when table is not found", func() {
				body := `{ "columns": [{ "name": "a", "value": 1}]}`
				code := rest.ExecutePost(routes, pathFormat, body, nil, "killrvideo", "tbl_not_found")
				Expect(code).To(Equal(http.StatusNotFound))
			})

			It("Should return 400 when columns do not match", func() {
				body := `{ "columns": [{ "name": "a", "value": 1}]}`
				code := rest.ExecutePost(routes, pathFormat, body, nil, "killrvideo", "videos")
				Expect(code).To(Equal(http.StatusBadRequest))
			})

			It("Should support inserting null values", func() {
				id := schemas.NewUuid()

				// Insert some value
				insertIntoVideos(dbClient, id, "sample video")

				// Create the tombstone to delete the value
				body := fmt.Sprintf(`{ "columns": [
				  { "name": "videoid", "value": "%s"},
				  { "name": "userid", "value": "%s"},
				  { "name": "name", "value": null },
				  { "name": "location_type", "value": 100}
				]}`, id, id)

				var response models.RowsResponse
				code := rest.ExecutePost(routes, pathFormat, body, &response, "killrvideo", "videos")
				Expect(code).To(Equal(http.StatusCreated))
				Expect(response.Success).To(BeTrue())

				rs, err := dbClient.Execute("SELECT * FROM killrvideo.videos WHERE videoid = ?", nil, id)
				Expect(err).NotTo(HaveOccurred())
				row := rs.Values()[0]
				Expect(row["name"]).To(BeNil())
			})

			It("Should return 400 when columns are empty", func() {
				// Create the tombstone to delete the value
				code := rest.ExecutePost(routes, pathFormat, `{ "columns": []}`, nil, "killrvideo", "videos")
				Expect(code).To(Equal(http.StatusBadRequest))
			})

			It("Should return 500 when primary key is not defined", func() {
				body := `{ "columns": [
				  { "name": "name", "value": "sample"},
				  { "name": "location_type", "value": 1}
				]}`
				code := rest.ExecutePost(routes, pathFormat, body, nil, "killrvideo", "videos")
				// Once https://github.com/datastax/cassandra-data-apis/issues/129 is fixed
				// This should be a 400 BAD REQUEST
				Expect(code).To(Equal(http.StatusInternalServerError))
			})

			for _, itemEach := range datatypes.ScalarJsonValues() {
				// Capture item
				item := itemEach
				datatype := item[0].(string)

				It(fmt.Sprintf("Should support inserting %s data type values", datatype), func() {
					id := schemas.NewUuid()
					for i := 1; i < len(item); i++ {
						jsonValue := item[i]
						valueStr := fmt.Sprintf("%v", jsonValue)
						if _, ok := jsonValue.(string); ok {
							valueStr = fmt.Sprintf(`"%s"`, jsonValue)
						}

						body := fmt.Sprintf(`{ "columns": [
						  { "name": "id", "value": "%s"},
						  { "name": "%s_col", "value": %s}
						]}`, id, datatype, valueStr)

						var response models.RowsResponse
						code := rest.ExecutePost(routes, pathFormat, body, &response, "datatypes", "scalars")
						Expect(code).To(Equal(http.StatusCreated))
						Expect(response.Success).To(BeTrue())

						// Obtain the value using a GET request
						Expect(rest.ExecuteGetDataTypeJsonValue(routes, datatype, id)).To(Equal(jsonValue))
					}
				})
			}
		})

		Describe("PUT /keyspaces/{keyspaceName}/tables/{tableName}/rows/{rowIdentifier}", func() {
			pathFormat := e.RowSinglePathFormat

			It("Should upsert a row", func() {
				id := schemas.NewUuid()
				body := fmt.Sprintf(`{ "changeset": [
				  { "column": "userid", "value": "%s"},
				  { "column": "name", "value": "%s"},
				  { "column": "location_type", "value": %d}
				]}`, id, "video "+id, 456)

				var response models.RowsResponse
				code := rest.ExecutePut(routes, pathFormat, body, &response, "killrvideo", "videos", id)
				Expect(code).To(Equal(http.StatusOK))
				Expect(response.Success).To(BeTrue())

				rs, err := dbClient.Execute("SELECT * FROM killrvideo.videos WHERE videoid = ?", nil, id)
				Expect(err).NotTo(HaveOccurred())
				Expect(rs.Values()).To(HaveLen(1))
				Expect(rs.Values()[0]).To(MatchKeys(IgnoreExtras, Keys{
					"name":          PointTo(Equal("video " + id)),
					"userid":        PointTo(Equal(id)),
					"location_type": PointTo(Equal(456)),
				}))
			})

			It("Should return 404 when keyspace is not found", func() {
				id := schemas.NewUuid()
				body := `{ "changeset": [{ "column": "name", "value": "sample"}]}`
				code := rest.ExecutePut(routes, pathFormat, body, nil, "ks_not_found", "videos", id)
				Expect(code).To(Equal(http.StatusNotFound))
			})

			It("Should return 404 when table is not found", func() {
				id := schemas.NewUuid()
				body := `{ "changeset": [{ "column": "name", "value": "sample"}]}`
				code := rest.ExecutePut(routes, pathFormat, body, nil, "killrvideo", "table_not_found", id)
				Expect(code).To(Equal(http.StatusNotFound))
			})

			It("Should return 400 when columns do not match", func() {
				id := schemas.NewUuid()
				body := `{ "changeset": [{ "column": "col_not_found", "value": "sample"}]}`
				code := rest.ExecutePut(routes, pathFormat, body, nil, "killrvideo", "videos", id)
				Expect(code).To(Equal(http.StatusBadRequest))
			})

			It("Should support updating null values", func() {
				id := schemas.NewUuid()

				// Insert some value
				insertIntoVideos(dbClient, id, "sample video to update")

				// Create the tombstone to delete the value
				body := `{ "changeset": [
				  { "column": "name", "value": null },
				  { "column": "location_type", "value": 200}
				]}`

				var response models.RowsResponse
				code := rest.ExecutePut(routes, pathFormat, body, &response, "killrvideo", "videos", id)
				Expect(code).To(Equal(http.StatusOK))
				Expect(response.Success).To(BeTrue())

				rs, err := dbClient.Execute("SELECT * FROM killrvideo.videos WHERE videoid = ?", nil, id)
				Expect(err).NotTo(HaveOccurred())
				Expect(rs.Values()[0]).To(MatchKeys(IgnoreExtras, Keys{
					"name":          BeNil(),
					"location_type": PointTo(Equal(200)),
				}))
			})

			for _, itemEach := range datatypes.ScalarJsonValues() {
				// Capture item
				item := itemEach
				datatype := item[0].(string)

				It(fmt.Sprintf("Should support upserting %s data type values", datatype), func() {
					id := schemas.NewUuid()
					for i := 1; i < len(item); i++ {
						jsonValue := item[i]
						valueStr := fmt.Sprintf("%v", jsonValue)
						if _, ok := jsonValue.(string); ok {
							valueStr = fmt.Sprintf(`"%s"`, jsonValue)
						}

						var response models.RowsResponse
						body := fmt.Sprintf(`{"changeset": [{ "column": "%s_col", "value": %s}]}`, datatype, valueStr)
						code := rest.ExecutePut(routes, pathFormat, body, &response, "datatypes", "scalars", id)
						Expect(code).To(Equal(http.StatusOK))
						Expect(response.Success).To(BeTrue())

						// Obtain the value using a GET request
						Expect(rest.ExecuteGetDataTypeJsonValue(routes, datatype, id)).To(Equal(jsonValue))
					}
				})
			}
		})

		Describe("DELETE /keyspaces/{keyspaceName}/tables/{tableName}/rows/{rowIdentifier}", func() {
			pathFormat := e.RowSinglePathFormat

			It("Should delete a row", func() {
				id := schemas.NewUuid()
				// Insert a video
				insertIntoVideos(dbClient, id, "sample video")

				// DELETE it
				code := rest.ExecuteDelete(routes, pathFormat, "killrvideo", "videos", id)
				Expect(code).To(Equal(http.StatusNoContent))

				rs, err := dbClient.Execute("SELECT * FROM killrvideo.videos WHERE videoid = ?", nil, id)
				Expect(err).NotTo(HaveOccurred())
				Expect(rs.Values()).To(HaveLen(0))
			})

			It("Should return 404 when keyspace is not found", func() {
				code := rest.ExecuteDelete(routes, pathFormat, "keyspace_not_found", "videos", "abc")
				Expect(code).To(Equal(http.StatusNotFound))
			})

			It("Should return 404 when table is not found", func() {
				code := rest.ExecuteDelete(routes, pathFormat, "killrvideos", "table_not_found", "abc")
				Expect(code).To(Equal(http.StatusNotFound))
			})
		})
	})
})

func insertIntoVideos(dbClient *db.Db, id string, name string) {
	insertQuery := "INSERT INTO killrvideo.videos (videoid, userid, name) VALUES (?, ?, ?)"
	_, err := dbClient.Execute(insertQuery, nil, id, id, name)
	Expect(err).NotTo(HaveOccurred())
}

func getRestRoutes(cfg *DataEndpointConfig, singleKs string) ([]types.Route, *db.Db) {
	dbClient := db.NewDbWithConnectedInstance(GetSession())
	endpoint := cfg.newEndpointWithDb(dbClient)
	return endpoint.RoutesRest(rest.Prefix, config.AllSchemaOperations, singleKs), dbClient
}
