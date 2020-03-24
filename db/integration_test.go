package db

import (
	"github.com/riptano/data-endpoints/internal/testutil"
	"github.com/stretchr/testify/suite"
	"testing"
)

type IntegrationTestSuite struct {
	suite.Suite
	db *Db
}

func (suite *IntegrationTestSuite) SetupSuite() {
	session := testutil.SetupIntegrationTestFixture()
	err := session.Query("CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}").Exec()
	if err != nil {
		panic(err)
	}
	suite.db = &Db{session: &GoCqlSession{ref: session}}
}

func (suite *IntegrationTestSuite) TearDownSuite() {
	testutil.TearDownIntegrationTestFixture()
}

func TestDbIntegrationTestSuite(t *testing.T) {
	if testutil.IntegrationTestsEnabled() {
		suite.Run(t, new(IntegrationTestSuite))
	}
}
