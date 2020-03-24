package db

import (
	"fmt"
	"github.com/riptano/data-endpoints/internal/testutil"
	"github.com/stretchr/testify/suite"
	"testing"
)

type IntegrationTestSuite struct {
	suite.Suite
}

func (suite *IntegrationTestSuite) SetupSuite() {
	fmt.Println("Running setup suite")
	testutil.SetupIntegrationTestFixture()
}

func (suite *IntegrationTestSuite) TearDownSuite() {
	fmt.Println("Running teardown suite")
	testutil.TearDownIntegrationTestFixture()
}

func TestExampleTestSuite(t *testing.T) {
	if testutil.IntegrationTestsEnabled() {
		suite.Run(t, new(IntegrationTestSuite))
	}
}
