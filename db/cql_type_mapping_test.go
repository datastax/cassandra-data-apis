package db

import (
	"github.com/stretchr/testify/assert"
)

func (suite *IntegrationTestSuite) TestNumericCqlTypeMapping() {
	queries := []string{
		"CREATE TABLE ks1.tbl_numerics (id int PRIMARY KEY, bigint_value bigint)",
		"INSERT INTO ks1.tbl_numerics (id, bigint_value) VALUES (1, 1)",
	}

	for _, query := range queries {
		err := suite.db.session.Execute(query, nil)
		assert.Nil(suite.T(), err)
	}

	rs, err := suite.db.session.ExecuteIter("SELECT * FROM ks1.tbl_numerics WHERE id = ?", nil, 1)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), rs)
}
