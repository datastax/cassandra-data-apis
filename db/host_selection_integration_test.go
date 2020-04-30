// +build integration simulated

package db

import (
	"github.com/datastax/cassandra-data-apis/internal/testutil"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("NewDb()", func() {
	testutil.EnsureSimulacronCluster()

	It("Should only target local DC", func() {
		db, err := NewDb("", "", testutil.SimulacronStartIp)
		Expect(err).NotTo(HaveOccurred())
		query := "SELECT * FROM ks1.tbl1"
		length := 100
		for i := 0; i < length; i++ {
			_, err := db.session.ExecuteIter(query, nil)
			Expect(err).NotTo(HaveOccurred())
		}
		dc1Logs := testutil.GetQueryLogs(0)
		Expect(dc1Logs.DataCenters).To(HaveLen(1))
		dc1Queries := testutil.CountLogMatches(dc1Logs.DataCenters[0].Nodes, query)

		// All executions to be made on DC1
		Expect(dc1Queries).To(Equal(testutil.QueryMatches{
			Prepare: 3, // One per node
			Execute: length,
		}))

		dc2Logs := testutil.GetQueryLogs(1)
		Expect(dc2Logs.DataCenters).To(HaveLen(1))
		dc2Queries := testutil.CountLogMatches(dc2Logs.DataCenters[0].Nodes, query)

		// No executions on DC2
		Expect(dc2Queries.Execute).To(BeZero())
	})
})
