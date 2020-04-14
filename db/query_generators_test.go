package db

import (
	"github.com/gocql/gocql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/riptano/data-endpoints/types"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("db", func() {
	Describe("Delete", func() {
		items := []struct {
			description string
			columnNames []string
			queryParams []interface{}
			query       string
			ifExists    bool
			ifCondition []types.ConditionItem
		}{
			{
				"a single column",
				[]string{"a"}, []interface{}{"b"}, "DELETE FROM \"ks1\".\"tbl1\" WHERE a = ?", false, nil},
			{
				"multiple columns",
				[]string{"a", "b"},
				[]interface{}{"A Value", 2}, "DELETE FROM \"ks1\".\"tbl1\" WHERE a = ? AND b = ?", false, nil},
			{
				"IF EXISTS",
				[]string{"a"}, []interface{}{"b"}, "DELETE FROM \"ks1\".\"tbl1\" WHERE a = ? IF EXISTS", true, nil},
			{
				"IF condition",
				[]string{"a"}, []interface{}{"b"},
				"DELETE FROM \"ks1\".\"tbl1\" WHERE a = ? IF c = ?", false, []types.ConditionItem{{"c", "=", "z"}}},
		}

		for _, item := range items {
			It("Should generate DELETE statement with "+item.description, func() {
				sessionMock := SessionMock{}
				sessionMock.On("ExecuteIter", mock.Anything, mock.Anything, mock.Anything).Return(ResultMock{}, nil)
				db := &Db{
					session: &sessionMock,
				}

				_, err := db.Delete(&DeleteInfo{
					Keyspace:    "ks1",
					Table:       "tbl1",
					Columns:     item.columnNames,
					QueryParams: item.queryParams,
					IfExists:    item.ifExists,
					IfCondition: item.ifCondition,
				}, nil)
				Expect(err).NotTo(HaveOccurred())

				expectedQueryParams := make([]interface{}, len(item.queryParams))
				copy(expectedQueryParams, item.queryParams)

				if len(item.ifCondition) > 0 {
					for _, condition := range item.ifCondition {
						expectedQueryParams = append(expectedQueryParams, condition.Value)
					}
				}
				sessionMock.AssertCalled(GinkgoT(), "ExecuteIter", item.query, mock.Anything, expectedQueryParams)
				sessionMock.AssertExpectations(GinkgoT())
			})
		}
	})

	Describe("Update", func() {
		items := []struct {
			description    string
			columnNames    []string
			queryParams    []interface{}
			ifExists       bool
			ifCondition    []types.ConditionItem
			ttl            int
			query          string
			expectedParams []interface{}
		}{
			{
				"multiple set columns",
				[]string{"ck1", "a", "b", "pk2", "pk1"}, []interface{}{1, 2, 3, 4, 5}, false, nil, -1,
				"UPDATE \"ks1\".\"tbl1\" SET a = ?, b = ? WHERE ck1 = ? AND pk2 = ? AND pk1 = ?",
				[]interface{}{2, 3, 1, 4, 5}},
			{
				"ttl and IF EXISTS",
				[]string{"a", "ck1", "pk1", "pk2"}, []interface{}{1, 2, 3, 4}, true, nil, 60,
				"UPDATE \"ks1\".\"tbl1\" USING TTL ? SET a = ? WHERE ck1 = ? AND pk1 = ? AND pk2 = ? IF EXISTS",
				[]interface{}{60, 1, 2, 3, 4}},
			{
				"IF condition",
				[]string{"a", "ck1", "pk1", "pk2"}, []interface{}{1, 2, 3, 4}, false,
				[]types.ConditionItem{{"c", ">", 100}}, -1,
				"UPDATE \"ks1\".\"tbl1\" SET a = ? WHERE ck1 = ? AND pk1 = ? AND pk2 = ? IF c > ?",
				[]interface{}{1, 2, 3, 4, 100}},
		}

		for _, item := range items {
			It("Should generate UPDATE statement with "+item.description, func() {
				table := &gocql.TableMetadata{
					Name:              "tbl1",
					PartitionKey:      []*gocql.ColumnMetadata{{Name: "pk1"}, {Name: "pk2"}},
					ClusteringColumns: []*gocql.ColumnMetadata{{Name: "ck1"}},
				}
				sessionMock := SessionMock{}
				sessionMock.On("ExecuteIter", mock.Anything, mock.Anything, mock.Anything).Return(ResultMock{}, nil)
				db := &Db{
					session: &sessionMock,
				}

				_, err := db.Update(&UpdateInfo{
					Keyspace:    "ks1",
					Table:       table,
					Columns:     item.columnNames,
					QueryParams: item.queryParams,
					IfExists:    item.ifExists,
					IfCondition: item.ifCondition,
					TTL:         item.ttl,
				}, nil)
				Expect(err).NotTo(HaveOccurred())
				sessionMock.AssertCalled(GinkgoT(), "ExecuteIter", item.query, mock.Anything, item.expectedParams)
				sessionMock.AssertExpectations(GinkgoT())
			})
		}
	})

	Describe("Insert", func() {
		items := []struct {
			description string
			columnNames []string
			queryParams []interface{}
			ttl         int
			ifNotExists bool
			query       string
		}{
			{
				"a single column",
				[]string{"a"}, []interface{}{100}, -1, false, "INSERT INTO \"ks1\".\"tbl1\" (a) VALUES (?)"},
			{
				"multiple columns",
				[]string{"a", "b"}, []interface{}{100, 2}, -1, false,
				"INSERT INTO \"ks1\".\"tbl1\" (a, b) VALUES (?, ?)"},
			{
				"IF NOT EXISTS",
				[]string{"a"}, []interface{}{100}, -1, true,
				"INSERT INTO \"ks1\".\"tbl1\" (a) VALUES (?) IF NOT EXISTS"},
			{
				"TTL",
				[]string{"a"}, []interface{}{"z"}, 3600, true,
				"INSERT INTO \"ks1\".\"tbl1\" (a) VALUES (?) IF NOT EXISTS USING TTL ?"},
		}

		for _, item := range items {
			It("Should generate INSERT statement with "+item.description, func() {

				sessionMock := SessionMock{}
				sessionMock.On("ExecuteIter", mock.Anything, mock.Anything, mock.Anything).Return(ResultMock{}, nil)
				db := &Db{
					session: &sessionMock,
				}

				expectedQueryParams := make([]interface{}, len(item.queryParams))
				copy(expectedQueryParams, item.queryParams)

				if item.ttl >= 0 {
					expectedQueryParams = append(expectedQueryParams, item.ttl)
				}

				_, err := db.Insert(&InsertInfo{
					Keyspace:    "ks1",
					Table:       "tbl1",
					Columns:     item.columnNames,
					QueryParams: item.queryParams,
					TTL:         item.ttl,
					IfNotExists: item.ifNotExists,
				}, nil)

				Expect(err).NotTo(HaveOccurred())
				sessionMock.AssertCalled(GinkgoT(), "ExecuteIter", item.query, mock.Anything, expectedQueryParams)
				sessionMock.AssertExpectations(GinkgoT())
			})
		}
	})

	Describe("Select", func() {
		items := []struct {
			description string
			where       []types.ConditionItem
			options     *types.QueryOptions
			orderBy     []ColumnOrder
			query       string
		}{
			{"", []types.ConditionItem{{"a", "=", 1}}, &types.QueryOptions{}, nil,
				"SELECT * FROM \"ks1\".\"tbl1\" WHERE a = ?"},
			{"", []types.ConditionItem{{"a", "=", 1}, {"b", ">", 2}}, &types.QueryOptions{}, nil,
				"SELECT * FROM \"ks1\".\"tbl1\" WHERE a = ? AND b > ?"},
			{"", []types.ConditionItem{{"a", "=", 1}, {"b", ">", 2}, {"b", "<=", 5}}, &types.QueryOptions{}, nil,
				"SELECT * FROM \"ks1\".\"tbl1\" WHERE a = ? AND b > ? AND b <= ?"},
			{"", []types.ConditionItem{{"a", "=", 1}}, &types.QueryOptions{}, []ColumnOrder{{"c", "DESC"}},
				"SELECT * FROM \"ks1\".\"tbl1\" WHERE a = ? ORDER BY c DESC"},
			{"", []types.ConditionItem{{"a", "=", "z"}}, &types.QueryOptions{Limit: 1}, []ColumnOrder{{"c", "ASC"}},
				"SELECT * FROM \"ks1\".\"tbl1\" WHERE a = ? ORDER BY c ASC LIMIT ?"},
		}

		for _, item := range items {
			It("Should generate SELECT statement with "+item.description, func() {
				resultMock := &ResultMock{}
				resultMock.
					On("PageState").Return([]byte{}).
					On("Values").Return([]map[string]interface{}{}, nil)
				sessionMock := SessionMock{}
				sessionMock.On("ExecuteIter", mock.Anything, mock.Anything, mock.Anything).Return(resultMock, nil)
				db := &Db{
					session: &sessionMock,
				}

				queryParams := make([]interface{}, 0)
				for _, v := range item.where {
					queryParams = append(queryParams, v.Value)
				}

				if item.options != nil && item.options.Limit > 0 {
					queryParams = append(queryParams, item.options.Limit)
				}

				_, err := db.Select(&SelectInfo{
					Keyspace: "ks1",
					Table:    "tbl1",
					Where:    item.where,
					Options:  item.options,
					OrderBy:  item.orderBy,
				}, nil)
				Expect(err).NotTo(HaveOccurred())
				sessionMock.AssertCalled(GinkgoT(), "ExecuteIter", item.query, mock.Anything, queryParams)
				sessionMock.AssertExpectations(GinkgoT())
			})
		}
	})
})
