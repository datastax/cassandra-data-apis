package db

import (
	"github.com/datastax/cassandra-data-apis/internal/testutil"
	"github.com/datastax/cassandra-data-apis/types"
	"github.com/gocql/gocql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"testing"
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
				[]string{"a"}, []interface{}{"b"}, `DELETE FROM "ks1"."tbl1" WHERE "a" = ?`, false, nil},
			{
				"multiple columns",
				[]string{"A", "b"},
				[]interface{}{"A Value", 2}, `DELETE FROM "ks1"."tbl1" WHERE "A" = ? AND "b" = ?`, false, nil},
			{
				"IF EXISTS",
				[]string{"a"}, []interface{}{"b"}, `DELETE FROM "ks1"."tbl1" WHERE "a" = ? IF EXISTS`, true, nil},
			{
				"IF condition",
				[]string{"a"}, []interface{}{"b"},
				`DELETE FROM "ks1"."tbl1" WHERE "a" = ? IF "C" = ?`, false, []types.ConditionItem{{"C", "=", "z"}}},
		}

		for i := 0; i < len(items); i++ {
			// Capture the item in the closure
			item := items[i]

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
				[]string{"CK1", "a", "b", "pk2", "pk1"}, []interface{}{1, 2, 3, 4, 5}, false, nil, -1,
				`UPDATE "ks1"."tbl1" SET "a" = ?, "b" = ? WHERE "CK1" = ? AND "pk2" = ? AND "pk1" = ?`,
				[]interface{}{2, 3, 1, 4, 5}},
			{
				"ttl and IF EXISTS",
				[]string{"a", "CK1", "pk1", "pk2"}, []interface{}{1, 2, 3, 4}, true, nil, 60,
				`UPDATE "ks1"."tbl1" USING TTL ? SET "a" = ? WHERE "CK1" = ? AND "pk1" = ? AND "pk2" = ? IF EXISTS`,
				[]interface{}{60, 1, 2, 3, 4}},
			{
				"IF condition",
				[]string{"a", "CK1", "pk1", "pk2"}, []interface{}{1, 2, 3, 4}, false,
				[]types.ConditionItem{{"c", ">", 100}}, -1,
				`UPDATE "ks1"."tbl1" SET "a" = ? WHERE "CK1" = ? AND "pk1" = ? AND "pk2" = ? IF "c" > ?`,
				[]interface{}{1, 2, 3, 4, 100}},
		}

		for i := 0; i < len(items); i++ {
			// Capture the item in the closure
			item := items[i]

			It("Should generate UPDATE statement with "+item.description, func() {
				table := &gocql.TableMetadata{
					Name: "tbl1",
					Columns: map[string]*gocql.ColumnMetadata{
						"pk1": {Name: "pk1", Kind: gocql.ColumnPartitionKey},
						"pk2": {Name: "pk2", Kind: gocql.ColumnPartitionKey},
						"CK1": {Name: "CK1", Kind: gocql.ColumnClusteringKey},
					},
				}
				table.PartitionKey = createKey(table.Columns, gocql.ColumnPartitionKey)
				table.ClusteringColumns = createKey(table.Columns, gocql.ColumnClusteringKey)
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
				[]string{"a"}, []interface{}{100}, -1, false,
				`INSERT INTO "ks1"."tbl1" ("a") VALUES (?)`},
			{
				"multiple columns",

				[]string{"a", "B"}, []interface{}{100, 2}, -1, false,
				`INSERT INTO "ks1"."tbl1" ("a", "B") VALUES (?, ?)`},
			{
				"IF NOT EXISTS",
				[]string{"a"}, []interface{}{100}, -1, true,
				`INSERT INTO "ks1"."tbl1" ("a") VALUES (?) IF NOT EXISTS`},
			{
				"TTL",
				[]string{"a"}, []interface{}{"z"}, 3600, true,
				`INSERT INTO "ks1"."tbl1" ("a") VALUES (?) IF NOT EXISTS USING TTL ?`},
		}

		for i := 0; i < len(items); i++ {
			// Capture the item in the closure
			item := items[i]
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
			columns     []string
			query       string
		}{
			{"a single condition", []types.ConditionItem{{"a", "=", 1}}, &types.QueryOptions{}, nil, nil,
				`SELECT * FROM "ks1"."tbl1" WHERE "a" = ?`},
			{"condition and one select column", []types.ConditionItem{{"a", "=", 1}}, &types.QueryOptions{}, nil,
				[]string{"col1"},
				`SELECT "col1" FROM "ks1"."tbl1" WHERE "a" = ?`},
			{"condition and select columns", []types.ConditionItem{{"col1", "=", 1}}, &types.QueryOptions{}, nil,
				[]string{"COL2", "col1"},
				`SELECT "COL2", "col1" FROM "ks1"."tbl1" WHERE "col1" = ?`},
			{"no where clause", []types.ConditionItem{}, &types.QueryOptions{}, nil, nil,
				`SELECT * FROM "ks1"."tbl1"`},
			{"no where clause and limit", []types.ConditionItem{}, &types.QueryOptions{Limit: 1}, nil, nil,
				`SELECT * FROM "ks1"."tbl1" LIMIT ?`},
			{"multiple conditions", []types.ConditionItem{{"a", "=", 1}, {"B", ">", 2}}, &types.QueryOptions{}, nil, nil,
				`SELECT * FROM "ks1"."tbl1" WHERE "a" = ? AND "B" > ?`},
			{"relational operators", []types.ConditionItem{{"a", "=", 1}, {"b", ">", 2}, {"b", "<=", 5}},
				&types.QueryOptions{}, nil, nil, `SELECT * FROM "ks1"."tbl1" WHERE "a" = ? AND "b" > ? AND "b" <= ?`},
			{"order clause", []types.ConditionItem{{"a", "=", 1}},
				&types.QueryOptions{}, []ColumnOrder{{"c", "DESC"}}, nil,
				`SELECT * FROM "ks1"."tbl1" WHERE "a" = ? ORDER BY "c" DESC`},
			{"order and limit", []types.ConditionItem{{"ABC", "=", "z"}}, &types.QueryOptions{Limit: 1},
				[]ColumnOrder{{"DEF", "ASC"}}, nil,
				`SELECT * FROM "ks1"."tbl1" WHERE "ABC" = ? ORDER BY "DEF" ASC LIMIT ?`},
		}

		for i := 0; i < len(items); i++ {
			// Capture the item in the closure
			item := items[i]

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
					Columns:  item.columns,
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

func TestTypeMapping(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Db test suite")
}

var _ = BeforeSuite(testutil.BeforeTestSuite)

var _ = AfterSuite(testutil.AfterTestSuite)
