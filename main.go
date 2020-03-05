package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/riptano/data-endpoints/db"
	"github.com/riptano/data-endpoints/schema"

	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/iancoleman/strcase"
	"github.com/julienschmidt/httprouter"
)

func executeQuery(query string, schema graphql.Schema) *graphql.Result {
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
	})
	if len(result.Errors) > 0 {
		fmt.Printf("wrong result, unexpected errors: %v", result.Errors)
	}
	return result
}

type requestBody struct {
	Query string `json:"query"`
}

func main() {
	mydb, err := db.NewDb("127.0.0.1")
	if err != nil {
		fmt.Println("Unable to make DB connection")
		return
	}

	keyspaceMeta, err := mydb.Keyspace("store")
	if err != nil {
		fmt.Println("Unable to find keyspace")
		return
	}

	s, err := schema.BuildSchema(keyspaceMeta, func(params graphql.ResolveParams) (interface{}, error) {
		tableMeta := keyspaceMeta.Tables[strcase.ToSnake(params.Info.FieldName)]
		if tableMeta == nil {
			return nil, fmt.Errorf("Unable to find table '%s'", params.Info.FieldName)
		}

		queryParams := make([]interface{}, 0)

		// FIXME: How do we figure out the filter columns from graphql.ResolveParams?
		//        Also, we need to valid and convert complex type here.

		whereClause := ""
		for _, metadata := range tableMeta.PartitionKey {
			if params.Args[metadata.Name] == nil {
				return nil, fmt.Errorf("Query does not contain full primary key")
			}
			queryParams = append(queryParams, params.Args[metadata.Name])
			if len(whereClause) > 0 {
				whereClause += fmt.Sprintf(" AND %s = ?", metadata.Name)
			} else {
				whereClause += fmt.Sprintf(" %s = ?", metadata.Name)
			}
		}

		for _, metadata := range tableMeta.ClusteringColumns {
			if params.Args[metadata.Name] != nil {
				queryParams = append(queryParams, params.Args[metadata.Name])
				if len(whereClause) > 0 {
					whereClause += fmt.Sprintf(" AND %s = ?", metadata.Name)
				} else {
					whereClause += fmt.Sprintf(" %s = ?", metadata.Name)
				}
			}
		}

		query := fmt.Sprintf("SELECT * FROM %s.%s WHERE%s", keyspaceMeta.Name, tableMeta.Name, whereClause)

		iter := mydb.Select(query, gocql.LocalOne, queryParams...)

		results := make([]map[string]interface{}, 0)
		row := map[string]interface{}{}

		for iter.MapScan(row) {
			rowCamel := map[string]interface{}{}
			for k, v := range row {
				rowCamel[strcase.ToLowerCamel(k)] = v
			}
			results = append(results, rowCamel)
			row = map[string]interface{}{}
		}

		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("Error executing query: %v", err)
		}

		return results, nil
	})

	if err != nil {
		fmt.Println("Unable to build schema")
		return
	}

	router := httprouter.New()
	router.GET("/graphql", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		result := executeQuery(r.URL.Query().Get("query"), s)
		json.NewEncoder(w).Encode(result)
	})
	router.POST("/graphql", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		if r.Body == nil {
			http.Error(w, "No request body", 400)
			return
		}

		var body requestBody
		err := json.NewDecoder(r.Body).Decode(&body)
		if err != nil {
			http.Error(w, "Request body is invalid", 400)
			return
		}

		result := executeQuery(body.Query, s)
		json.NewEncoder(w).Encode(result)
	})

	fmt.Println("Now server is running on port 8080")
	http.ListenAndServe(":8080", router)
}
