package graphql

import (
	"encoding/base64"
	"fmt"
	"github.com/datastax/cassandra-data-apis/db"
	"github.com/datastax/cassandra-data-apis/types"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/inf.v0"
	"math/big"
	"reflect"
	"strings"
	"time"
)

type mutationOperation int

const (
	insertOperation mutationOperation = iota
	updateOperation
	deleteOperation
)

func (sg *SchemaGenerator) queryFieldResolver(
	table *gocql.TableMetadata,
	ksSchema *KeyspaceGraphQLSchema,
	isFilter bool,
) graphql.FieldResolveFn {
	return func(params graphql.ResolveParams) (interface{}, error) {
		// GraphQL operation is lower camel
		var value map[string]interface{}
		if isFilter {
			value = params.Args["filter"].(map[string]interface{})
		} else if params.Args["value"] != nil {
			value = params.Args["value"].(map[string]interface{})
		}

		var whereClause []types.ConditionItem

		if !isFilter {
			whereClause = make([]types.ConditionItem, 0, len(value))
			for key, value := range value {
				whereClause = append(whereClause, types.ConditionItem{
					Column:   ksSchema.naming.ToCQLColumn(table.Name, key),
					Operator: "=",
					Value:    adaptParameterValue(value),
				})
			}
		} else {
			whereClause = ksSchema.adaptCondition(table.Name, value)
		}

		var orderBy []interface{}
		var options types.QueryOptions
		if err := mapstructure.Decode(params.Args["options"], &options); err != nil {
			return nil, err
		}

		if params.Args["orderBy"] != nil {
			orderBy = params.Args["orderBy"].([]interface{})
		}

		userOrRole, err := sg.checkUserOrRoleAuth(params)
		if err != nil {
			return nil, err
		}

		pageState, err := base64.StdEncoding.DecodeString(options.PageState)
		if err != nil {
			return nil, err
		}

		result, err := sg.dbClient.Select(
			&db.SelectInfo{
				Keyspace: table.Keyspace,
				Table:    table.Name,
				Where:    whereClause,
				OrderBy:  parseColumnOrder(orderBy),
				Options:  &options,
			},
			db.NewQueryOptions().
				WithUserOrRole(userOrRole).
				WithPageSize(options.PageSize).
				WithPageState(pageState).
				WithConsistency(gocql.Consistency(options.Consistency)))

		if err != nil {
			return nil, err
		}

		return &types.QueryResult{
			PageState: base64.StdEncoding.EncodeToString(result.PageState()),
			Values:    ksSchema.adaptResult(table.Name, result.Values()),
		}, nil
	}
}

func (sg *SchemaGenerator) mutationFieldResolver(
	table *gocql.TableMetadata,
	ksSchema *KeyspaceGraphQLSchema,
	operation mutationOperation,
) graphql.FieldResolveFn {
	return func(params graphql.ResolveParams) (interface{}, error) {
		value := params.Args["value"].(map[string]interface{})
		columnNames := make([]string, 0, len(value))
		queryParams := make([]interface{}, 0, len(value))

		for key, value := range value {
			columnNames = append(columnNames, ksSchema.naming.ToCQLColumn(table.Name, key))
			queryParams = append(queryParams, adaptParameterValue(value))
		}

		var options types.MutationOptions
		if err := mapstructure.Decode(params.Args["options"], &options); err != nil {
			return nil, err
		}

		userOrRole, err := sg.checkUserOrRoleAuth(params)
		if err != nil {
			return nil, err
		}

		queryOptions := db.NewQueryOptions().
			WithUserOrRole(userOrRole).
			WithConsistency(gocql.Consistency(options.Consistency)).
			WithSerialConsistency(gocql.SerialConsistency(options.SerialConsistency))

		var result db.ResultSet

		switch operation {
		case insertOperation:
			ifNotExists := params.Args["ifNotExists"] == true
			result, err = sg.dbClient.Insert(&db.InsertInfo{
				Keyspace:    table.Keyspace,
				Table:       table.Name,
				Columns:     columnNames,
				QueryParams: queryParams,
				IfNotExists: ifNotExists,
				TTL:         options.TTL,
			}, queryOptions)
		case deleteOperation:
			var ifCondition []types.ConditionItem
			if params.Args["ifCondition"] != nil {
				ifCondition = ksSchema.adaptCondition(
					table.Name, params.Args["ifCondition"].(map[string]interface{}))
			}
			result, err = sg.dbClient.Delete(&db.DeleteInfo{
				Keyspace:    table.Keyspace,
				Table:       table.Name,
				Columns:     columnNames,
				QueryParams: queryParams,
				IfCondition: ifCondition,
				IfExists:    params.Args["ifExists"] == true}, queryOptions)
		case updateOperation:
			var ifCondition []types.ConditionItem
			if params.Args["ifCondition"] != nil {
				ifCondition = ksSchema.adaptCondition(
					table.Name, params.Args["ifCondition"].(map[string]interface{}))
			}
			result, err = sg.dbClient.Update(&db.UpdateInfo{
				Keyspace:    table.Keyspace,
				Table:       table,
				Columns:     columnNames,
				QueryParams: queryParams,
				IfCondition: ifCondition,
				TTL:         options.TTL,
				IfExists:    params.Args["ifExists"] == true}, queryOptions)
		default:
			return false, fmt.Errorf("operation not supported")
		}

		return ksSchema.getModificationResult(table, data, result, err)
	}
}

func adaptParameterValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch value.(type) {
	case int8, int16, int, float32, float64, string, bool:
		// Avoid using reflection for common scalars
		// Ideally, the algorithm should function without this optimization
		return value
	}

	return adaptCollectionParameter(value)
}

func adaptCollectionParameter(value interface{}) interface{} {
	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Slice:
		// Type element (rv.Type().Elem()) is an interface{}
		// We have to inspect the first value
		length := rv.Len()
		if length == 0 {
			return value
		}
		firstElement := rv.Index(0)
		if reflect.TypeOf(firstElement.Interface()).Kind() != reflect.Map {
			return value
		}

		result := make(map[interface{}]interface{})
		// It's a slice of maps that only contains two keys: 'key' and 'value'
		// It's the graphql representation of a map: [KeyValueType]
		for i := 0; i < length; i++ {
			element := rv.Index(i).Interface().(map[string]interface{})
			result[element["key"]] = adaptParameterValue(element["value"])
		}

		return result
	}

	return value
}

func parseColumnOrder(values []interface{}) []db.ColumnOrder {
	result := make([]db.ColumnOrder, 0, len(values))

	for _, value := range values {
		strValue := value.(string)
		index := strings.LastIndex(strValue, "_")
		result = append(result, db.ColumnOrder{
			Column: strValue[0:index],
			Order:  strValue[index+1:],
		})
	}

	return result
}

func adaptResultValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch value.(type) {
	case *int8, *int16, *int, *float32, *float64, *int32, *string, *bool,
		*time.Time, *inf.Dec, *big.Int, *gocql.UUID, *[]byte:
		// Avoid reflection whenever possible
		return value
	}

	rv := reflect.ValueOf(value)
	typeKind := rv.Type().Kind()

	if typeKind == reflect.Ptr && rv.IsNil() {
		return nil
	}

	if !(typeKind == reflect.Ptr && rv.Elem().Type().Kind() == reflect.Map) {
		return value
	}

	rv = rv.Elem()

	// Maps should be adapted to a slice of maps, each map containing 2 keys: 'key' and 'value'
	result := make([]map[string]interface{}, 0, rv.Len())
	iter := rv.MapRange()
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		result = append(result, map[string]interface{}{
			"key":   key.Interface(),
			"value": value.Interface(),
		})
	}

	return result
}
