package graphql

import (
	"encoding/base64"
	"fmt"
	"github.com/datastax/cassandra-data-apis/db"
	"github.com/datastax/cassandra-data-apis/types"
	"github.com/gocql/gocql"
	"github.com/graphql-go/graphql"
	"github.com/iancoleman/strcase"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/inf.v0"
	"math/big"
	"reflect"
	"strings"
	"time"
)

func (sg *SchemaGenerator) queryFieldResolver(
	keyspace *gocql.KeyspaceMetadata,
	ksSchema *KeyspaceGraphQLSchema,
) graphql.FieldResolveFn {
	return func(params graphql.ResolveParams) (interface{}, error) {
		fieldName := params.Info.FieldName
		// TODO: Get the table metadata from the closure
		var table *gocql.TableMetadata
		// TODO: Remove
		// GraphQL operation is lower camel
		typeName := strcase.ToCamel(fieldName)
		table, tableFound := keyspace.Tables[ksSchema.naming.ToCQLTable(typeName)]
		var data map[string]interface{}
		if params.Args["data"] != nil {
			data = params.Args["data"].(map[string]interface{})
		} else {
			data = params.Args["filter"].(map[string]interface{})
		}

		var whereClause []types.ConditionItem

		if tableFound {
			whereClause = make([]types.ConditionItem, 0, len(data))
			for key, value := range data {
				whereClause = append(whereClause, types.ConditionItem{
					Column:   ksSchema.naming.ToCQLColumn(table.Name, key),
					Operator: "=",
					Value:    adaptParameterValue(value),
				})
			}
		} else if strings.HasSuffix(typeName, "Filter") {
			table, tableFound = keyspace.Tables[ksSchema.naming.ToCQLTable(strings.TrimSuffix(typeName, "Filter"))]
			if !tableFound {
				return nil, fmt.Errorf("unable to find table '%s'", params.Info.FieldName)
			}

			whereClause = ksSchema.adaptCondition(table.Name, data)
		} else {
			return nil, fmt.Errorf("Unable to find table for '%s'", params.Info.FieldName)
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

		result, err := sg.dbClient.Select(
			&db.SelectInfo{
				Keyspace: keyspace.Name,
				Table:    table.Name,
				Where:    whereClause,
				OrderBy:  parseColumnOrder(orderBy),
				Options:  &options,
			},
			db.NewQueryOptions().
				WithUserOrRole(userOrRole).
				WithPageSize(options.PageSize).
				WithPageState(pageState).
				WithConsistency(gocql.Consistency(options.Consistency)).
				WithSerialConsistency(gocql.SerialConsistency(options.SerialConsistency)))

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
	keyspace *gocql.KeyspaceMetadata,
	ksSchema *KeyspaceGraphQLSchema,
) graphql.FieldResolveFn {
	return func(params graphql.ResolveParams) (interface{}, error) {
		fieldName := params.Info.FieldName
		// TODO: Get the table name from the closure
		operation, typeName := mutationPrefix(fieldName)
		if table, ok := keyspace.Tables[ksSchema.naming.ToCQLTable(typeName)]; ok {
			data := params.Args["data"].(map[string]interface{})
			columnNames := make([]string, 0, len(data))
			queryParams := make([]interface{}, 0, len(data))

			for key, value := range data {
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
			case insertPrefix:
				ifNotExists := params.Args["ifNotExists"] == true
				result, err = sg.dbClient.Insert(&db.InsertInfo{
					Keyspace:    keyspace.Name,
					Table:       table.Name,
					Columns:     columnNames,
					QueryParams: queryParams,
					IfNotExists: ifNotExists,
					TTL:         options.TTL,
				}, queryOptions)
			case deletePrefix:
				var ifCondition []types.ConditionItem
				if params.Args["ifCondition"] != nil {
					ifCondition = ksSchema.adaptCondition(
						table.Name, params.Args["ifCondition"].(map[string]interface{}))
				}
				result, err = sg.dbClient.Delete(&db.DeleteInfo{
					Keyspace:    keyspace.Name,
					Table:       table.Name,
					Columns:     columnNames,
					QueryParams: queryParams,
					IfCondition: ifCondition,
					IfExists:    params.Args["ifExists"] == true}, queryOptions)
			case updatePrefix:
				var ifCondition []types.ConditionItem
				if params.Args["ifCondition"] != nil {
					ifCondition = ksSchema.adaptCondition(
						table.Name, params.Args["ifCondition"].(map[string]interface{}))
				}
				result, err = sg.dbClient.Update(&db.UpdateInfo{
					Keyspace:    keyspace.Name,
					Table:       table,
					Columns:     columnNames,
					QueryParams: queryParams,
					IfCondition: ifCondition,
					TTL:         options.TTL,
					IfExists:    params.Args["ifExists"] == true}, queryOptions)
			default:
				return false, fmt.Errorf("operation '%s' not supported", operation)
			}

			return ksSchema.getModificationResult(table.Name, result, err)
		} else {
			return nil, fmt.Errorf("unable to find table for type name '%s'", params.Info.FieldName)
		}
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

func mutationPrefix(value string) (string, string) {
	mutationPrefixes := []string{insertPrefix, deletePrefix, updatePrefix}

	for _, prefix := range mutationPrefixes {
		if strings.Index(value, prefix) == 0 {
			return prefix, value[len(prefix):]
		}
	}

	panic("Unsupported mutation")
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
