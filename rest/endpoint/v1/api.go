package endpoint

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/datastax/cassandra-data-apis/auth"
	"github.com/datastax/cassandra-data-apis/config"
	"github.com/datastax/cassandra-data-apis/db"
	e "github.com/datastax/cassandra-data-apis/errors"
	m "github.com/datastax/cassandra-data-apis/rest/models"
	"github.com/datastax/cassandra-data-apis/types"
	"github.com/go-playground/locales/en"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	enTranslations "github.com/go-playground/validator/v10/translations/en"
	"github.com/gocql/gocql"
	"net/http"
	"strings"
)

var (
	inputValidator *validator.Validate
	trans          ut.Translator
)

func init() {
	inputValidator = validator.New()

	uni := ut.New(en.New(), en.New())
	trans, _ = uni.GetTranslator("en")

	_ = enTranslations.RegisterDefaultTranslations(inputValidator, trans)

	_ = inputValidator.RegisterTranslation("required", trans, func(ut ut.Translator) error {
		return ut.Add("required", "{0} is a required field", true)
	}, func(ut ut.Translator, fe validator.FieldError) string {
		translator, _ := ut.T("required", fe.Field())
		return translator
	})

	_ = inputValidator.RegisterTranslation("oneof", trans, func(ut ut.Translator) error {
		return ut.Add("ColumnDefinition.TypeDefinition", "{0} must be a valid type", true) // see universal-translator for details
	}, func(ut ut.Translator, fe validator.FieldError) string {
		translator, _ := ut.T("ColumnDefinition.TypeDefinition", fe.Field())
		return translator
	})
}

func (s *routeList) GetColumns(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	keyspaceName := s.params(r, keyspaceParam)
	tableName := s.params(r, tableParam)
	user := auth.ContextUserOrRole(r.Context())

	table, err := s.dbClient.DescribeTable(keyspaceName, tableName, user)
	if err != nil {
		msg := "unable to describe table"
		s.logger.Debug(msg, "table", tableName, "error", err)

		switch err.(type) {
		case *e.NotFoundError:
			RespondWithError(w, msg, http.StatusNotFound)
			return
		default:
			RespondWithError(w, msg, http.StatusInternalServerError)
			return
		}
	}

	RespondJSONObjectWithCode(w, http.StatusOK, columnMetadataToColumnDefinition(table.Columns))
}

func (s *routeList) GetColumn(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	keyspaceName := s.params(r, keyspaceParam)
	tableName := s.params(r, tableParam)
	columnName := s.params(r, "columnName")
	user := auth.ContextUserOrRole(r.Context())

	table, err := s.dbClient.DescribeTable(keyspaceName, tableName, user)
	if err != nil {
		msg := "unable to describe table"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "column", columnName, "error", err)

		switch err.(type) {
		case *e.NotFoundError:
			RespondWithError(w, msg, http.StatusNotFound)
			return
		default:
			RespondWithError(w, msg, http.StatusInternalServerError)
			return
		}
	}

	columns := columnMetadataToColumnDefinition(table.Columns)
	var column m.ColumnDefinition
	found := false
	for _, col := range columns {
		if col.Name == columnName {
			column = col
			found = true
			break
		}
	}

	if !found {
		RespondWithError(w, fmt.Sprintf("column '%s' not found in table", columnName), http.StatusNotFound)
		return
	}

	RespondJSONObjectWithCode(w, http.StatusOK, column)
}

func (s *routeList) AddColumn(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	keyspaceName := s.params(r, keyspaceParam)
	tableName := s.params(r, tableParam)
	user := auth.ContextUserOrRole(r.Context())

	var columnDefinition m.ColumnDefinition
	if err := parseAndValidatePayload(&columnDefinition, r); err != nil {
		msg := "unable to parse payload"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName)
		RespondWithError(w, msg, http.StatusBadRequest)
		return
	}

	column, err := m.ToDbColumn(columnDefinition)

	if err != nil {
		RespondWithError(w, err.Error(), http.StatusBadRequest)
	}

	tableInfo := db.AlterTableAddInfo{
		Keyspace: keyspaceName,
		Table:    tableName,
		ToAdd:    []*gocql.ColumnMetadata{column},
	}

	err = s.dbClient.AlterTableAdd(&tableInfo, newDbOptions(user))
	if err != nil {
		msg := "unable to execute alter table query"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)

		switch err.(type) {
		case *e.ConflictError:
			RespondWithError(w, msg, http.StatusConflict)
			return
		default:
			RespondWithError(w, msg, http.StatusInternalServerError)
			return
		}
	}

	RespondJSONObjectWithCode(w, http.StatusCreated, m.TablesResponse{Success: true})
}

func (s *routeList) DeleteColumn(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	keyspaceName := s.params(r, keyspaceParam)
	tableName := s.params(r, tableParam)
	columnName := s.params(r, "columnName")
	user := auth.ContextUserOrRole(r.Context())

	err := s.dbClient.AlterTableDrop(&db.AlterTableDropInfo{
		Keyspace: keyspaceName,
		Table:    tableName,
		ToDrop:   []string{columnName},
	}, newDbOptions(user))

	if err != nil {
		msg := "unable to execute alter table query"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "column", columnName, "error", err)

		switch err.(type) {
		case *e.NotFoundError:
			RespondWithError(w, msg, http.StatusNotFound)
			return
		default:
			RespondWithError(w, msg, http.StatusInternalServerError)
			return
		}
	}

	RespondJSONObjectWithCode(w, http.StatusNoContent, nil)
}

func (s *routeList) GetRow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	keyspaceName := s.params(r, keyspaceParam)
	tableName := s.params(r, tableParam)
	rowIdentifier := s.params(r, "rowIdentifier")
	user := auth.ContextUserOrRole(r.Context())

	tblMetadata, err := s.dbClient.Table(keyspaceName, tableName)
	if err != nil {
		if _, ok := err.(*db.DbObjectNotFound); ok {
			RespondWithError(w, fmt.Sprintf(`Table "%s"."%s" not found`, keyspaceName, tableName), http.StatusNotFound)
			return
		}

		msg := "Unable to get table metadata"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
		RespondWithError(w, msg, http.StatusInternalServerError)
		return
	}

	// grab table and extract primary key col names
	columns, values, err := primaryKeyValues(rowIdentifier, tblMetadata)
	if err != nil {
		msg := "Unable to get primary key"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
		RespondWithError(w, msg, http.StatusBadRequest)
		return
	}

	where := make([]types.ConditionItem, len(columns))
	for i, columnName := range columns {
		where[i] = types.ConditionItem{
			Column:   columnName,
			Operator: "=",
			Value:    values[i],
		}
	}

	rs, err := s.dbClient.Select(&db.SelectInfo{
		Keyspace: keyspaceName,
		Table:    tableName,
		Where:    where,
	}, newDbOptions(user))

	if err != nil {
		msg := "unable to execute select query"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
		RespondWithError(w, msg, http.StatusInternalServerError)
		return
	}

	length := len(rs.Values())
	if length == 0 {
		RespondWithError(w, fmt.Sprintf("no row found for primary key %s", s.primaryKeyToString(where)),
			http.StatusNotFound)
		return
	}

	rowsModel := m.Rows{
		Rows:  types.ToJsonValues(rs.Values(), tblMetadata),
		Count: length,
	}

	RespondJSONObjectWithCode(w, http.StatusOK, rowsModel)
}

func (s *routeList) AddRow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	keyspaceName := s.params(r, keyspaceParam)
	tableName := s.params(r, tableParam)
	user := auth.ContextUserOrRole(r.Context())

	var rowAdd m.RowAdd
	if err := parseAndValidatePayload(&rowAdd, r); err != nil {
		msg := "unable to parse payload"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
		RespondWithError(w, msg, http.StatusBadRequest)
		return
	}

	tblMetadata, err := s.dbClient.Table(keyspaceName, tableName)
	if err != nil {
		if _, ok := err.(*db.DbObjectNotFound); ok {
			RespondWithError(w, fmt.Sprintf(`Table "%s"."%s" not found`, keyspaceName, tableName), http.StatusNotFound)
			return
		}

		msg := "unable to get table metadata"
		s.logger.Debug("keyspace", keyspaceName, "table", tableName, "error", err)
		RespondWithError(w, msg, http.StatusInternalServerError)
		return
	}

	if len(rowAdd.Columns) == 0 {
		RespondWithError(w, "Columns can not be empty", http.StatusBadRequest)
		return
	}

	columns := make([]string, len(rowAdd.Columns))
	values := make([]interface{}, len(rowAdd.Columns))

	for i, val := range rowAdd.Columns {
		if _, ok := tblMetadata.Columns[*val.Name]; !ok {
			msg := "Missing column for insert"
			s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
			RespondWithError(w, msg, http.StatusBadRequest)
			return
		}

		convertedType, typeErr := types.FromJsonValue(val.Value, tblMetadata.Columns[*val.Name].Type)
		if typeErr != nil {
			msg := "Wrong type provided for column " + *val.Name
			s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
			RespondWithError(w, msg, http.StatusBadRequest)
			return
		}

		columns[i] = *val.Name
		values[i] = convertedType
	}

	_, err = s.dbClient.Insert(&db.InsertInfo{
		Keyspace:    keyspaceName,
		Table:       tableName,
		Columns:     columns,
		QueryParams: values,
		TTL:         0,
	}, newDbOptions(user))

	if err != nil {
		msg := "unable to execute insert query"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
		RespondWithError(w, msg, http.StatusInternalServerError)
		return
	}

	RespondJSONObjectWithCode(w, http.StatusCreated, m.RowsResponse{
		Success:      true,
		RowsModified: 1,
	})
}

func (s *routeList) Query(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	keyspaceName := s.params(r, keyspaceParam)
	tableName := s.params(r, tableParam)
	user := auth.ContextUserOrRole(r.Context())

	tblMetadata, err := s.dbClient.Table(keyspaceName, tableName)
	if err != nil {
		if _, ok := err.(*db.DbObjectNotFound); ok {
			RespondWithError(w, fmt.Sprintf(`Table "%s"."%s" not found`, keyspaceName, tableName), http.StatusNotFound)
			return
		}

		msg := "Unable to get table metadata"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
		RespondWithError(w, msg, http.StatusInternalServerError)
		return
	}

	var queryModel m.Query
	if err := parseAndValidatePayload(&queryModel, r); err != nil {
		msg := "unable to parse payload"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
		RespondWithError(w, msg, http.StatusBadRequest)
		return
	}

	where := make([]types.ConditionItem, len(queryModel.Filters))

	for i, filter := range queryModel.Filters {
		operator, found := types.CqlOperators[filter.Operator]

		if !found {
			RespondWithError(w, fmt.Sprintf("operator '%s' not found", filter.Operator), http.StatusBadRequest)
			return
		}

		where[i] = types.ConditionItem{
			Column:   filter.ColumnName,
			Operator: operator,
			Value:    filter.Value,
		}
	}

	pageState, err := base64.StdEncoding.DecodeString(queryModel.PageState)
	if err != nil {
		RespondWithError(w, "Invalid page state", http.StatusBadRequest)
		return
	}

	var orderBy []db.ColumnOrder
	if queryModel.OrderBy != nil && queryModel.OrderBy.Column != nil {
		order := "ASC"
		// Allow only ASC/DESC values
		if queryModel.OrderBy.Order != nil && strings.ToUpper(*queryModel.OrderBy.Order) == "DESC" {
			order = "DESC"
		}
		orderBy = []db.ColumnOrder{{
			Column: *queryModel.OrderBy.Column,
			Order:  order,
		}}
	}

	rs, err := s.dbClient.Select(&db.SelectInfo{
		Keyspace: keyspaceName,
		Table:    tableName,
		Columns:  queryModel.ColumnNames,
		Where:    where,
		OrderBy:  orderBy,
	}, newDbOptions(user).WithPageSize(queryModel.PageSize).WithPageState(pageState))

	if err != nil {
		msg := "unable to execute select query"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
		RespondWithError(w, msg, http.StatusInternalServerError)
		return
	}

	rowsModel := m.Rows{
		Rows:      types.ToJsonValues(rs.Values(), tblMetadata),
		PageState: base64.StdEncoding.EncodeToString(rs.PageState()),
		Count:     len(rs.Values()),
	}
	RespondJSONObjectWithCode(w, http.StatusOK, rowsModel)
}

func (s *routeList) UpdateRow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	keyspaceName := s.params(r, keyspaceParam)
	tableName := s.params(r, tableParam)
	rowIdentifier := s.params(r, "rowIdentifier")
	user := auth.ContextUserOrRole(r.Context())

	tblMetadata, err := s.dbClient.Table(keyspaceName, tableName)
	if err != nil {
		if _, ok := err.(*db.DbObjectNotFound); ok {
			RespondWithError(w, fmt.Sprintf(`Table "%s"."%s" not found`, keyspaceName, tableName), http.StatusNotFound)
			return
		}

		msg := "Unable to get table metadata"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
		RespondWithError(w, msg, http.StatusInternalServerError)
		return
	}

	primaryKeysColumns, primaryKeyValues, err := primaryKeyValues(rowIdentifier, tblMetadata)
	if err != nil {
		RespondWithError(w, "Invalid primary keys", http.StatusBadRequest)
	}

	var rowUpdate m.RowsUpdate
	if err := parseAndValidatePayload(&rowUpdate, r); err != nil {
		msg := "unable to parse payload"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
		RespondWithError(w, msg, http.StatusBadRequest)
		return
	}

	columns := make([]string, len(rowUpdate.Changeset)+len(primaryKeysColumns))
	values := make([]interface{}, len(columns))

	for i, val := range rowUpdate.Changeset {
		if _, ok := tblMetadata.Columns[val.Column]; !ok {
			msg := "missing column for changeset"
			s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
			RespondWithError(w, msg, http.StatusBadRequest)
			return
		}

		convertedType, typeErr := types.FromJsonValue(val.Value, tblMetadata.Columns[val.Column].Type)
		if typeErr != nil {
			msg := "wrong type provided for column " + val.Column
			s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
			RespondWithError(w, msg, http.StatusBadRequest)
			return
		}

		columns[i] = val.Column
		values[i] = convertedType
	}

	index := len(rowUpdate.Changeset)
	for i, key := range primaryKeysColumns {
		columns[index] = key
		values[index] = primaryKeyValues[i]
		index++
	}

	_, err = s.dbClient.Update(&db.UpdateInfo{
		Keyspace:    keyspaceName,
		Table:       tblMetadata,
		Columns:     columns,
		QueryParams: values,
		TTL:         -1,
	}, newDbOptions(user))

	if err != nil {
		msg := "Unable to execute update query"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
		RespondWithError(w, msg, http.StatusInternalServerError)
		return
	}

	RespondJSONObjectWithCode(w, http.StatusOK, &m.RowsResponse{
		Success:      true,
		RowsModified: 1,
	})
}

func (s *routeList) DeleteRow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	keyspaceName := s.params(r, keyspaceParam)
	tableName := s.params(r, tableParam)
	rowIdentifier := s.params(r, "rowIdentifier")
	user := auth.ContextUserOrRole(r.Context())

	tblMetadata, err := s.dbClient.Table(keyspaceName, tableName)
	if err != nil {
		if _, ok := err.(*db.DbObjectNotFound); ok {
			RespondWithError(w, fmt.Sprintf(`Table "%s"."%s" not found`, keyspaceName, tableName), http.StatusNotFound)
			return
		}

		msg := "Unable to get table metadata"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
		RespondWithError(w, msg, http.StatusInternalServerError)
		return
	}

	// grab table and extract primary key col names
	columns, values, err := primaryKeyValues(rowIdentifier, tblMetadata)
	if err != nil {
		msg := "Unable to get primary key"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
		RespondWithError(w, msg, http.StatusBadRequest)
		return
	}

	_, err = s.dbClient.Delete(&db.DeleteInfo{
		Keyspace:    keyspaceName,
		Table:       tableName,
		Columns:     columns,
		QueryParams: values,
	}, newDbOptions(user))

	if err != nil {
		msg := "unable to execute delete query"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)
		RespondWithError(w, msg, http.StatusInternalServerError)
		return
	}

	RespondJSONObjectWithCode(w, http.StatusNoContent, nil)
}

func (s *routeList) GetTables(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	keyspaceName := s.params(r, keyspaceParam)
	user := auth.ContextUserOrRole(r.Context())

	if _, err := s.dbClient.Keyspace(keyspaceName); err != nil {
		if _, ok := err.(*db.DbObjectNotFound); ok {
			RespondWithError(w, fmt.Sprintf("Keyspace '%s' not found", keyspaceName), http.StatusNotFound)
			return
		}
		msg := "error retrieving the keyspace"
		s.logger.Error(msg, "keyspace", keyspaceName, "error", err)
		RespondWithError(w, msg, http.StatusInternalServerError)
		return
	}

	tables, err := s.dbClient.DescribeTables(keyspaceName, user)
	if err != nil {
		msg := "unable to describe tables"
		s.logger.Debug(msg, "keyspace", keyspaceName, "error", err)

		switch err.(type) {
		case *e.NotFoundError:
			RespondWithError(w, msg, http.StatusNotFound)
			return
		default:
			RespondWithError(w, msg, http.StatusInternalServerError)
			return
		}
	}

	RespondJSONObjectWithCode(w, http.StatusOK, tables)
}

func (s *routeList) GetTable(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	keyspaceName := s.params(r, keyspaceParam)
	tableName := s.params(r, tableParam)
	user := auth.ContextUserOrRole(r.Context())

	table, err := s.dbClient.DescribeTable(keyspaceName, tableName, user)
	if err != nil {
		msg := "unable to describe table"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)

		switch err.(type) {
		case *e.NotFoundError:
			RespondWithError(w, msg, http.StatusNotFound)
			return
		default:
			RespondWithError(w, msg, http.StatusInternalServerError)
			return
		}
	}

	RespondJSONObjectWithCode(w, http.StatusOK, tableMetadataToTable(table))
}

func (s *routeList) AddTable(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	keyspaceName := s.params(r, keyspaceParam)
	user := auth.ContextUserOrRole(r.Context())

	var tableAdd m.TableAdd
	if err := parseAndValidatePayload(&tableAdd, r); err != nil {
		msg := "unable to parse payload"
		s.logger.Debug(msg, "keyspace", keyspaceName, "error", err)
		RespondWithError(w, msg, http.StatusBadRequest)
		return
	}

	tableInfo := db.CreateTableInfo{
		Keyspace:    keyspaceName,
		Table:       tableAdd.Name,
		IfNotExists: tableAdd.IfNotExists,
	}

	for _, definition := range tableAdd.ColumnDefinitions {
		column, err := m.ToDbColumn(definition)
		if err != nil {
			RespondWithError(w, err.Error(), http.StatusBadRequest)
			return
		}

		if lookup(tableAdd.PrimaryKey.PartitionKey, definition.Name) {
			tableInfo.PartitionKeys = append(tableInfo.PartitionKeys, column)
		} else if lookup(tableAdd.PrimaryKey.ClusteringKey, definition.Name) {
			tableInfo.ClusteringKeys = append(tableInfo.ClusteringKeys, column)
			column.ClusteringOrder = "asc"
			if tableAdd.TableOptions != nil {
				for _, ck := range tableAdd.TableOptions.ClusteringExpression {
					if ck.Column != nil && ck.Order != nil && *ck.Column == definition.Name {
						column.ClusteringOrder = *ck.Order
						break
					}
				}
			}
		} else {
			tableInfo.Values = append(tableInfo.Values, column)
		}
	}

	err := s.dbClient.CreateTable(&tableInfo, newDbOptions(user))
	if err != nil {
		msg := "unable to execute create table query"
		s.logger.Debug(msg, "keyspace", keyspaceName, "error", err)
		RespondWithError(w, msg, http.StatusInternalServerError)
		return
	}

	RespondJSONObjectWithCode(w, http.StatusCreated, m.TablesResponse{Success: true})
}

func (s *routeList) DeleteTable(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	keyspaceName := s.params(r, keyspaceParam)
	tableName := s.params(r, tableParam)
	user := auth.ContextUserOrRole(r.Context())

	err := s.dbClient.DropTable(&db.DropTableInfo{
		Keyspace: keyspaceName,
		Table:    tableName,
	}, newDbOptions(user))

	if err != nil {
		msg := "unable to execute drop table query"
		s.logger.Debug(msg, "keyspace", keyspaceName, "table", tableName, "error", err)

		switch err.(type) {
		case *e.NotFoundError:
			RespondWithError(w, msg, http.StatusNotFound)
			return
		default:
			RespondWithError(w, msg, http.StatusInternalServerError)
			return
		}
	}

	RespondJSONObjectWithCode(w, http.StatusNoContent, nil)
}

func (s *routeList) GetKeyspaces(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	user := auth.ContextUserOrRole(r.Context())

	keyspaces, err := s.dbClient.Keyspaces(user)
	if err != nil {
		msg := "unable to describe keyspaces"
		s.logger.Error(msg, "error", err)

		RespondWithError(w, msg, http.StatusInternalServerError)
		return
	}

	var result []string
	if s.singleKeyspace != "" {
		if !lookup(keyspaces, s.singleKeyspace) {
			// The single keyspace was not found, maybe it's added later
			RespondWithError(w, "Keyspace not found", http.StatusNotFound)
			return
		}

		// Only list the configured keyspace
		result = []string{s.singleKeyspace}
	} else {
		// Filter out excluded keyspaces
		result = make([]string, 0, len(keyspaces))
		for _, ks := range keyspaces {
			if s.excludedKeyspaces[ks] {
				continue
			}
			result = append(result, ks)
		}
	}

	RespondJSONObjectWithCode(w, http.StatusOK, result)
}

func tableMetadataToTable(tableMetadata *gocql.TableMetadata) interface{} {
	partitionKeys := make([]string, 0)
	for _, key := range tableMetadata.PartitionKey {
		partitionKeys = append(partitionKeys, key.Name)
	}

	clusteringKeys := make([]string, 0)
	clusteringExpression := make([]m.ClusteringExpression, 0)
	for _, key := range tableMetadata.ClusteringColumns {
		clusteringKeys = append(clusteringKeys, key.Name)
		clusteringExpression = append(clusteringExpression, m.ClusteringExpression{
			Column: &key.Name,
			Order:  &key.ClusteringOrder,
		})
	}

	primaryKey := &m.PrimaryKey{
		PartitionKey:  partitionKeys,
		ClusteringKey: clusteringKeys,
	}

	tableOptions := &m.TableOptions{
		DefaultTimeToLive:    nil,
		ClusteringExpression: clusteringExpression,
	}

	columnDefinitions := columnMetadataToColumnDefinition(tableMetadata.Columns)

	table := m.Table{
		Name:              tableMetadata.Name,
		Keyspace:          tableMetadata.Keyspace,
		ColumnDefinitions: columnDefinitions,
		PrimaryKey:        primaryKey,
		TableOptions:      tableOptions,
	}
	return table
}

func columnMetadataToColumnDefinition(columns map[string]*gocql.ColumnMetadata) []m.ColumnDefinition {
	columnDefinitions := make([]m.ColumnDefinition, 0)
	for _, col := range columns {
		isStatic := false
		if col.Kind == gocql.ColumnStatic {
			isStatic = true
		}

		columnDefinitions = append(columnDefinitions, m.ColumnDefinition{
			Name:           col.Name,
			TypeDefinition: col.Type.Type().String(),
			Static:         isStatic,
		})
	}
	return columnDefinitions
}

func parseAndValidatePayload(obj interface{}, r *http.Request) error {
	if err := json.NewDecoder(r.Body).Decode(obj); err != nil {
		return err
	}

	if err := inputValidator.Struct(obj); err != nil {
		return e.TranslateValidatorError(err, trans)
	}

	return nil
}

func (s *routeList) primaryKeyToString(m []types.ConditionItem) string {
	jsonString, err := json.Marshal(m)
	if err != nil {
		s.logger.Debug("unable to convert primary key map to string", "error", err)
	}

	return string(jsonString)
}

// primaryKeyValues will return the partition columns of a table
func primaryKeyValues(identifier string, tblMetadata *gocql.TableMetadata) ([]string, []interface{}, error) {
	// Use array to maintain field order all the way
	parts := strings.Split(identifier, ";")
	columns := make([]string, 0, len(parts))
	values := make([]interface{}, len(parts))
	for i, key := range tblMetadata.PartitionKey {
		if i >= len(parts) {
			// In this case the table has a composite key but we were not passed in a value for each column
			return nil, nil, errors.New("not enough parts provided for primary keys")
		}
		index := len(columns)
		values[index] = parts[index]
		columns = append(columns, key.Name)
	}

	// Add clustering keys when provided
	for i := 0; i < len(tblMetadata.ClusteringColumns) && len(columns) < len(parts); i++ {
		key := tblMetadata.ClusteringColumns[i]
		index := len(columns)
		values[index] = parts[index]
		columns = append(columns, key.Name)
	}

	return columns, values, nil
}

func newDbOptions(user string) *db.QueryOptions {
	return db.NewQueryOptions().
		WithUserOrRole(user).
		WithConsistency(config.DefaultConsistencyLevel).
		WithSerialConsistency(config.DefaultSerialConsistencyLevel).
		WithPageSize(config.DefaultPageSize)
}

// lookup linear search for an item into slice, useful for small slices
func lookup(s []string, value string) bool {
	for _, item := range s {
		if item == value {
			return true
		}
	}

	return false
}
