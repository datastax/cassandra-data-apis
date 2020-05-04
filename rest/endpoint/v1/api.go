package endpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-playground/locales/en"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	enTranslations "github.com/go-playground/validator/v10/translations/en"
	"github.com/gocql/gocql"
	inf "gopkg.in/inf.v0"

	"github.com/datastax/cassandra-data-apis/rest/contextutils"
	"github.com/datastax/cassandra-data-apis/rest/db"
	e "github.com/datastax/cassandra-data-apis/rest/errors"
	m "github.com/datastax/cassandra-data-apis/rest/models"
	t "github.com/datastax/cassandra-data-apis/rest/translator"
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
	ctx := r.Context()

	keyspaceName := s.params(r, "keyspaceName")
	tableName := s.params(r, "tableName")
	user := contextutils.GetContextUser(ctx)

	table, err := s.dbConn.Database.DescribeTable(keyspaceName, tableName, user)
	if err != nil {
		msg := "unable to describe table"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)

		switch err.(type) {
		case *e.NotFoundError:
			RespondWithError(w, fmt.Errorf(msg), http.StatusNotFound)
			return
		default:
			RespondWithError(w, fmt.Errorf(msg), http.StatusInternalServerError)
			return
		}
	}

	RespondJSONObjectWithCode(w, http.StatusOK, columnMetadataToColumnDefinition(table.Columns))
}

func (s *routeList) GetColumn(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	ctx := r.Context()

	keyspaceName := s.params(r, "keyspaceName")
	tableName := s.params(r, "tableName")
	columnName := s.params(r, "columnName")
	user := contextutils.GetContextUser(ctx)

	table, err := s.dbConn.Database.DescribeTable(keyspaceName, tableName, user)
	if err != nil {
		msg := "unable to describe table"
		//TODO: log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("column", columnName).
		//	With("error", err).
		//	Error(msg)

		switch err.(type) {
		case *e.NotFoundError:
			RespondWithError(w, fmt.Errorf(msg), http.StatusNotFound)
			return
		default:
			RespondWithError(w, fmt.Errorf(msg), http.StatusInternalServerError)
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
		RespondWithError(w, fmt.Errorf("column '%s' not found in table", columnName), http.StatusNotFound)
		return
	}

	RespondJSONObjectWithCode(w, http.StatusOK, column)
}

func (s *routeList) AddColumn(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	ctx := r.Context()

	keyspaceName := s.params(r, "keyspaceName")
	tableName := s.params(r, "tableName")
	user := contextutils.GetContextUser(ctx)

	var columnDefinition m.ColumnDefinition
	if err := parseAndValidatePayload(&columnDefinition, r); err != nil {
		msg := "unable to parse payload"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusBadRequest)
		return
	}

	translator := t.APITranslator{
		KeyspaceName: keyspaceName,
		TableName:    tableName,
	}

	query, err := translator.ToAlterTableAddColumn(columnDefinition)
	if err != nil {
		msg := "unable to translate to alter table query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusBadRequest)
		return
	}

	err = s.dbConn.Database.Alter(query, user)
	if err != nil {
		msg := "unable to execute alter table query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)

		switch err.(type) {
		case *e.ConflictError:
			RespondWithError(w, fmt.Errorf(msg), http.StatusConflict)
			return
		default:
			RespondWithError(w, fmt.Errorf(msg), http.StatusInternalServerError)
			return
		}
	}

	RespondJSONObjectWithCode(w, http.StatusCreated, m.TablesResponse{Success: true})
}

func (s *routeList) UpdateColumn(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	ctx := r.Context()

	keyspaceName := s.params(r, "keyspaceName")
	tableName := s.params(r, "tableName")
	columnName := s.params(r, "columnName")
	user := contextutils.GetContextUser(ctx)

	var columnUpdate m.ColumnUpdate
	if err := parseAndValidatePayload(&columnUpdate, r); err != nil {
		msg := "unable to parse payload"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("column", columnName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusBadRequest)
		return
	}

	translator := t.APITranslator{
		KeyspaceName: keyspaceName,
		TableName:    tableName,
	}

	query, err := translator.ToAlterTableUpdateColumn(columnName, columnUpdate.NewName)
	if err != nil {
		msg := "unable to translate to alter table query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("column", columnName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusBadRequest)
		return
	}

	err = s.dbConn.Database.Alter(query, user)
	if err != nil {
		msg := "unable to execute alter table query"
		//TODO: log.With("keyspace", keyspaceName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusInternalServerError)
		return
	}

	RespondJSONObjectWithCode(w, http.StatusOK, m.TablesResponse{Success: true})
}

func (s *routeList) DeleteColumn(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	ctx := r.Context()

	keyspaceName := s.params(r, "keyspaceName")
	tableName := s.params(r, "tableName")
	columnName := s.params(r, "columnName")
	user := contextutils.GetContextUser(ctx)

	translator := t.APITranslator{
		KeyspaceName: keyspaceName,
		TableName:    tableName,
	}

	query, err := translator.ToAlterTableDeleteColumn(columnName)
	if err != nil {
		msg := "unable to translate to alter table query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("column", columnName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusBadRequest)
		return
	}

	err = s.dbConn.Database.Alter(query, user)
	if err != nil {
		msg := "unable to execute alter table query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("column", columnName).
		//	With("error", err).
		//	Error(msg)

		switch err.(type) {
		case *e.NotFoundError:
			RespondWithError(w, fmt.Errorf(msg), http.StatusNotFound)
			return
		default:
			RespondWithError(w, fmt.Errorf(msg), http.StatusInternalServerError)
			return
		}
	}

	RespondJSONObjectWithCode(w, http.StatusNoContent, nil)
}

func (s *routeList) GetRow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	ctx := r.Context()

	keyspaceName := s.params(r, "keyspaceName")
	tableName := s.params(r, "tableName")
	rowIdentifier := s.params(r, "rowIdentifier")
	user := contextutils.GetContextUser(ctx)

	// grab table and extract primary key col names
	primaryKey, err := rowIdentifierToMap(rowIdentifier, keyspaceName, tableName, s.dbConn.Database)
	if err != nil {
		msg := "unable to get primary key"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)

		switch err.(type) {
		case *e.InternalError:
			RespondWithError(w, fmt.Errorf(msg), http.StatusInternalServerError)
			return
		default:
			RespondWithError(w, fmt.Errorf(msg), http.StatusBadRequest)
			return
		}
	}

	translator := t.APITranslator{
		KeyspaceName: keyspaceName,
		TableName:    tableName,
	}

	query, vals, err := translator.ToSelect(primaryKey)
	if err != nil {
		msg := "unable to translate to select query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusBadRequest)
		return
	}

	rows, err := s.dbConn.Database.Select(query, user, vals...)
	if err != nil {
		msg := "unable to execute select query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusInternalServerError)
		return
	}

	if len(rows) == 0 {
		RespondWithError(w, fmt.Errorf("no row found for primary key %s", primaryKeyToString(primaryKey)), http.StatusNotFound)
		return
	}

	rowsModel := m.Rows{
		Rows:  rows,
		Count: len(rows),
	}
	RespondJSONObjectWithCode(w, http.StatusOK, rowsModel)
}

func (s *routeList) AddRow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	ctx := r.Context()

	keyspaceName := s.params(r, "keyspaceName")
	tableName := s.params(r, "tableName")
	user := contextutils.GetContextUser(ctx)

	var rowAdd m.RowAdd
	if err := parseAndValidatePayload(&rowAdd, r); err != nil {
		msg := "unable to parse payload"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusBadRequest)
		return
	}

	tblMetadata, err := s.dbConn.Database.DescribeTable(keyspaceName, tableName, "")
	if err != nil {
		msg := "unable to get table metadata"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)

		RespondWithError(w, fmt.Errorf(msg), http.StatusInternalServerError)
		return
	}

	for i, val := range rowAdd.Columns {
		if _, ok := tblMetadata.Columns[*val.Name]; !ok {
			msg := "missing column for insert"
			//TODO log.With("keyspace", keyspaceName).
			//	With("table", tableName).
			//	With("error", err).
			//	Error(msg)

			RespondWithError(w, fmt.Errorf(msg), http.StatusBadRequest)
			return
		}

		if val.Value == nil {
			msg := "missing value for insert"
			//TODO log.With("keyspace", keyspaceName).
			//	With("table", tableName).
			//	With("error", err).
			//	Error(msg)

			RespondWithError(w, fmt.Errorf(msg), http.StatusBadRequest)
			return
		}

		convertedType, typeErr := getCQLType(tblMetadata.Columns[*val.Name].Type, val.Value)
		if typeErr != nil {
			msg := "wrong type provided for column " + *val.Name
			//TODO log.With("keyspace", keyspaceName).
			//	With("table", tableName).
			//	With("error", err).
			//	Error(msg)

			RespondWithError(w, fmt.Errorf(msg), http.StatusBadRequest)
			return
		}
		rowAdd.Columns[i].Value = convertedType
	}

	translator := t.APITranslator{
		KeyspaceName: keyspaceName,
		TableName:    tableName,
	}

	query, vals, err := translator.ToInsert(rowAdd.Columns)
	if err != nil {
		msg := "unable to translate to insert query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusBadRequest)
		return
	}

	err = s.dbConn.Database.Insert(query, user, vals...)
	if err != nil {
		msg := "unable to execute insert query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusInternalServerError)
		return
	}

	RespondJSONObjectWithCode(w, http.StatusCreated, m.RowsResponse{
		Success:      true,
		RowsModified: 1,
	})
}

func (s *routeList) Query(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	ctx := r.Context()

	keyspaceName := s.params(r, "keyspaceName")
	tableName := s.params(r, "tableName")
	user := contextutils.GetContextUser(ctx)

	var queryModel m.Query
	if err := parseAndValidatePayload(&queryModel, r); err != nil {
		msg := "unable to parse payload"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusBadRequest)
		return
	}

	translator := t.APITranslator{
		KeyspaceName: keyspaceName,
		TableName:    tableName,
	}

	query, vals, err := translator.ToSelectFromQuery(queryModel)
	if err != nil {
		msg := "unable to translate to select query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusBadRequest)
		return
	}

	rows, pageState, err := s.dbConn.Database.SelectWithPaging(query, user, queryModel.PageState, queryModel.PageSize, vals...)
	if err != nil {
		msg := "unable to execute select query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusInternalServerError)
		return
	}

	rowsModel := m.Rows{
		Rows:      rows,
		PageState: pageState,
		Count:     len(rows),
	}
	RespondJSONObjectWithCode(w, http.StatusOK, rowsModel)
}

func (s *routeList) UpdateRow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	ctx := r.Context()

	keyspaceName := s.params(r, "keyspaceName")
	tableName := s.params(r, "tableName")
	rowIdentifier := s.params(r, "rowIdentifier")
	user := contextutils.GetContextUser(ctx)

	var rowUpdate m.RowsUpdate
	if err := parseAndValidatePayload(&rowUpdate, r); err != nil {
		msg := "unable to parse payload"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusBadRequest)
		return
	}

	// grab table and extract primary key col names
	primaryKey, err := rowIdentifierToMapForUpdate(rowIdentifier, keyspaceName, tableName, s.dbConn.Database)
	if err != nil {
		msg := "unable to get primary key"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)

		switch err.(type) {
		case *e.InternalError:
			RespondWithError(w, fmt.Errorf(msg), http.StatusInternalServerError)
			return
		default:
			RespondWithError(w, fmt.Errorf(msg), http.StatusBadRequest)
			return
		}
	}

	tblMetadata, err := s.dbConn.Database.DescribeTable(keyspaceName, tableName, "")
	if err != nil {
		msg := "unable to get table metadata"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)

		RespondWithError(w, fmt.Errorf(msg), http.StatusInternalServerError)
		return
	}

	for i, val := range rowUpdate.Changeset {
		if _, ok := tblMetadata.Columns[val.Column]; !ok {
			msg := "missing column for changeset"
			//TODO log.With("keyspace", keyspaceName).
			//	With("table", tableName).
			//	With("error", err).
			//	Error(msg)

			RespondWithError(w, fmt.Errorf(msg), http.StatusBadRequest)
			return
		}

		if val.Value == nil {
			msg := "missing value for changeset"
			//TODO log.With("keyspace", keyspaceName).
			//	With("table", tableName).
			//	With("error", err).
			//	Error(msg)

			RespondWithError(w, fmt.Errorf(msg), http.StatusBadRequest)
			return
		}

		convertedType, typeErr := getCQLType(tblMetadata.Columns[val.Column].Type, val.Value)
		if typeErr != nil {
			msg := "wrong type provided for column " + val.Column
			//TODO log.With("keyspace", keyspaceName).
			//	With("table", tableName).
			//	With("error", err).
			//	Error(msg)

			RespondWithError(w, fmt.Errorf(msg), http.StatusBadRequest)
			return
		}
		rowUpdate.Changeset[i].Value = convertedType
	}

	translator := t.APITranslator{
		KeyspaceName: keyspaceName,
		TableName:    tableName,
	}

	query, vals, err := translator.ToUpdate(rowUpdate, primaryKey)
	if err != nil {
		msg := "unable to translate to update query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusBadRequest)
		return
	}

	err = s.dbConn.Database.Update(query, user, vals...)
	if err != nil {
		msg := "unable to execute update query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusInternalServerError)
		return
	}

	RespondJSONObjectWithCode(w, http.StatusOK, &m.RowsResponse{
		Success:      true,
		RowsModified: 1,
	})
}

func (s *routeList) DeleteRow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	ctx := r.Context()

	keyspaceName := s.params(r, "keyspaceName")
	tableName := s.params(r, "tableName")
	rowIdentifier := s.params(r, "rowIdentifier")
	user := contextutils.GetContextUser(ctx)

	// grab table and extract primary key col names
	primaryKey, err := rowIdentifierToMap(rowIdentifier, keyspaceName, tableName, s.dbConn.Database)
	if err != nil {
		msg := "unable to get primary key"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)

		switch err.(type) {
		case *e.InternalError:
			RespondWithError(w, fmt.Errorf(msg), http.StatusInternalServerError)
			return
		default:
			RespondWithError(w, fmt.Errorf(msg), http.StatusBadRequest)
			return
		}
	}

	translator := t.APITranslator{
		KeyspaceName: keyspaceName,
		TableName:    tableName,
	}

	query, vals, err := translator.ToDelete(primaryKey)
	if err != nil {
		msg := "unable to translate to delete query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusBadRequest)
		return
	}

	err = s.dbConn.Database.Delete(query, user, vals...)
	if err != nil {
		msg := "unable to execute delete query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusInternalServerError)
		return
	}

	RespondJSONObjectWithCode(w, http.StatusNoContent, nil)
}

func (s *routeList) GetTables(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	ctx := r.Context()

	keyspaceName := s.params(r, "keyspaceName")
	user := contextutils.GetContextUser(ctx)

	tables, err := s.dbConn.Database.DescribeTables(keyspaceName, user)
	if err != nil {
		msg := "unable to describe tables"
		//TODO log.With("keyspace", keyspaceName).
		//	With("error", err).
		//	Error(msg)

		switch err.(type) {
		case *e.NotFoundError:
			RespondWithError(w, fmt.Errorf(msg), http.StatusNotFound)
			return
		default:
			RespondWithError(w, fmt.Errorf(msg), http.StatusInternalServerError)
			return
		}
	}

	RespondJSONObjectWithCode(w, http.StatusOK, tables)
}

func (s *routeList) GetTable(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	ctx := r.Context()

	keyspaceName := s.params(r, "keyspaceName")
	tableName := s.params(r, "tableName")
	user := contextutils.GetContextUser(ctx)

	table, err := s.dbConn.Database.DescribeTable(keyspaceName, tableName, user)
	if err != nil {
		msg := "unable to describe table"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)

		switch err.(type) {
		case *e.NotFoundError:
			RespondWithError(w, fmt.Errorf(msg), http.StatusNotFound)
			return
		default:
			RespondWithError(w, fmt.Errorf(msg), http.StatusInternalServerError)
			return
		}
	}

	RespondJSONObjectWithCode(w, http.StatusOK, tableMetadataToTable(table))
}

func (s *routeList) AddTable(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	ctx := r.Context()

	keyspaceName := s.params(r, "keyspaceName")
	user := contextutils.GetContextUser(ctx)

	var tableAdd m.TableAdd
	if err := parseAndValidatePayload(&tableAdd, r); err != nil {
		msg := "unable to parse payload"
		//TODO log.With("keyspace", keyspaceName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusBadRequest)
		return
	}

	translator := t.APITranslator{
		KeyspaceName: keyspaceName,
	}

	query, err := translator.ToCreateTable(tableAdd)
	if err != nil {
		msg := "unable to translate to create table query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusBadRequest)
		return
	}

	err = s.dbConn.Database.Create(query, user)
	if err != nil {
		msg := "unable to execute create table query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusInternalServerError)
		return
	}

	RespondJSONObjectWithCode(w, http.StatusCreated, m.TablesResponse{Success: true})
}

func (s *routeList) DeleteTable(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	ctx := r.Context()

	keyspaceName := s.params(r, "keyspaceName")
	tableName := s.params(r, "tableName")
	user := contextutils.GetContextUser(ctx)

	translator := t.APITranslator{
		KeyspaceName: keyspaceName,
		TableName:    tableName,
	}

	query, err := translator.ToDropTable()
	if err != nil {
		msg := "unable to translate to drop table query"
		//TODO: log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)
		RespondWithError(w, errors.New(msg), http.StatusBadRequest)
		return
	}

	err = s.dbConn.Database.Drop(query, user)
	if err != nil {
		msg := "unable to execute drop table query"
		//TODO log.With("keyspace", keyspaceName).
		//	With("table", tableName).
		//	With("error", err).
		//	Error(msg)

		switch err.(type) {
		case *e.NotFoundError:
			RespondWithError(w, fmt.Errorf(msg), http.StatusNotFound)
			return
		default:
			RespondWithError(w, fmt.Errorf(msg), http.StatusInternalServerError)
			return
		}
	}

	RespondJSONObjectWithCode(w, http.StatusNoContent, nil)
}

func (s *routeList) GetKeyspaces(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	ctx := r.Context()

	user := contextutils.GetContextUser(ctx)

	keyspaces, err := s.dbConn.Database.DescribeKeyspaces(user)
	if err != nil {
		msg := "unable to describe keyspaces"
		//TODO log.With("error", err).Error(msg)

		RespondWithError(w, fmt.Errorf(msg), http.StatusInternalServerError)
		return
	}

	RespondJSONObjectWithCode(w, http.StatusOK, keyspaces)
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

func primaryKeyToString(m map[string]interface{}) string {
	jsonString, err := json.Marshal(m)
	if err != nil {
		//TODO log.With("error", err).Error("unable to convert primary key map to string")
	}

	return string(jsonString)
}

// rowIdentifierToMap will return the partition columns of a table
func rowIdentifierToMap(identifier, keyspace, table string, db db.DB) (map[string]interface{}, error) {
	tblMetadata, err := db.DescribeTable(keyspace, table, "")
	if err != nil {
		return nil, e.NewInternalError(fmt.Sprintf("Unable to describe table: %s", err))
	}

	vals := strings.Split(identifier, ";")
	primaryKey := make(map[string]interface{}, len(vals))
	for i, key := range tblMetadata.PartitionKey {
		if i >= len(vals) {
			// In this case the table has a composite key but we were not passed in a value for each column
			return nil, errors.New("not enough values provided for primary keys")
		}

		primaryKey[key.Name] = vals[i]
	}

	return primaryKey, nil
}

// rowIdentifierToMapForUpdate will return the partition columns of a table and the clustering keys if they exists
func rowIdentifierToMapForUpdate(identifier, keyspace, table string, db db.DB) (map[string]interface{}, error) {
	tblMetadata, err := db.DescribeTable(keyspace, table, "")
	if err != nil {
		return nil, e.NewInternalError(fmt.Sprintf("Unable to describe table: %s", err))
	}

	vals := strings.Split(identifier, ";")
	primaryKey := make(map[string]interface{}, len(vals))
	for i, key := range append(tblMetadata.PartitionKey, tblMetadata.ClusteringColumns...) {
		if i >= len(vals) {
			// In this case the table has a composite key but we were not passed in a value for each column
			return nil, errors.New("not enough values provided for primary keys")
		}

		primaryKey[key.Name] = vals[i]
	}

	return primaryKey, nil
}

func getCQLType(typeInfo gocql.TypeInfo, val interface{}) (interface{}, error) {
	switch typeInfo.Type() {
	case gocql.TypeDecimal:
		d := new(inf.Dec)
		if f, ok := val.(float64); ok {
			d.SetString(fmt.Sprintf("%f", f))
		} else if s, ok := val.(string); ok {
			floatVal, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return nil, errors.New("wrong type provided for decimal type")
			}
			d.SetString(fmt.Sprintf("%f", floatVal))
		}
		return d, nil
	case gocql.TypeInt:
		if f, ok := val.(float64); ok {
			return int(f), nil
		} else if s, ok := val.(string); ok {
			n, _ := strconv.Atoi(s)
			return n, nil
		} else if i, ok := val.(int); ok {
			// unlikely case since the incoming request should marshal the field as a float64
			return i, nil
		}

		return nil, errors.New("wrong type provided for int type")
	}

	return val, nil
}
