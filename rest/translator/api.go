package translator

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/go-playground/locales/en"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	enTranslations "github.com/go-playground/validator/v10/translations/en"

	e "github.com/datastax/cassandra-data-apis/rest/errors"
	m "github.com/datastax/cassandra-data-apis/rest/models"
)

var (
	validator *validator.Validate
	trans     ut.Translator
)

func init() {
	validator = validator.New()

	uni := ut.New(en.New(), en.New())
	trans, _ = uni.GetTranslator("en")

	_ = enTranslations.RegisterDefaultTranslations(validator, trans)

	_ = validator.RegisterTranslation("required", trans, func(ut ut.Translator) error {
		return ut.Add("required", "{0} is a required field", true)
	}, func(ut ut.Translator, fe validator.FieldError) string {
		t, _ := ut.T("required", fe.Field())
		return t
	})

	_ = validator.RegisterTranslation("oneof", trans, func(ut ut.Translator) error {
		return ut.Add("ColumnDefinition.TypeDefinition", "{0} must be a valid type", true) // see universal-translator for details
	}, func(ut ut.Translator, fe validator.FieldError) string {
		t, _ := ut.T("ColumnDefinition.TypeDefinition", fe.Field())
		return t
	})
}

// APITranslator serves as a translator for going from request objects to CQL statements
type APITranslator struct {
	KeyspaceName string `validate:"required"`
	TableName    string `validate:"required"`
}

// ToSelectFromQuery will transform a Query struct into a SELECT statement with the conjunction of filters comprising the primary
// key as the WHERE clause.
func (a APITranslator) ToSelectFromQuery(queryModel m.Query) (string, []interface{}, error) {
	if err := validator.Struct(a); err != nil {
		return "", nil, e.TranslateValidatorError(err, trans)
	}

	if len(queryModel.Filters) == 0 {
		return "", nil, errors.New("must provide at least one filter")
	}

	returnColumns := "*"
	if len(queryModel.ColumnNames) != 0 {
		returnColumns = strings.Join(queryModel.ColumnNames, ",")
	}

	expression, vals, err := buildExpressionFromOperators(queryModel.Filters)
	if err != nil {
		return "", nil, err
	}

	orderByExpression := ""
	if queryModel.OrderBy != nil {
		if queryModel.OrderBy.Order == nil || queryModel.OrderBy.Column == nil {
			return "", nil, errors.New("both order and column are required for order by expression")
		}

		if strings.ToLower(*queryModel.OrderBy.Order) != "asc" && strings.ToLower(*queryModel.OrderBy.Order) != "desc" {
			return "", nil, errors.New("order must be either 'asc' or 'desc'")
		}

		orderByExpression = "ORDER BY " + *queryModel.OrderBy.Column + " " + *queryModel.OrderBy.Order
	}

	query := fmt.Sprintf("SELECT %s FROM %s.%s WHERE %s %s", returnColumns, a.KeyspaceName, a.TableName, expression, orderByExpression)
	return strings.TrimSuffix(query, " "), vals, nil
}

func buildExpressionFromOperators(filters []m.Filter) (string, []interface{}, error) {
	expression := ""
	var vals []interface{}
	for _, filter := range filters {
		vals = append(vals, filter.Value...)
		if expression != "" {
			expression += " AND "
		}

		op := getOp(filter.Operator)
		if op == "in" {
			placeholder := strings.Repeat("?,", len(filter.Value))
			expression += filter.ColumnName + " in (" + strings.TrimSuffix(placeholder, ",") + ")"
		} else {
			expression += filter.ColumnName + " " + op + " ?"
		}
	}

	return expression, vals, nil
}

func getOp(operator string) string {
	switch operator {
	case "eq":
		return "="
	case "notEq":
		return "!="
	case "gt":
		return ">"
	case "gte":
		return ">="
	case "lt":
		return "<"
	case "lte":
		return "<="
	case "in":
		return "in"
	default:
		return "="
	}
}

// ToSelect will transform a primary key map into a SELECT statement with the conjunction of values comprising the primary
// key as the WHERE clause.
func (a APITranslator) ToSelect(primaryKey map[string]interface{}) (string, []interface{}, error) {
	if err := validator.Struct(a); err != nil {
		return "", nil, e.TranslateValidatorError(err, trans)
	}

	if primaryKey == nil || len(primaryKey) == 0 {
		return "", nil, errors.New("primaryKey must be provided")
	}

	expression, vals, err := buildExpression(primaryKey)
	if err != nil {
		return "", nil, err
	}

	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", a.KeyspaceName, a.TableName, expression)
	return query, vals, nil
}

func buildExpression(primaryKey map[string]interface{}) (string, []interface{}, error) {

	// sort the map first so that we have consistent ordering
	keys := make([]string, 0, len(primaryKey))
	for k := range primaryKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	expression := ""
	var vals []interface{}
	for _, k := range keys {
		if k == "" || primaryKey[k] == nil {
			return "", nil, errors.New("both column name and value must be provided")
		}

		vals = append(vals, primaryKey[k])
		if expression != "" {
			expression += " AND "
		}

		expression += k + " = ?"
	}

	return expression, vals, nil
}

func (a APITranslator) ToInsert(columns []m.Column) (string, []interface{}, error) {
	if err := validator.Struct(a); err != nil {
		return "", nil, e.TranslateValidatorError(err, trans)
	}

	cols := "("
	valPlaceholders := "("
	vals := make([]interface{}, len(columns))
	for i, column := range columns {
		if column.Name == nil || column.Value == nil {
			return "", nil, errors.New("both column name and value must be provided")
		}

		vals[i] = column.Value
		if i == len(columns)-1 {
			cols += *column.Name + ")"
			valPlaceholders += "?)"
			continue
		}
		cols += *column.Name + ", "
		valPlaceholders += "?, "
	}

	query := fmt.Sprintf("INSERT INTO %s.\"%s\" %s VALUES %s", a.KeyspaceName, a.TableName, cols, valPlaceholders)
	// INSERT INTO data_endpoint_auth."token" (auth_token, created_timestamp, username) values ('', '', '')
	return query, vals, nil
}

func (a APITranslator) ToUpdate(rowsUpdate m.RowsUpdate, primaryKey map[string]interface{}) (string, []interface{}, error) {
	if err := validator.Struct(a); err != nil {
		return "", nil, e.TranslateValidatorError(err, trans)
	}

	if primaryKey == nil || len(primaryKey) == 0 {
		return "", nil, errors.New("primaryKey must be provided")
	}

	if len(rowsUpdate.Changeset) == 0 {
		return "", nil, errors.New("change set must be provided for update")
	}

	var vals []interface{}
	setExpression := ""
	for i, changeset := range rowsUpdate.Changeset {
		if changeset.Column == "" || changeset.Value == "" {
			return "", nil, errors.New("both column name and value must be provided")
		}

		vals = append(vals, changeset.Value)
		if i == len(rowsUpdate.Changeset)-1 {
			setExpression += changeset.Column + " = ?"
			continue
		}

		setExpression += changeset.Column + " = ?, "
	}

	expression, primaryKeyVals, err := buildExpression(primaryKey)
	if err != nil {
		return "", nil, err
	}

	vals = append(vals, primaryKeyVals...)

	query := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s", a.KeyspaceName, a.TableName, setExpression, expression)
	return query, vals, nil
}

func (a APITranslator) ToDelete(primaryKey map[string]interface{}) (string, []interface{}, error) {
	if err := validator.Struct(a); err != nil {
		return "", nil, e.TranslateValidatorError(err, trans)
	}

	if primaryKey == nil || len(primaryKey) == 0 {
		return "", nil, errors.New("primaryKey must be provided")
	}

	expression, vals, err := buildExpression(primaryKey)
	if err != nil {
		return "", nil, err
	}

	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", a.KeyspaceName, a.TableName, expression)
	return query, vals, nil
}

func (a APITranslator) ToCreateTable(table m.TableAdd) (string, error) {
	if strings.TrimSpace(a.KeyspaceName) == "" {
		return "", errors.New("keyspaceName must be provided")
	}
	if strings.TrimSpace(table.Name) == "" {
		return "", errors.New("table name must be provided")
	}

	if table.PrimaryKey == nil {
		return "", errors.New("primary key must be provided")
	}

	if len(table.PrimaryKey.PartitionKey) == 0 {
		return "", errors.New("primary key must include at least one partition key")
	}

	createStmt := "CREATE TABLE"
	if table.IfNotExists {
		createStmt += " IF NOT EXISTS"
	}

	columnDefinitions := "("
	for _, colDef := range table.ColumnDefinitions {
		if err := validator.Struct(colDef); err != nil {
			return "", e.TranslateValidatorError(err, trans)
		}

		columnDefinitions += colDef.Name + " " + colDef.TypeDefinition
		if colDef.Static {
			columnDefinitions += "STATIC"
		}

		columnDefinitions += ", "
	}

	primaryKey := "(" + strings.Join(table.PrimaryKey.PartitionKey, ", ") + ")"
	if len(table.PrimaryKey.ClusteringKey) > 0 {
		clusteringKey := strings.Join(table.PrimaryKey.ClusteringKey, ", ")
		primaryKey = "(" + primaryKey + ", " + clusteringKey + ")"
	}

	columnDefinitions += "PRIMARY KEY " + primaryKey + ")"

	tableOptions, err := getTableOptions(table)
	if err != nil {
		return "", fmt.Errorf("unable to get table options for create: %v", err.Error())
	}

	query := fmt.Sprintf("%s %s.%s %s %s", createStmt, a.KeyspaceName, table.Name, columnDefinitions, tableOptions)
	return strings.TrimSuffix(query, " "), nil
}

func getTableOptions(table m.TableAdd) (string, error) {
	options := table.TableOptions
	if options == nil {
		return "", nil
	}

	tableOptions := ""
	if options.DefaultTimeToLive != nil {
		tableOptions = getOptionPrefix(tableOptions)

		tableOptions += "default_time_to_live = " + strconv.FormatInt(int64(*options.DefaultTimeToLive), 10)
	}

	if options.ClusteringExpression != nil && len(options.ClusteringExpression) > 0 {
		tableOptions = getOptionPrefix(tableOptions)

		expression := ""
		for i, exp := range options.ClusteringExpression {
			if exp.Order == nil || exp.Column == nil {
				return "", errors.New("both order and column are required for clustering expression")
			}

			if strings.ToLower(*exp.Order) != "asc" && strings.ToLower(*exp.Order) != "desc" {
				return "", errors.New("order must be either 'asc' or 'desc'")
			}

			if i == len(options.ClusteringExpression)-1 {
				expression += *exp.Column + " " + *exp.Order
				continue
			}

			expression += *exp.Column + " " + *exp.Order + ", "
		}

		tableOptions += fmt.Sprintf("CLUSTERING ORDER BY (%s)", expression)
	}
	return tableOptions, nil
}

func getOptionPrefix(tableOptions string) string {
	if tableOptions == "" {
		tableOptions = "WITH "
	} else {
		tableOptions += " AND "
	}
	return tableOptions
}

func (a APITranslator) ToAlterTableAddColumn(columnDefinition m.ColumnDefinition) (string, error) {
	if err := validator.Struct(a); err != nil {
		return "", err
	}

	columnDefinitions := columnDefinition.Name + " " + columnDefinition.TypeDefinition
	if columnDefinition.Static {
		columnDefinitions += " STATIC"
	}

	alterInstructions := "ADD (" + columnDefinitions + ")"

	query := fmt.Sprintf("ALTER TABLE %s.%s %s", a.KeyspaceName, a.TableName, alterInstructions)
	return query, nil
}

func (a APITranslator) ToAlterTableUpdateColumn(oldColumn, newColumn string) (string, error) {
	if err := validator.Struct(a); err != nil {
		return "", err
	}

	alterInstructions := "RENAME " + oldColumn + " TO " + newColumn

	query := fmt.Sprintf("ALTER TABLE %s.%s %s", a.KeyspaceName, a.TableName, alterInstructions)
	return query, nil
}

func (a APITranslator) ToAlterTableDeleteColumn(columnName string) (string, error) {
	if err := validator.Struct(a); err != nil {
		return "", err
	}

	alterInstructions := "DROP (" + columnName + ")"

	query := fmt.Sprintf("ALTER TABLE %s.%s %s", a.KeyspaceName, a.TableName, alterInstructions)
	return query, nil
}

func (a APITranslator) ToDropTable() (string, error) {
	if err := validator.Struct(a); err != nil {
		return "", e.TranslateValidatorError(err, trans)
	}

	query := fmt.Sprintf("DROP TABLE %s.%s", a.KeyspaceName, a.TableName)

	return query, nil
}
