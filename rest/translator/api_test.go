package translator

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/sergi/go-diff/diffmatchpatch"

	m "github.com/datastax/cassandra-data-apis/rest/models"
)

func intToPointer(val int32) *int32 {
	return &val
}

func stringToPointer(val string) *string {
	return &val
}

type fields struct {
	KeyspaceName string
	TableName    string
}

func TestToSelect(t *testing.T) {
	type args struct {
		primaryKey map[string]interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		want1   []interface{}
		wantErr bool
	}{
		{
			name: "Happy Path",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				primaryKey: map[string]interface{}{
					"id": "8be6d514-3436-4e04-a5fc-0ffbefa4c1fe",
				},
			},
			want:    `SELECT * FROM company.employees WHERE id = ?`,
			want1:   []interface{}{"8be6d514-3436-4e04-a5fc-0ffbefa4c1fe"},
			wantErr: false,
		}, {
			name: "Missing keyspaceName",
			fields: fields{
				KeyspaceName: "",
				TableName:    "employees",
			},
			args: args{
				primaryKey: map[string]interface{}{
					"foo": "bar",
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		}, {
			name: "Missing tableName",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "",
			},
			args: args{
				primaryKey: map[string]interface{}{
					"foo": "bar",
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		}, {
			name: "Missing primary key",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				primaryKey: map[string]interface{}{},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		}, {
			name: "Missing primary key value",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				primaryKey: map[string]interface{}{
					"foo": nil,
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		}, {
			name: "Injection Escaped",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				primaryKey: map[string]interface{}{
					"name": "Robert'; DROP TABLE Students;--",
				},
			},
			want:    `SELECT * FROM company.employees WHERE name = ?`,
			want1:   []interface{}{"Robert'; DROP TABLE Students;--"},
			wantErr: false,
		}, {
			name: "Text Key",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				primaryKey: map[string]interface{}{
					"id": "8be6d514-3436-4e04-a5fc-0ffbefa4c1fe",
				},
			},
			want:    `SELECT * FROM company.employees WHERE id = ?`,
			want1:   []interface{}{"8be6d514-3436-4e04-a5fc-0ffbefa4c1fe"},
			wantErr: false,
		}, {
			name: "Number Key",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				primaryKey: map[string]interface{}{
					"id": 10,
				},
			},
			want:    `SELECT * FROM company.employees WHERE id = ?`,
			want1:   []interface{}{10},
			wantErr: false,
		}, {
			name: "String uuid Key",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				primaryKey: map[string]interface{}{
					"id": "8be6d514-3436-4e04-a5fc-0ffbefa4c1fe",
				},
			},
			want:    `SELECT * FROM company.employees WHERE id = ?`,
			want1:   []interface{}{"8be6d514-3436-4e04-a5fc-0ffbefa4c1fe"},
			wantErr: false,
		}, {
			name: "Composite Key",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				primaryKey: map[string]interface{}{
					"id":  "8be6d514-3436-4e04-a5fc-0ffbefa4c1fe",
					"age": 20,
				},
			},
			want:    `SELECT * FROM company.employees WHERE age = ? AND id = ?`,
			want1:   []interface{}{20, "8be6d514-3436-4e04-a5fc-0ffbefa4c1fe"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := APITranslator{
				KeyspaceName: tt.fields.KeyspaceName,
				TableName:    tt.fields.TableName,
			}
			got, got1, err := a.ToSelect(tt.args.primaryKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToSelect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ToSelect() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ToSelect() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestToUpdate(t *testing.T) {
	type args struct {
		rowsUpdate m.RowsUpdate
		primaryKey map[string]interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		want1   []interface{}
		wantErr bool
	}{
		{
			name: "Happy Path",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				rowsUpdate: m.RowsUpdate{
					Changeset: []m.Changeset{{
						Column: "firstName",
						Value:  "Joe",
					}},
				},
				primaryKey: map[string]interface{}{
					"id": 123,
				},
			},
			want:    `UPDATE company.employees SET firstName = ? WHERE id = ?`,
			want1:   []interface{}{"Joe", 123},
			wantErr: false,
		}, {
			name: "Composite key",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				rowsUpdate: m.RowsUpdate{
					Changeset: []m.Changeset{{
						Column: "firstName",
						Value:  "Joe",
					}},
				},
				primaryKey: map[string]interface{}{
					"id":       123,
					"lastname": "Doe",
				},
			},
			want:    `UPDATE company.employees SET firstName = ? WHERE id = ? AND lastname = ?`,
			want1:   []interface{}{"Joe", 123, "Doe"},
			wantErr: false,
		}, {
			name: "Multiple columns",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				rowsUpdate: m.RowsUpdate{
					Changeset: []m.Changeset{{
						Column: "firstName",
						Value:  "Joe",
					}, {
						Column: "lastName",
						Value:  "Smith",
					}},
				},
				primaryKey: map[string]interface{}{
					"id": "123",
				},
			},
			want:    `UPDATE company.employees SET firstName = ?, lastName = ? WHERE id = ?`,
			want1:   []interface{}{"Joe", "Smith", "123"},
			wantErr: false,
		}, {
			name: "Extra param provided",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				rowsUpdate: m.RowsUpdate{
					Changeset: []m.Changeset{{
						Column: "",
						Value:  "",
					}},
				},
				primaryKey: map[string]interface{}{
					"foo": "bar",
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		}, {
			name: "Missing param",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				rowsUpdate: m.RowsUpdate{
					Changeset: []m.Changeset{{
						Column: "",
						Value:  "",
					}},
				},
				primaryKey: map[string]interface{}{
					"foo": "bar",
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		}, {
			name: "No params",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				rowsUpdate: m.RowsUpdate{
					Changeset: []m.Changeset{{
						Column: "",
						Value:  "",
					}},
				},
				primaryKey: map[string]interface{}{
					"foo": "bar",
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		}, {
			name: "Missing key",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				rowsUpdate: m.RowsUpdate{
					Changeset: []m.Changeset{{
						Column: "",
						Value:  "",
					}},
				},
				primaryKey: map[string]interface{}{
					"foo": "bar",
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := APITranslator{
				KeyspaceName: tt.fields.KeyspaceName,
				TableName:    tt.fields.TableName,
			}
			got, got1, err := a.ToUpdate(tt.args.rowsUpdate, tt.args.primaryKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToUpdate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ToUpdate() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ToUpdate() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestToInsert(t *testing.T) {
	type args struct {
		columns []m.Column
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		want1   []interface{}
		wantErr bool
	}{
		{
			name: "Happy Path",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				columns: []m.Column{{
					Name:  stringToPointer("id"),
					Value: "8be6d514-3436-4e04-a5fc-0ffbefa4c1fe",
				}, {
					Name:  stringToPointer("lastname"),
					Value: "Doe",
				}},
			},
			want:    `INSERT INTO company."employees" (id, lastname) VALUES (?, ?)`,
			want1:   []interface{}{"8be6d514-3436-4e04-a5fc-0ffbefa4c1fe", "Doe"},
			wantErr: false,
		}, {
			name: "Missing column for value",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				columns: []m.Column{{
					Name:  stringToPointer("id"),
					Value: stringToPointer("8be6d514-3436-4e04-a5fc-0ffbefa4c1fe"),
				}, {
					Value: stringToPointer("Doe"),
				}},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		}, {
			name: "Missing value for column",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				columns: []m.Column{{
					Name: stringToPointer("id"),
				}, {
					Name:  stringToPointer("lastname"),
					Value: stringToPointer("Doe"),
				}},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := APITranslator{
				KeyspaceName: tt.fields.KeyspaceName,
				TableName:    tt.fields.TableName,
			}
			got, got1, err := a.ToInsert(tt.args.columns)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToInsert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ToInsert() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ToInsert() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestToDelete(t *testing.T) {
	type args struct {
		primaryKey map[string]interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		want1   []interface{}
		wantErr bool
	}{
		{
			name: "Happy Path",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				primaryKey: map[string]interface{}{
					"id": "123",
				},
			},
			want:    `DELETE FROM company.employees WHERE id = ?`,
			want1:   []interface{}{"123"},
			wantErr: false,
		}, {
			name: "Missing primary key",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args:    args{},
			want:    "",
			want1:   nil,
			wantErr: true,
		}, {
			name: "Composite primary key",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				primaryKey: map[string]interface{}{
					"id":        123,
					"firstName": "Joe",
				},
			},
			want:    `DELETE FROM company.employees WHERE firstName = ? AND id = ?`,
			want1:   []interface{}{"Joe", 123},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := APITranslator{
				KeyspaceName: tt.fields.KeyspaceName,
				TableName:    tt.fields.TableName,
			}
			got, got1, err := a.ToDelete(tt.args.primaryKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToDelete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ToDelete() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ToDelete() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestToCreateTable(t *testing.T) {
	type args struct {
		table m.TableAdd
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Happy Path",
			fields: fields{
				KeyspaceName: "cycling",
			},
			args: args{
				table: m.TableAdd{
					Name:        "cyclist_name",
					IfNotExists: true,
					ColumnDefinitions: []m.ColumnDefinition{{
						Name:           "id",
						TypeDefinition: "uuid",
						Static:         false,
					}, {
						Name:           "lastname",
						TypeDefinition: "text",
						Static:         false,
					}, {
						Name:           "firstname",
						TypeDefinition: "text",
						Static:         false,
					}},
					PrimaryKey: &m.PrimaryKey{
						PartitionKey: []string{
							"id",
						},
					},
					TableOptions: nil,
				},
			},
			want:    `CREATE TABLE IF NOT EXISTS cycling.cyclist_name (id uuid, lastname text, firstname text, PRIMARY KEY (id))`,
			wantErr: false,
		}, {
			name: "Create with clustering order",
			fields: fields{
				KeyspaceName: "cycling",
			},
			args: args{
				table: m.TableAdd{
					Name:        "cyclist_category",
					IfNotExists: true,
					ColumnDefinitions: []m.ColumnDefinition{{
						Name:           "category",
						TypeDefinition: "text",
						Static:         false,
					}, {
						Name:           "points",
						TypeDefinition: "int",
						Static:         false,
					}, {
						Name:           "id",
						TypeDefinition: "uuid",
						Static:         false,
					}, {
						Name:           "lastname",
						TypeDefinition: "text",
						Static:         false,
					}},
					PrimaryKey: &m.PrimaryKey{
						PartitionKey: []string{
							"category",
							"points",
						},
					},
					TableOptions: &m.TableOptions{
						ClusteringExpression: []m.ClusteringExpression{{
							Column: stringToPointer("points"),
							Order:  stringToPointer("DESC"),
						}},
					},
				},
			},
			want:    `CREATE TABLE IF NOT EXISTS cycling.cyclist_category (category text, points int, id uuid, lastname text, PRIMARY KEY (category, points)) WITH CLUSTERING ORDER BY (points DESC)`,
			wantErr: false,
		}, {
			name: "Create with bad clustering order",
			fields: fields{
				KeyspaceName: "cycling",
			},
			args: args{
				table: m.TableAdd{
					Name:        "cyclist_category",
					IfNotExists: true,
					ColumnDefinitions: []m.ColumnDefinition{{
						Name:           "category",
						TypeDefinition: "text",
						Static:         false,
					}, {
						Name:           "points",
						TypeDefinition: "int",
						Static:         false,
					}, {
						Name:           "id",
						TypeDefinition: "uuid",
						Static:         false,
					}, {
						Name:           "lastname",
						TypeDefinition: "text",
						Static:         false,
					}},
					PrimaryKey: &m.PrimaryKey{
						PartitionKey: []string{
							"category",
							"points",
						},
					},
					TableOptions: &m.TableOptions{
						ClusteringExpression: []m.ClusteringExpression{{
							Column: stringToPointer("points"),
							Order:  stringToPointer("bad"),
						}},
					},
				},
			},
			want:    ``,
			wantErr: true,
		}, {
			name: "Create with bad type definition",
			fields: fields{
				KeyspaceName: "cycling",
			},
			args: args{
				table: m.TableAdd{
					Name:        "cyclist_category",
					IfNotExists: true,
					ColumnDefinitions: []m.ColumnDefinition{{
						Name:           "category",
						TypeDefinition: "text",
						Static:         false,
					}, {
						Name:           "points",
						TypeDefinition: "int",
						Static:         false,
					}, {
						Name:           "id",
						TypeDefinition: "uuid",
						Static:         false,
					}, {
						Name:           "lastname",
						TypeDefinition: "bad_type",
						Static:         false,
					}},
					PrimaryKey: &m.PrimaryKey{
						PartitionKey: []string{
							"category",
							"points",
						},
					},
					TableOptions: &m.TableOptions{
						ClusteringExpression: []m.ClusteringExpression{{
							Column: stringToPointer("points"),
							Order:  stringToPointer("asc"),
						}},
					},
				},
			},
			want:    ``,
			wantErr: true,
		}, {
			name: "Create with multiple clustering order columns",
			fields: fields{
				KeyspaceName: "cycling",
			},
			args: args{
				table: m.TableAdd{
					Name:        "cyclist_category",
					IfNotExists: true,
					ColumnDefinitions: []m.ColumnDefinition{{
						Name:           "category",
						TypeDefinition: "text",
						Static:         false,
					}, {
						Name:           "points",
						TypeDefinition: "int",
						Static:         false,
					}, {
						Name:           "id",
						TypeDefinition: "uuid",
						Static:         false,
					}, {
						Name:           "lastname",
						TypeDefinition: "text",
						Static:         false,
					}},
					PrimaryKey: &m.PrimaryKey{
						PartitionKey: []string{
							"category",
							"points",
						},
					},
					TableOptions: &m.TableOptions{
						ClusteringExpression: []m.ClusteringExpression{{
							Column: stringToPointer("points"),
							Order:  stringToPointer("DESC"),
						}, {
							Column: stringToPointer("category"),
							Order:  stringToPointer("ASC"),
						}},
					},
				},
			},
			want:    `CREATE TABLE IF NOT EXISTS cycling.cyclist_category (category text, points int, id uuid, lastname text, PRIMARY KEY (category, points)) WITH CLUSTERING ORDER BY (points DESC, category ASC)`,
			wantErr: false,
		}, {
			name: "Create with ttl",
			fields: fields{
				KeyspaceName: "some_keyspace",
			},
			args: args{
				table: m.TableAdd{
					Name:        "some_table",
					IfNotExists: true,
					ColumnDefinitions: []m.ColumnDefinition{{
						Name:           "key",
						TypeDefinition: "text",
						Static:         false,
					}, {
						Name:           "value",
						TypeDefinition: "text",
						Static:         false,
					}},
					PrimaryKey: &m.PrimaryKey{
						PartitionKey: []string{
							"key",
						},
					},
					TableOptions: &m.TableOptions{
						DefaultTimeToLive: intToPointer(180),
					},
				},
			},
			want:    `CREATE TABLE IF NOT EXISTS some_keyspace.some_table (key text, value text, PRIMARY KEY (key)) WITH default_time_to_live = 180`,
			wantErr: false,
		}, {
			name: "With clustering key",
			fields: fields{
				KeyspaceName: "cycling",
			},
			args: args{
				table: m.TableAdd{
					Name:        "cyclist_name",
					IfNotExists: true,
					ColumnDefinitions: []m.ColumnDefinition{{
						Name:           "id",
						TypeDefinition: "uuid",
						Static:         false,
					}, {
						Name:           "lastname",
						TypeDefinition: "text",
						Static:         false,
					}, {
						Name:           "firstname",
						TypeDefinition: "text",
						Static:         false,
					}},
					PrimaryKey: &m.PrimaryKey{
						PartitionKey: []string{
							"id",
						},
						ClusteringKey: []string{
							"firstname",
						},
					},
					TableOptions: nil,
				},
			},
			want:    `CREATE TABLE IF NOT EXISTS cycling.cyclist_name (id uuid, lastname text, firstname text, PRIMARY KEY ((id), firstname))`,
			wantErr: false,
		}, {
			name: "Missing table name",
			fields: fields{
				KeyspaceName: "cycling",
			},
			args: args{
				table: m.TableAdd{
					IfNotExists: true,
					ColumnDefinitions: []m.ColumnDefinition{{
						Name:           "id",
						TypeDefinition: "uuid",
						Static:         false,
					}, {
						Name:           "lastname",
						TypeDefinition: "text",
						Static:         false,
					}, {
						Name:           "firstname",
						TypeDefinition: "text",
						Static:         false,
					}},
					PrimaryKey: &m.PrimaryKey{
						PartitionKey: []string{
							"id",
						},
					},
					TableOptions: nil,
				},
			},
			want:    "",
			wantErr: true,
		}, {
			name: "Missing column name",
			fields: fields{
				KeyspaceName: "cycling",
			},
			args: args{
				table: m.TableAdd{
					Name:        "cyclist_name",
					IfNotExists: true,
					ColumnDefinitions: []m.ColumnDefinition{{
						TypeDefinition: "uuid",
						Static:         false,
					}, {
						Name:           "lastname",
						TypeDefinition: "text",
						Static:         false,
					}, {
						Name:           "firstname",
						TypeDefinition: "text",
						Static:         false,
					}},
					PrimaryKey: &m.PrimaryKey{
						PartitionKey: []string{
							"id",
						},
					},
					TableOptions: nil,
				},
			},
			want:    "",
			wantErr: true,
		}, {
			name: "Missing cql definition",
			fields: fields{
				KeyspaceName: "cycling",
			},
			args: args{
				table: m.TableAdd{
					Name:        "cyclist_name",
					IfNotExists: true,
					ColumnDefinitions: []m.ColumnDefinition{{
						Name:           "id",
						TypeDefinition: "uuid",
						Static:         false,
					}, {
						Name:   "lastname",
						Static: false,
					}, {
						Name:           "firstname",
						TypeDefinition: "text",
						Static:         false,
					}},
					PrimaryKey: &m.PrimaryKey{
						PartitionKey: []string{
							"id",
						},
					},
					TableOptions: nil,
				},
			},
			want:    "",
			wantErr: true,
		}, {
			name: "Missing primary key",
			fields: fields{
				KeyspaceName: "cycling",
			},
			args: args{
				table: m.TableAdd{
					Name:        "table_name",
					IfNotExists: true,
					ColumnDefinitions: []m.ColumnDefinition{{
						Name:           "id",
						TypeDefinition: "uuid",
						Static:         false,
					}, {
						Name:           "lastname",
						TypeDefinition: "text",
						Static:         false,
					}, {
						Name:           "firstname",
						TypeDefinition: "text",
						Static:         false,
					}},
					TableOptions: nil,
				},
			},
			want:    "",
			wantErr: true,
		},
	}
	dmp := diffmatchpatch.New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := APITranslator{
				KeyspaceName: tt.fields.KeyspaceName,
				TableName:    tt.fields.TableName,
			}
			got, err := a.ToCreateTable(tt.args.table)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToCreateTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.want != got {
				diffs := dmp.DiffMain(tt.want, got, false)
				fmt.Println(dmp.DiffPrettyText(diffs))
				t.Errorf("ToCreateTable() got = '%v', want '%v'", got, tt.want)
			}
		})
	}
}

func TestToDropTable(t *testing.T) {
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		{
			name: "Happy Path",
			fields: fields{
				KeyspaceName: "foo",
				TableName:    "bar",
			},
			want:    `DROP TABLE foo.bar`,
			wantErr: false,
		}, {
			name: "Missing keyspaceName",
			fields: fields{
				TableName: "bar",
			},
			want:    "",
			wantErr: true,
		}, {
			name: "Missing tableName",
			fields: fields{
				KeyspaceName: "foo",
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := APITranslator{
				KeyspaceName: tt.fields.KeyspaceName,
				TableName:    tt.fields.TableName,
			}
			got, err := a.ToDropTable()
			if (err != nil) != tt.wantErr {
				t.Errorf("TestToDropTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.want != got {
				t.Errorf("TestToDropTable() got = '%v', want '%v'", got, tt.want)
			}
		})
	}
}

func TestAstraAPITranslator_ToAlterTableAddColumn(t *testing.T) {
	type fields struct {
		KeyspaceName string
		TableName    string
	}
	type args struct {
		columnDefinition m.ColumnDefinition
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Happy Path",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				columnDefinition: m.ColumnDefinition{
					Name:           "firstName",
					TypeDefinition: "text",
					Static:         false,
				},
			},
			want:    "ALTER TABLE company.employees ADD (firstName text)",
			wantErr: false,
		}, {
			name: "Missing table name",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "",
			},
			args: args{
				columnDefinition: m.ColumnDefinition{
					Name:           "firstName",
					TypeDefinition: "text",
					Static:         false,
				},
			},
			want:    "",
			wantErr: true,
		}, {
			name: "With static",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				columnDefinition: m.ColumnDefinition{
					Name:           "firstName",
					TypeDefinition: "text",
					Static:         true,
				},
			},
			want:    "ALTER TABLE company.employees ADD (firstName text STATIC)",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := APITranslator{
				KeyspaceName: tt.fields.KeyspaceName,
				TableName:    tt.fields.TableName,
			}
			got, err := a.ToAlterTableAddColumn(tt.args.columnDefinition)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToAlterTableAddColumn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ToAlterTableAddColumn() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAstraAPITranslator_ToAlterTableDeleteColumn(t *testing.T) {
	type fields struct {
		KeyspaceName string
		TableName    string
	}
	type args struct {
		columnName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Happy Path",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				columnName: "lastname",
			},
			want:    "ALTER TABLE company.employees DROP (lastname)",
			wantErr: false,
		}, {
			name: "Missing table",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "",
			},
			args: args{
				columnName: "lastname",
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := APITranslator{
				KeyspaceName: tt.fields.KeyspaceName,
				TableName:    tt.fields.TableName,
			}
			got, err := a.ToAlterTableDeleteColumn(tt.args.columnName)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToAlterTableDeleteColumn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ToAlterTableDeleteColumn() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAstraAPITranslator_ToAlterTableUpdateColumn(t *testing.T) {
	type fields struct {
		KeyspaceName string
		TableName    string
	}
	type args struct {
		oldColumn string
		newColumn string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Happy Path",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				oldColumn: "name",
				newColumn: "firstName",
			},
			want:    "ALTER TABLE company.employees RENAME name TO firstName",
			wantErr: false,
		}, {
			name: "Missing table",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "",
			},
			args: args{
				oldColumn: "name",
				newColumn: "firstName",
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := APITranslator{
				KeyspaceName: tt.fields.KeyspaceName,
				TableName:    tt.fields.TableName,
			}
			got, err := a.ToAlterTableUpdateColumn(tt.args.oldColumn, tt.args.newColumn)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToAlterTableUpdateColumn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ToAlterTableUpdateColumn() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAstraAPITranslator_ToSelectFromQuery(t *testing.T) {
	type fields struct {
		KeyspaceName string
		TableName    string
	}
	type args struct {
		queryModel m.Query
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		want1   []interface{}
		wantErr bool
	}{
		{
			name: "Happy Path",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				queryModel: m.Query{
					ColumnNames: []string{"id"},
					Filters: []m.Filter{{
						ColumnName: "id",
						Operator:   "=",
						Value:      []interface{}{"8be6d514-3436-4e04-a5fc-0ffbefa4c1fe"},
					}},
					OrderBy: &m.ClusteringExpression{
						Column: stringToPointer("id"),
						Order:  stringToPointer("ASC"),
					},
				},
			},
			want:    `SELECT id FROM company.employees WHERE id = ? ORDER BY id ASC`,
			want1:   []interface{}{"8be6d514-3436-4e04-a5fc-0ffbefa4c1fe"},
			wantErr: false,
		}, {
			name: "Multiple Columns",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				queryModel: m.Query{
					ColumnNames: []string{"id", "firstname", "lastname"},
					Filters: []m.Filter{{
						ColumnName: "id",
						Operator:   "=",
						Value:      []interface{}{"8be6d514-3436-4e04-a5fc-0ffbefa4c1fe"},
					}},
					OrderBy: &m.ClusteringExpression{
						Column: stringToPointer("id"),
						Order:  stringToPointer("ASC"),
					},
				},
			},
			want:    `SELECT id,firstname,lastname FROM company.employees WHERE id = ? ORDER BY id ASC`,
			want1:   []interface{}{"8be6d514-3436-4e04-a5fc-0ffbefa4c1fe"},
			wantErr: false,
		}, {
			name: "No columns",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				queryModel: m.Query{
					Filters: []m.Filter{{
						ColumnName: "id",
						Operator:   "=",
						Value:      []interface{}{"8be6d514-3436-4e04-a5fc-0ffbefa4c1fe"},
					}},
					OrderBy: &m.ClusteringExpression{
						Column: stringToPointer("id"),
						Order:  stringToPointer("ASC"),
					},
				},
			},
			want:    `SELECT * FROM company.employees WHERE id = ? ORDER BY id ASC`,
			want1:   []interface{}{"8be6d514-3436-4e04-a5fc-0ffbefa4c1fe"},
			wantErr: false,
		}, {
			name: "In Operator",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				queryModel: m.Query{
					ColumnNames: []string{"id"},
					Filters: []m.Filter{{
						ColumnName: "id",
						Operator:   "in",
						Value:      []interface{}{"123", "456", "789"},
					}},
					OrderBy: &m.ClusteringExpression{
						Column: stringToPointer("id"),
						Order:  stringToPointer("ASC"),
					},
				},
			},
			want:    `SELECT id FROM company.employees WHERE id in (?,?,?) ORDER BY id ASC`,
			want1:   []interface{}{"123", "456", "789"},
			wantErr: false,
		}, {
			name: "No order by",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				queryModel: m.Query{
					ColumnNames: []string{"id"},
					Filters: []m.Filter{{
						ColumnName: "id",
						Operator:   "=",
						Value:      []interface{}{"8be6d514-3436-4e04-a5fc-0ffbefa4c1fe"},
					}},
				},
			},
			want:    `SELECT id FROM company.employees WHERE id = ?`,
			want1:   []interface{}{"8be6d514-3436-4e04-a5fc-0ffbefa4c1fe"},
			wantErr: false,
		}, {
			name: "Greater than and IN operator",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				queryModel: m.Query{
					ColumnNames: []string{"id"},
					Filters: []m.Filter{{
						ColumnName: "id",
						Operator:   "in",
						Value:      []interface{}{"123", "456", "789"},
					}, {
						ColumnName: "age",
						Operator:   "gt",
						Value:      []interface{}{"34"},
					}},
				},
			},
			want:    `SELECT id FROM company.employees WHERE id in (?,?,?) AND age > ?`,
			want1:   []interface{}{"123", "456", "789", "34"},
			wantErr: false,
		}, {
			name: "Bad orderby",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				queryModel: m.Query{
					ColumnNames: []string{"id"},
					Filters: []m.Filter{{
						ColumnName: "id",
						Operator:   "in",
						Value:      []interface{}{"123", "456", "789"},
					}, {
						ColumnName: "age",
						Operator:   "gt",
						Value:      []interface{}{"34"},
					}},
					OrderBy: &m.ClusteringExpression{
						Column: nil,
						Order:  stringToPointer("asc"),
					},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		}, {
			name: "Missing filter",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				queryModel: m.Query{
					ColumnNames: []string{"id"},
					Filters: []m.Filter{{
						ColumnName: "id",
						Operator:   "in",
						Value:      []interface{}{"123", "456", "789"},
					}, {
						ColumnName: "age",
						Operator:   "gt",
						Value:      []interface{}{"34"},
					}},
					OrderBy: &m.ClusteringExpression{
						Column: nil,
						Order:  stringToPointer("asc"),
					},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		}, {
			name: "Complex operator",
			fields: fields{
				KeyspaceName: "company",
				TableName:    "employees",
			},
			args: args{
				queryModel: m.Query{
					ColumnNames: []string{"id", "foo"},
					Filters: []m.Filter{{
						ColumnName: "id",
						Operator:   "in",
						Value:      []interface{}{"123", "456", "789"},
					}, {
						ColumnName: "age",
						Operator:   "gt",
						Value:      []interface{}{"34"},
					}, {
						ColumnName: "lastname",
						Operator:   "notEq",
						Value:      []interface{}{"smith"},
					}, {
						ColumnName: "date",
						Operator:   "gt",
						Value:      []interface{}{"2020-03-03"},
					}, {
						ColumnName: "date",
						Operator:   "lte",
						Value:      []interface{}{"2020-04-03"},
					}},
				},
			},
			want:    `SELECT id,foo FROM company.employees WHERE id in (?,?,?) AND age > ? AND lastname != ? AND date > ? AND date <= ?`,
			want1:   []interface{}{"123", "456", "789", "34", "smith", "2020-03-03", "2020-04-03"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := APITranslator{
				KeyspaceName: tt.fields.KeyspaceName,
				TableName:    tt.fields.TableName,
			}
			got, got1, err := a.ToSelectFromQuery(tt.args.queryModel)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToSelectFromQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ToSelectFromQuery() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ToSelectFromQuery() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
