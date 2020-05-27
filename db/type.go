package db

import (
    "fmt"
    "github.com/gocql/gocql"
    "strings"
)

type CreateTypeInfo struct {
    Keyspace       string
    Name           string
    Values         []*gocql.ColumnMetadata
    IfNotExists    bool
}

type AlterTypeAddInfo struct {
    Keyspace       string
    Name           string
    Values         []*gocql.ColumnMetadata
}

type AlterTypeRenameInfo struct {
    Keyspace       string
    Name           string
    Rename         []*AlterTypeRenameItem
}

type AlterTypeRenameItem struct {
    From    string
    To      string
}

type DropTypeInfo struct {
    Keyspace       string
    Name           string
    IfExists       bool
}

func (db *Db) CreateType(info *CreateTypeInfo, options *QueryOptions) error {
    columns := []string{}

    for _, c := range info.Values {
        columns = append(columns, fmt.Sprintf(`"%s" %s`, c.Name, toTypeString(c.Type)))
    }

    query := fmt.Sprintf(`CREATE TYPE %s"%s"."%s" (%s)`,
        ifNotExistsStr(info.IfNotExists), info.Keyspace, info.Name, strings.Join(columns, ", "))

    return db.session.Execute(query, options)
}

func (db *Db) AlterTypeAdd(info *AlterTypeAddInfo, options *QueryOptions) error {
    columns := []string{}
    for _, c := range info.Values {
        columns = append(columns, fmt.Sprintf(`"%s" %s`, c.Name, toTypeString(c.Type)))
    }

    query := fmt.Sprintf(`ALTER TYPE "%s"."%s" ADD %s`,
        info.Keyspace, info.Name, strings.Join(columns, ","))

    return db.session.Execute(query, options)
}

func (db *Db) AlterTypeRename(info *AlterTypeRenameInfo, options *QueryOptions) error {
    columns := []string{}
    for _, c := range info.Rename {
        columns = append(columns, fmt.Sprintf(`"%s" TO "%s"`, c.From, c.To))
    }

    query := fmt.Sprintf(`ALTER TYPE "%s"."%s" RENAME %s`,
        info.Keyspace, info.Name, strings.Join(columns, " AND "))

    fmt.Print(query)
    return db.session.Execute(query, options)
}

func (db *Db) DropType(info *DropTypeInfo, options *QueryOptions) error {
    query := fmt.Sprintf(`DROP TYPE %s"%s"."%s"`,
        ifExistsStr(info.IfExists), info.Keyspace, info.Name)

    return db.session.Execute(query, options)
}