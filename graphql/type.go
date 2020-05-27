package graphql

import (
    "github.com/datastax/cassandra-data-apis/db"
    "github.com/graphql-go/graphql"
    "github.com/mitchellh/mapstructure"
)

var alterTypeRenameInput = graphql.NewInputObject(graphql.InputObjectConfig{
    Name: "AlterTypeRenameInput",
    Fields: graphql.InputObjectConfigFieldMap{
        "from": {Type: graphql.NewNonNull(graphql.String)},
        "to": {Type: graphql.NewNonNull(graphql.String)},
    },
})

func (sg *SchemaGenerator) createType(params graphql.ResolveParams) (interface{}, error) {
    args := params.Args
    ksName := args["keyspaceName"].(string)
    name := args["name"].(string)

    values, err := decodeColumns(args["values"].([]interface{}));
    if err != nil {
        return nil, err
    }

    userOrRole, err := sg.checkUserOrRoleAuth(params)
    if err != nil {
        return nil, err
    }

    ifNotExists := getBoolArg(args, "ifNotExists")

    err = sg.dbClient.CreateType(&db.CreateTypeInfo{
        Keyspace:       ksName,
        Name:           name,
        Values:         values,
        IfNotExists:    ifNotExists,
    }, db.NewQueryOptions().WithUserOrRole(userOrRole).WithContext(params.Context))
    return err != nil, err
}

func (sg *SchemaGenerator) alterTypeAdd(params graphql.ResolveParams) (interface{}, error) {
    args := params.Args
    ksName := args["keyspaceName"].(string)
    name := args["name"].(string)

    values, err := decodeColumns(args["values"].([]interface{}));
    if err != nil {
        return nil, err
    }

    userOrRole, err := sg.checkUserOrRoleAuth(params)
    if err != nil {
        return nil, err
    }

    err = sg.dbClient.AlterTypeAdd(&db.AlterTypeAddInfo{
        Keyspace:       ksName,
        Name:            name,
        Values:         values,
    }, db.NewQueryOptions().WithUserOrRole(userOrRole).WithContext(params.Context))
    return err != nil, err
}

func (sg *SchemaGenerator) alterTypeRename(params graphql.ResolveParams) (interface{}, error) {
    args := params.Args
    ksName := args["keyspaceName"].(string)
    name := args["name"].(string)

    userOrRole, err := sg.checkUserOrRoleAuth(params)
    if err != nil {
        return nil, err
    }

    rename, err := decodeRenameColumns(args["rename"].([]interface{}));
    if err != nil {
        return nil, err
    }

    err = sg.dbClient.AlterTypeRename(&db.AlterTypeRenameInfo{
        Keyspace:       ksName,
        Name:           name,
        Rename:         rename,
    }, db.NewQueryOptions().WithUserOrRole(userOrRole).WithContext(params.Context))
    return err != nil, err
}

func (sg *SchemaGenerator) dropType(params graphql.ResolveParams) (interface{}, error) {
    args := params.Args
    ksName := args["keyspaceName"].(string)
    name := args["name"].(string)

    userOrRole, err := sg.checkUserOrRoleAuth(params)
    if err != nil {
        return nil, err
    }

    ifExists := getBoolArg(args, "ifExists")

    err = sg.dbClient.DropType(&db.DropTypeInfo{
        Keyspace:       ksName,
        Name:           name,
        IfExists:       ifExists,
    }, db.NewQueryOptions().WithUserOrRole(userOrRole).WithContext(params.Context))
    return err != nil, err
}

func decodeRenameColumns(columns []interface{}) ([]*db.AlterTypeRenameItem, error) {
    renameColumns := make([]*db.AlterTypeRenameItem, 0)
    for _, column := range columns {
        var value db.AlterTypeRenameItem
        if err := mapstructure.Decode(column, &value); err != nil {
            return nil, err
        }

        renameColumns = append(renameColumns, &value)
    }
    return renameColumns, nil
}