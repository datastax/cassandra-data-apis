package db

import (
	"fmt"
	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
	"reflect"
)

var typeForCqlType = map[gocql.Type]reflect.Type{
	gocql.TypeFloat:    reflect.TypeOf(float32(0)),
	gocql.TypeDouble:   reflect.TypeOf(float64(0)),
	gocql.TypeInt:      reflect.TypeOf(0),
	gocql.TypeSmallInt: reflect.TypeOf(int16(0)),
	gocql.TypeTinyInt:  reflect.TypeOf(int8(0)),
	gocql.TypeBigInt:   reflect.TypeOf("0"),
	gocql.TypeCounter:  reflect.TypeOf("0"),
	gocql.TypeDecimal:  reflect.TypeOf(new(inf.Dec)),
	gocql.TypeText:     reflect.TypeOf("0"),
	gocql.TypeVarchar:  reflect.TypeOf("0"),
	gocql.TypeAscii:    reflect.TypeOf("0"),
	gocql.TypeInet:     reflect.TypeOf("0"),
	gocql.TypeBoolean:  reflect.TypeOf(false),
}

func mapScan(scanner gocql.Scanner, columns []gocql.ColumnInfo) (map[string]interface{}, error) {
	values := make([]interface{}, len(columns))

	for i := range values {
		typeInfo := columns[i].TypeInfo
		allocated := allocateForType(typeInfo)
		if allocated == nil {
			return nil, fmt.Errorf("Support for CQL type not found: %s", typeInfo.Type().String())
		}
		values[i] = allocated
	}

	if err := scanner.Scan(values...); err != nil {
		return nil, err
	}

	mapped := make(map[string]interface{}, len(values))
	for i, column := range columns {
		value := values[i]
		switch column.TypeInfo.Type() {
		case gocql.TypeVarchar, gocql.TypeAscii, gocql.TypeInet, gocql.TypeText,
			gocql.TypeBigInt, gocql.TypeInt, gocql.TypeSmallInt, gocql.TypeTinyInt,
			gocql.TypeCounter, gocql.TypeBoolean,
			gocql.TypeTimeUUID, gocql.TypeUUID,
			gocql.TypeFloat, gocql.TypeDouble,
			gocql.TypeDecimal:
			value = reflect.Indirect(reflect.ValueOf(value)).Interface()
		}

		mapped[column.Name] = value
	}

	return mapped, nil
}

func allocateForType(info gocql.TypeInfo) interface{} {
	switch info.Type() {
	case gocql.TypeVarchar, gocql.TypeAscii, gocql.TypeInet, gocql.TypeText:
		return new(*string)
	case gocql.TypeBigInt, gocql.TypeCounter:
		// We try to use types that have graphql/json representation
		return new(*string)
	case gocql.TypeDecimal:
		return new(*inf.Dec)
	case gocql.TypeBoolean:
		return new(*bool)
	case gocql.TypeFloat:
		// Mapped to a json Number
		return new(*float32)
	case gocql.TypeDouble:
		// Mapped to a json Number
		return new(*float64)
	case gocql.TypeInt:
		return new(*int)
	case gocql.TypeSmallInt:
		return new(*int16)
	case gocql.TypeTinyInt:
		return new(*int8)
	case gocql.TypeTimeUUID, gocql.TypeUUID:
		return new(*gocql.UUID)
	case gocql.TypeList, gocql.TypeSet:
		listInfo, ok := info.(gocql.CollectionType)
		if !ok {
			return nil
		}
		t, typeFound := typeForCqlType[listInfo.Elem.Type()]

		if !typeFound {
			// Create the subtype by allocating it
			t = reflect.TypeOf(allocateForType(listInfo.Elem))
		}
		return reflect.New(reflect.SliceOf(t)).Interface()
	default:
		return nil
	}
}
