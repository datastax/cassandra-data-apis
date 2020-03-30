package db

import (
	"fmt"
	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
	"math/big"
	"reflect"
)

var typeForCqlType = map[gocql.Type]reflect.Type{
	gocql.TypeFloat:     reflect.TypeOf(float32(0)),
	gocql.TypeDouble:    reflect.TypeOf(float64(0)),
	gocql.TypeInt:       reflect.TypeOf(0),
	gocql.TypeSmallInt:  reflect.TypeOf(int16(0)),
	gocql.TypeTinyInt:   reflect.TypeOf(int8(0)),
	gocql.TypeBigInt:    reflect.TypeOf("0"),
	gocql.TypeCounter:   reflect.TypeOf("0"),
	gocql.TypeDecimal:   reflect.TypeOf(new(inf.Dec)),
	gocql.TypeVarint:    reflect.TypeOf(new(big.Int)),
	gocql.TypeText:      reflect.TypeOf("0"),
	gocql.TypeVarchar:   reflect.TypeOf("0"),
	gocql.TypeAscii:     reflect.TypeOf("0"),
	gocql.TypeBoolean:   reflect.TypeOf(false),
	gocql.TypeInet:      reflect.TypeOf("0"),
	gocql.TypeUUID:      reflect.TypeOf("0"),
	gocql.TypeTimeUUID:  reflect.TypeOf("0"),
	gocql.TypeTimestamp: reflect.TypeOf("0"),
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
			gocql.TypeDecimal, gocql.TypeVarint:
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
	case gocql.TypeDecimal:
		return new(*inf.Dec)
	case gocql.TypeVarint:
		return new(*big.Int)
	case gocql.TypeTimeUUID, gocql.TypeUUID:
		// Mapped to a json string
		return new(*string)
	case gocql.TypeList, gocql.TypeSet:
		subTypeInfo, ok := info.(gocql.CollectionType)
		if !ok {
			return nil
		}

		var subType reflect.Type
		if subType = getSubType(subTypeInfo.Elem); subType == nil {
			return nil
		}

		return reflect.New(reflect.SliceOf(subType)).Interface()
	case gocql.TypeMap:
		subTypeInfo, ok := info.(gocql.CollectionType)
		if !ok {
			return nil
		}

		var keyType reflect.Type
		var valueType reflect.Type
		if keyType = getSubType(subTypeInfo.Key); keyType == nil {
			return nil
		}
		if valueType = getSubType(subTypeInfo.Elem); valueType == nil {
			return nil
		}

		return reflect.New(reflect.MapOf(keyType, valueType)).Interface()
	default:
		return nil
	}
}

func getSubType(info gocql.TypeInfo) reflect.Type {
	t, typeFound := typeForCqlType[info.Type()]

	if !typeFound {
		// Create the subtype by allocating it
		allocated := allocateForType(info)

		if allocated == nil {
			return nil
		}

		t = reflect.ValueOf(allocated).Elem().Type()
	}

	return t
}
