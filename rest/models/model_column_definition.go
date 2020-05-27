package models

import (
	"fmt"
	"github.com/gocql/gocql"
)

// ColumnDefinition defines a column to be added to a table
type ColumnDefinition struct {
	// Name is a unique name for the column.
	Name string `json:"name" validate:"required"`

	// TypeDefinition defines the type of data allowed in the column
	TypeDefinition string `json:"typeDefinition" validate:"required,oneof=ascii text varchar tinyint smallint int bigint varint decimal float double date duration time timestamp uuid timeuuid blob boolean counter inet"`

	// Denotes that the column is shared by all rows of a partition
	Static bool `json:"static,omitempty"`
}

// toDbType gets a gocql data type for the provided string
func toDbType(typeDefinition string) (gocql.TypeInfo, error) {
	var t gocql.Type
	switch typeDefinition {
	case gocql.TypeCustom.String():
		t = gocql.TypeCustom
	case gocql.TypeAscii.String():
		t = gocql.TypeAscii
	case gocql.TypeBigInt.String():
		t = gocql.TypeBigInt
	case gocql.TypeBlob.String():
		t = gocql.TypeBlob
	case gocql.TypeBoolean.String():
		t = gocql.TypeBoolean
	case gocql.TypeCounter.String():
		t = gocql.TypeCounter
	case gocql.TypeDecimal.String():
		t = gocql.TypeDecimal
	case gocql.TypeDouble.String():
		t = gocql.TypeDouble
	case gocql.TypeFloat.String():
		t = gocql.TypeFloat
	case gocql.TypeInt.String():
		t = gocql.TypeInt
	case gocql.TypeText.String():
		t = gocql.TypeText
	case gocql.TypeTimestamp.String():
		t = gocql.TypeTimestamp
	case gocql.TypeUUID.String():
		t = gocql.TypeUUID
	case gocql.TypeVarchar.String():
		t = gocql.TypeVarchar
	case gocql.TypeTimeUUID.String():
		t = gocql.TypeTimeUUID
	case gocql.TypeInet.String():
		t = gocql.TypeInet
	case gocql.TypeDate.String():
		t = gocql.TypeDate
	case gocql.TypeDuration.String():
		t = gocql.TypeDuration
	case gocql.TypeTime.String():
		t = gocql.TypeTime
	case gocql.TypeSmallInt.String():
		t = gocql.TypeSmallInt
	case gocql.TypeTinyInt.String():
		t = gocql.TypeTinyInt
	case gocql.TypeVarint.String():
		t = gocql.TypeVarint
	default:
		return nil, fmt.Errorf("type '%s' Not supported", typeDefinition)
	}

	return gocql.NewNativeType(0, t, ""), nil
}

// ToDbColumn gets a gocql column for the provided definition
func ToDbColumn(definition ColumnDefinition) (*gocql.ColumnMetadata, error) {
	kind := gocql.ColumnRegular
	if definition.Static {
		kind = gocql.ColumnStatic
	}

	dbType, err := toDbType(definition.TypeDefinition)
	if err != nil {
		return nil, err
	}

	return &gocql.ColumnMetadata{
		Name: definition.Name,
		Kind: kind,
		Type: dbType,
	}, nil
}
