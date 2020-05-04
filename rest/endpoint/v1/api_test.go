package endpoint

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/gocql/gocql"
	inf "gopkg.in/inf.v0"
)

func Test_getCQLType(t *testing.T) {
	d := new(inf.Dec)
	d.SetString(fmt.Sprintf("%f", 3.14))

	type args struct {
		typeInfo gocql.TypeInfo
		val      interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "Text Type",
			args: args{
				typeInfo: gocql.NewNativeType(0, gocql.TypeText, ""),
				val:      "foo",
			},
			want:    "foo",
			wantErr: false,
		},
		{
			name: "Decimal Type",
			args: args{
				typeInfo: gocql.NewNativeType(0, gocql.TypeDecimal, ""),
				val:      3.14,
			},
			want:    d,
			wantErr: false,
		},
		{
			name: "Decimal Type as string",
			args: args{
				typeInfo: gocql.NewNativeType(0, gocql.TypeDecimal, ""),
				val:      "foo",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Decimal Type as string number",
			args: args{
				typeInfo: gocql.NewNativeType(0, gocql.TypeDecimal, ""),
				val:      "3.14",
			},
			want:    d,
			wantErr: false,
		},
		{
			name: "Int Type",
			args: args{
				typeInfo: gocql.NewNativeType(0, gocql.TypeInt, ""),
				val:      2,
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "Float Type",
			args: args{
				typeInfo: gocql.NewNativeType(0, gocql.TypeFloat, ""),
				val:      2.25,
			},
			want:    2.25,
			wantErr: false,
		},
		{
			name: "Int Type as string number",
			args: args{
				typeInfo: gocql.NewNativeType(0, gocql.TypeInt, ""),
				val:      "123",
			},
			want:    123,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getCQLType(tt.args.typeInfo, tt.args.val)
			if (err != nil) != tt.wantErr {
				t.Errorf("getCQLType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getCQLType() got = %v, want %v", got, tt.want)
			}
		})
	}
}
