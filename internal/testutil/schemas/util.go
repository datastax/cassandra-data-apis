package schemas

import (
	"bytes"
	"encoding/json"
	"github.com/iancoleman/strcase"
	. "github.com/onsi/gomega"
)

type ResponseBody struct {
	Data   map[string]interface{} `json:"data"`
	Errors []ErrorEntry           `json:"errors"`
}

type ErrorEntry struct {
	Message   string   `json:"message"`
	Path      []string `json:"path"`
	Locations []struct {
		Line   int `json:"line"`
		Column int `json:"column"`
	} `json:"locations"`
}

func DecodeResponse(buffer *bytes.Buffer) ResponseBody {
	var response ResponseBody
	err := json.NewDecoder(buffer).Decode(&response)
	Expect(err).ToNot(HaveOccurred())
	return response
}

func DecodeData(buffer *bytes.Buffer, key string) map[string]interface{} {
	return DecodeResponse(buffer).Data[key].(map[string]interface{})
}

func DecodeDataAsSliceOfMaps(buffer *bytes.Buffer, key string, property string) []map[string]interface{} {
	arr := DecodeData(buffer, key)[property].([]interface{})
	result := make([]map[string]interface{}, 0, len(arr))
	for _, item := range arr {
		result = append(result, item.(map[string]interface{}))
	}
	return result
}

func NewResponseBody(operationName string, elementMap map[string]interface{}) ResponseBody {
	return ResponseBody{
		Data: map[string]interface{}{
			operationName: elementMap,
		},
	}
}

func GetTypeNamesByTable(tableName string) []string {
	baseName := strcase.ToCamel(tableName)
	return []string{
		baseName + "Input",
		baseName + "FilterInput",
		baseName,
		baseName + "Result",
		baseName + "Order",
		baseName + "MutationResult",
	}
}
