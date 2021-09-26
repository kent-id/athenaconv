package athenaconv

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/kent-id/athenaconv/util"
)

func castAthenaRowData(ctx context.Context, rowData types.Datum, athenaType string) (interface{}, error) {
	data := util.SafeString(rowData.VarCharValue)

	var castedData interface{} = nil
	var err error = nil

	// for supported data types, see https://docs.aws.amazon.com/athena/latest/ug/data-types.html
	fmt.Println("athenaType", athenaType)
	switch athenaType {
	case "boolean":
		castedData = strings.ToLower(data) == "true"
	case "varchar":
		castedData = data
	case "integer":
		castedData, err = strconv.Atoi(data)
	case "bigint":
		castedData, err = strconv.ParseInt(data, 10, 64)
	case "array":
		arrayValueString := strings.Trim(data, "[]")
		newStringSlice := make([]string, 0)
		if len(arrayValueString) > 0 {
			arrayValue := strings.Split(arrayValueString, ", ")
			newStringSlice = append(newStringSlice, arrayValue...)
		}
		castedData = newStringSlice
	case "timestamp":
		castedData, err = time.Parse("2006-01-02 15:04:05", data)
	case "date":
		castedData, err = time.Parse("2006-01-02", data)
	default:
		fmt.Printf("ATHENA DATA TYPE NOT SUPPORTED: '%s', defaulting to string\n", athenaType)
		castedData = data
	}

	return castedData, err
}
