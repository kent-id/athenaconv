package athenaconv

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/kent-id/athenaconv/util"
)

func castAthenaRowData(ctx context.Context, rowData types.Datum, athenaType string, destKind reflect.Kind) (interface{}, error) {
	data := util.SafeString(rowData.VarCharValue)
	if athenaType != "varchar" && data == "" && destKind == reflect.Ptr {
		return nil, nil
	}

	// for supported data types, see https://docs.aws.amazon.com/athena/latest/ug/data-types.html
	switch athenaType {
	case "boolean":
		v, err := strconv.ParseBool(data)
		if destKind == reflect.Ptr {
			return &v, err
		}
		return v, err
	case "integer":
		v, err := strconv.Atoi(data)
		if destKind == reflect.Ptr {
			return &v, err
		}
		return v, err
	case "bigint":
		v, err := strconv.ParseInt(data, 10, 64)
		if destKind == reflect.Ptr {
			return &v, err
		}
		return v, err
	case "array":
		arrayValueString := strings.Trim(data, "[]")
		var v []string
		if len(arrayValueString) == 0 {
			v = make([]string, 0)
		} else {
			v = strings.Split(arrayValueString, ", ")
		}
		return v, nil
	case "timestamp":
		v, err := time.Parse("2006-01-02 15:04:05", data)
		if destKind == reflect.Ptr {
			return &v, err
		}
		return v, err
	case "date":
		v, err := time.Parse("2006-01-02", data)
		if destKind == reflect.Ptr {
			return &v, err
		}
		return v, err
	default:
		if athenaType != "varchar" {
			LogWarnf("athena data type '%s' not supported, defaulting to string: if this is intended consider doing conversion in SQL to be explicit", athenaType)
		}

		v := rowData.VarCharValue
		if destKind == reflect.Ptr {
			return v, nil
		}
		return util.SafeString(v), nil
	}
}
