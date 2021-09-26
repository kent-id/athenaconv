package athenaconv

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/kent-id/athenaconv/util"
)

type resultSetColumnSchema struct {
	index            int
	athenaColumnType string
}

type dataMapper struct {
	modelType   reflect.Type
	modelSchema map[string]string // map fieldAthenaColumnName:fieldName
}

// DataMapper provides abstraction to convert athena ResultSet object to
type DataMapper interface {
	FromAthenaResultSet(ctx context.Context, input *types.ResultSet) ([]interface{}, error)
}

func NewMapperFor(modelType reflect.Type) (DataMapper, error) {
	mapper := &dataMapper{
		modelType:   modelType,
		modelSchema: make(map[string]string),
	}
	// generate schema from struct tags:
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		fieldName := field.Name
		fieldAthenaColumnName := field.Tag.Get("athenaconv")
		if fieldAthenaColumnName == "" {
			err := fmt.Errorf("missing fieldAthenaColumnName for fieldName: %s", fieldName)
			return nil, err
		}

		if _, ok := mapper.modelSchema[fieldAthenaColumnName]; !ok {
			mapper.modelSchema[fieldAthenaColumnName] = fieldName
		} else {
			err := fmt.Errorf("duplicate fieldAthenaColumnName found: %s", fieldAthenaColumnName)
			return nil, err
		}
	}

	fmt.Println("init mapper successfully, schema:")
	fmt.Printf("%+v", mapper.modelSchema)

	return mapper, nil
}

func (m *dataMapper) FromAthenaResultSet(ctx context.Context, resultSet *types.ResultSet) ([]interface{}, error) {
	schema, err := m.generateSchemaFromResultSetMetadata(ctx, resultSet.ResultSetMetadata)
	if err != nil {
		return nil, err
	}
	// fmt.Println("schema from ResultSetMetadata:")
	// fmt.Printf("%+v\n", schema)

	err = m.validateSchema(ctx, schema)
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, 0)
	for _, row := range resultSet.Rows {
		model := reflect.New(m.modelType)
		for fieldAthenaColumnName, fieldName := range m.modelSchema {
			mappedColumnInfo := schema[fieldAthenaColumnName]
			fmt.Printf("SET model.%s = row.Data[%d] with athena col name = '%s'", fieldName, mappedColumnInfo.index, fieldAthenaColumnName)
			columnData := util.SafeString(row.Data[mappedColumnInfo.index].VarCharValue)
			err := castAndAssignToReflectValue(ctx, model.Elem().FieldByName(fieldName), columnData, mappedColumnInfo.athenaColumnType)
			if err != nil {
				return nil, err
			}
			// model.Elem().FieldByName(fieldName).Set(reflect.ValueOf(row.Data[mappedColumnIndex].VarCharValue))
		}

		result = append(result, model.Elem())
		fmt.Printf("model: %+v\n", model.Elem())
	}

	// for fieldAthenaColumnName, fieldName := range m.modelSchema {
	// 	model := reflect.New(m.modelType)
	// 	model.FieldByName(fieldName).Set()
	// 	model.FieldByName(fieldName).Set
	// }

	return result, nil
}

// generateSchemaFromResultSet returns map[fieldAthenaColumnName]index from given metadata
func (m *dataMapper) generateSchemaFromResultSetMetadata(ctx context.Context, resultSetMetadataSchema *types.ResultSetMetadata) (map[string]resultSetColumnSchema, error) {

	schema := make(map[string]resultSetColumnSchema)
	for index, columnInfo := range resultSetMetadataSchema.ColumnInfo {
		columnName := util.SafeString(columnInfo.Name)
		if columnName == "" {
			err := fmt.Errorf("column name from result set is empty, index: %d, columnInfo: %+v", index, columnInfo)
			return nil, err
		}

		columnType := util.SafeString(columnInfo.Type)
		if columnType == "" {
			err := fmt.Errorf("column type from result set is empty, index: %d, name: %s, columnInfo: %+v", index, columnName, columnInfo)
			return nil, err
		}

		if _, ok := schema[*columnInfo.Name]; !ok {
			schema[*columnInfo.Name] = resultSetColumnSchema{
				index:            index,
				athenaColumnType: *columnInfo.Type,
			}
		} else {
			err := fmt.Errorf("duplicate column name from result set, index: %d, name: %s, columnInfo: %+v", index, columnName, columnInfo)
			return nil, err
		}
	}
	return schema, nil
}

func (m *dataMapper) validateSchema(ctx context.Context, resultSetMetadataSchema map[string]resultSetColumnSchema) error {
	modelSchemaLength := len(m.modelSchema)
	resultMetadataSchemaLength := len(resultSetMetadataSchema)
	if modelSchemaLength != resultMetadataSchemaLength {
		err := fmt.Errorf("mismatched schema definition and result set columns count, modelSchemaLength: %d, resultMetadataSchemaLength: %d", modelSchemaLength, resultMetadataSchemaLength)
		return err
	}

	for key, _ := range m.modelSchema {
		if _, ok := resultSetMetadataSchema[key]; !ok {
			err := fmt.Errorf("column '%s' is defined in model schema but not found in result set", key)
			return err
		}
	}

	return nil
}

// var (
// 	stringType = reflect.TypeOf("")

// 	int8Type  = reflect.TypeOf(int8(0))
// 	int16Type = reflect.TypeOf(int16(0))
// 	int32Type = reflect.TypeOf(int32(0))
// 	int64Type = reflect.TypeOf(int64(0))
// 	intType   = reflect.TypeOf(int(0))

// 	stringSliceType = reflect.TypeOf(make([]string, 0))
// 	timeType        = reflect.TypeOf(time.Now)

// 	// uint8Type  = reflect.TypeOf(uint8(0))
// 	// uint16Type = reflect.TypeOf(uint16(0))
// 	// uint32Type = reflect.TypeOf(uint32(0))
// 	// uint64Type = reflect.TypeOf(uint64(0))
// 	// uintType   = reflect.TypeOf(uint(0))
// )

func castAndAssignToReflectValue(ctx context.Context, target reflect.Value, data string, athenaType string) error {
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
		arrayValue := strings.Split(arrayValueString, ", ")
		newStringSlice := make([]string, 0)
		newStringSlice = append(newStringSlice, arrayValue...)
		castedData = newStringSlice
	case "timestamp":
		castedData, err = time.Parse("2006-01-02 15:04:05", data)
	case "date":
		castedData, err = time.Parse("2006-01-02", data)
	default:
		fmt.Printf("ATHENA DATA TYPE NOT SUPPORTED: '%s', defaulting to string\n", athenaType)
		castedData = data
	}

	// targetType := target.Type()
	// fmt.Println("targetType", targetType)
	// switch targetType {
	// case stringType:
	// 	castedData = data
	// case intType:
	// 	castedData, err = strconv.Atoi(data)
	// case int64Type:
	// 	castedData, err = strconv.ParseInt(data, 10, 64)
	// case int32Type:
	// 	castedData, err = strconv.ParseInt(data, 10, 32)
	// 	castedData = int32(castedData.(int64))
	// case int16Type:
	// 	castedData, err = strconv.ParseInt(data, 10, 16)
	// 	castedData = int16(castedData.(int64))
	// case int8Type:
	// 	castedData, err = strconv.ParseInt(data, 10, 8)
	// 	castedData = int8(castedData.(int64))
	// case stringSliceType:
	// 	arrayValueString := strings.Trim(data, "[]")
	// 	arrayValue := strings.Split(arrayValueString, ", ")
	// 	newStringSlice := make([]string, 0)
	// 	newStringSlice = append(newStringSlice, arrayValue...)
	// 	castedData = newStringSlice
	// case timeType:

	// default:
	// 	fmt.Printf("NOT IMPLEMENTED: cast to data type %s, defaulting to string\n", targetType.String())
	// 	castedData = data
	// }
	if err != nil {
		return err
	}

	if castedData != nil {
		target.Set(reflect.ValueOf(castedData))
	}
	return nil
}
