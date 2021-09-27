package athenaconv

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

type dataMapper struct {
	modelType             reflect.Type
	modelDefinitionSchema modelDefinitionMap
}

// DataMapper provides abstraction to convert athena ResultSet object to arbitrary user-defined struct
type DataMapper interface {
	FromAthenaResultSetV2(ctx context.Context, input *types.ResultSet) ([]interface{}, error)
}

// NewMapperFor creates new DataMapper for given reflect.Type
// reflect.Type should be of struct value type, not pointer to struct.
//
// Example:
//
// mapper, err := athenaconv.NewMapperFor(reflect.TypeOf(MyStruct{}))
func NewMapperFor(modelType reflect.Type) (DataMapper, error) {
	modelDefinitionSchema, err := newModelDefinitionMap(modelType)
	if err != nil {
		return nil, err
	}

	mapper := &dataMapper{
		modelType:             modelType,
		modelDefinitionSchema: modelDefinitionSchema,
	}
	return mapper, nil
}

// FromAthenaResultSetV2 converts ResultSet from aws-sdk-go-v2/service/athena/types into strongly-typed array[mapper.modelType]
// Returns conversion error if header values are passed, i.e. first row of your athena ResultSet in page 1.
// Returns error if the athena ResultSetMetadata does not match the mapper definition.
//
// Example:
// if page == 1 && len(queryResultOutput.ResultSet.Rows) > 0 {
// 		queryResultOutput.ResultSet.Rows = queryResultOutput.ResultSet.Rows[1:]		// skip header row
// }
// mapped, err := mapper.FromAthenaResultSetV2(ctx, queryResultOutput.ResultSet)
func (m *dataMapper) FromAthenaResultSetV2(ctx context.Context, resultSet *types.ResultSet) ([]interface{}, error) {
	resultSetSchema, err := newResultSetDefinitionMap(ctx, resultSet.ResultSetMetadata)
	if err != nil {
		return nil, err
	}

	err = validateResultSetSchema(ctx, resultSetSchema, m.modelDefinitionSchema)
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, 0)
	for _, row := range resultSet.Rows {
		model := reflect.New(m.modelType)
		for athenaColName, modelDefColInfo := range m.modelDefinitionSchema {
			mappedColumnInfo := resultSetSchema[athenaColName]
			fieldName := modelDefColInfo.fieldName

			// log.Printf("SET model.%s = row.Data[%d] with athena col name = '%s'", fieldName, mappedColumnInfo.index, athenaColName)
			colData, err := castAthenaRowData(ctx, row.Data[mappedColumnInfo.index], mappedColumnInfo.athenaColumnType)
			if err != nil {
				return nil, err
			}
			model.Elem().FieldByName(fieldName).Set(reflect.ValueOf(colData))
		}

		result = append(result, model.Interface())
	}

	return result, nil
}
