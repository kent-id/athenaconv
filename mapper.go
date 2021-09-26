package athenaconv

import (
	"context"
	"log"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

type dataMapper struct {
	modelType             reflect.Type
	modelDefinitionSchema modelDefinitionMap
}

// DataMapper provides abstraction to convert athena ResultSet object to arbitrary user-defined struct
type DataMapper interface {
	FromAthenaResultSet(ctx context.Context, input *types.ResultSet) ([]interface{}, error)
}

// NewMapperFor creates new DataMapper for given reflect.Type
// Supports reflect.Type of value rather than a pointer.
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

// FromAthenaResultSet converts the ResultSet passed into array of mapper.modelType.
// Returns conversion error if header values are passed, i.e. first row of your athena ResultSet in page 1.
// Returns error if the athena ResultSetMetadata does not match the mapper definition.
func (m *dataMapper) FromAthenaResultSet(ctx context.Context, resultSet *types.ResultSet) ([]interface{}, error) {
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

			log.Printf("SET model.%s = row.Data[%d] with athena col name = '%s'", fieldName, mappedColumnInfo.index, athenaColName)
			colData, err := castAthenaRowData(ctx, row.Data[mappedColumnInfo.index], mappedColumnInfo.athenaColumnType)
			model.Elem().FieldByName(fieldName).Set(reflect.ValueOf(colData))
			if err != nil {
				return nil, err
			}
		}

		result = append(result, model.Elem())
	}

	return result, nil
}
