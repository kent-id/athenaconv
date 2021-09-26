package athenaconv

import (
	"context"
	"fmt"
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

			fmt.Printf("SET model.%s = row.Data[%d] with athena col name = '%s'", fieldName, mappedColumnInfo.index, athenaColName)
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
