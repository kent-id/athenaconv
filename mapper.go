package athenaconv

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	typesv2 "github.com/aws/aws-sdk-go-v2/service/athena/types"
)

type destMode int

const (
	slice destMode = iota
	channel
)

type dataMapper struct {
	mu                    sync.Mutex // to guarantee protected read/appends to dest
	dest                  interface{}
	modelType             reflect.Type
	modelDefinitionSchema modelDefinitionMap
	mode                  destMode
}

// DataMapper provides abstraction to convert athena ResultSet object to arbitrary user-defined struct.
type DataMapper interface {
	AppendResultSetV2(ctx context.Context, input *typesv2.ResultSet) error
	GetModelType() reflect.Type
}

// NewMapperFor creates new DataMapper for the given *[]struct object where the results will be stored to.
// This will determine modelType and parse model definition from athenaconv tags in the struct fields,
// and returns error if any of the model is invalid.
//
// Example:
// var dest []MyModel
// mapper, err := athenaconv.NewMapperFor(&dest)
func NewMapperFor(dest interface{}) (mapper DataMapper, err error) {
	m := &dataMapper{dest: dest}
	err = m.parseMetadataFromDest()

	mapper = m
	return
}

// GetModelType tries to get struct type of dest, returns error if dest is not *[]struct TOREV
func (m *dataMapper) parseMetadataFromDest() (err error) {
	m.modelType = nil
	destType := reflect.TypeOf(m.dest)
	switch destType.Kind() {
	case reflect.Ptr:
		if destType.Elem().Kind() == reflect.Slice && destType.Elem().Elem().Kind() == reflect.Struct {
			// 1st Elem() gets SLICE/VALUE_TYPE from ptr
			// 2nd Elem() gets the struct type from the slice
			m.modelType = destType.Elem().Elem()
		}
	case reflect.Chan:
		if destType.Elem().Kind() == reflect.Ptr && destType.Elem().Elem().Kind() == reflect.Struct {
			// 1st Elem() gets *STRUCT from chan
			// 2nd Elem() gets the struct type from the ptr
			m.modelType = destType.Elem().Elem()
		}
	}

	if m.modelType == nil {
		return fmt.Errorf("invalid type: expecting *[]struct or chan *struct but got '%s' of kind '%s'", destType.String(), destType.Kind().String())
	}

	// finally if modelType is valid, parse model definition
	m.modelDefinitionSchema, err = newModelDefinitionMap(m.modelType)
	return

	// if destType.Kind() != reflect.Ptr || destType.Elem().Kind() != reflect.Slice || destType.Elem().Elem().Kind() != reflect.Struct {
	// 	return nil, fmt.Errorf("invalid type: expecting *[]struct or chan *struct but got '%s' of kind '%s'", destType.String(), destType.Kind().String())
	// }

	// if destType.Kind() != reflect.Ptr || destType.Elem().Kind() != reflect.Slice || destType.Elem().Elem().Kind() != reflect.Struct {
	// 	return nil, fmt.Errorf("invalid type: expecting *[]struct or chan *struct but got '%s' of kind '%s'", destType.String(), destType.Kind().String())
	// }

	// // 1st Elem() gets VALUE_TYPE from ptr which will be a slice,
	// // 2nd Elem() gets the struct type from the slice
	// return destType.Elem().Elem(), nil
}

// AppendResultSetV2 converts src from aws-sdk-go-v2/service/athena/types/ResultSet an append into strongly-typed dest.
// Returns conversion error if header values are passed, i.e. first row of your athena ResultSet in page 1.
// Returns error if the athena ResultSetMetadata does not match the mapper definition.
//
// Example:
// if page == 1 && len(queryResultOutput.ResultSet.Rows) > 0 {
// 		queryResultOutput.ResultSet.Rows = queryResultOutput.ResultSet.Rows[1:]		// skip header row
// }
// mapped, err := mapper.FromResultSetV2(ctx, queryResultOutput.ResultSet)
// XX TODO REV DOC
func (m *dataMapper) AppendResultSetV2(ctx context.Context, src *typesv2.ResultSet) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// get resultSetSchema from src
	resultSetSchema, err := newResultSetDefinitionMap(ctx, src.ResultSetMetadata)
	if err != nil {
		return err
	}

	// validate resultSetSchema and ensure it matches modelDefinitionSchema, hence allowing conversion
	err = validateResultSetSchema(ctx, resultSetSchema, m.modelDefinitionSchema)
	if err != nil {
		return err
	}

	// loop through src result set, convert each row to strongly-defined type, and append to result slice
	rowCount := len(src.Rows)
	tempDest := reflect.ValueOf(m.dest).Elem()

	LogDebug("reading %d rows from src", rowCount)
	for i, row := range src.Rows {
		// construct model of type modelType and set each field value within the struct
		model := reflect.New(m.modelType)
		for athenaColName, modelDefColInfo := range m.modelDefinitionSchema {
			mappedColumnInfo := resultSetSchema[athenaColName]
			fieldName := modelDefColInfo.fieldName

			if i == 0 {
				LogDebug("SET model.%s = row.Data[%d] with athena col name = '%s'", fieldName, mappedColumnInfo.index, athenaColName) // log only 1st row for brevity
			}

			colData, err := castAthenaRowData(ctx, row.Data[mappedColumnInfo.index], mappedColumnInfo.athenaColumnType)
			if err != nil {
				return err
			}
			model.Elem().FieldByName(fieldName).Set(reflect.ValueOf(colData))
		}

		// append model to tempDest
		LogDebug("appending to result slice: %+v", model.Elem())
		tempDest = reflect.Append(tempDest, reflect.ValueOf(model.Elem().Interface()))
	}

	// finally assign tempDest to dest
	reflect.ValueOf(m.dest).Elem().Set(tempDest)
	return nil
}

// GetModelType TODO
func (m *dataMapper) GetModelType() reflect.Type {
	return m.modelType
}

// FromResultSetV2 converts ResultSet from aws-sdk-go-v2/service/athena/types into dest where
// dest: *[]struct to save the results to,
// struct: has `athenaconv:column_name` tag for each field in the struct.
//
// Example:
// 	var dest []MyModel
// 	err := athenaconv.FromResultSetV2(ctx, &dest, queryResultOutput.ResultSet)
func FromResultSetV2(ctx context.Context, dest interface{}, src *typesv2.ResultSet) error {
	mapper, err := NewMapperFor(dest)
	if err != nil {
		return err
	}

	err = mapper.AppendResultSetV2(ctx, src)
	if err != nil {
		return err
	}

	return nil
}
