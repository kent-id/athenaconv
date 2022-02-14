package athenaconv

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	typesv2 "github.com/kent-id/athenaconv/types"
)

type destMode int

const (
	destModeSlice destMode = iota
	destModeChannel
)

type dataMapper struct {
	mu                    sync.Mutex // to guarantee protected read/appends to dest
	dest                  interface{}
	mode                  destMode
	modelType             reflect.Type
	modelDefinitionSchema modelDefinitionMap
}

// DataMapper provides abstraction to convert athena ResultSet object to arbitrary user-defined struct.
type DataMapper interface {
	AppendResultSetV2(ctx context.Context, input *typesv2.ResultSet) error
	GetModelType() reflect.Type
}

// NewMapperFor creates new DataMapper for strongly-typed `*[]struct` or `chan *struct` dest where the results will be stored to.
// This will determine modelType and parse model definition from athenaconv tags in the struct fields,
// and returns error if the model is invalid.
//
// Example:
// var dest []MyModel
// mapper, err := athenaconv.NewMapperFor(&dest)
func NewMapperFor(dest interface{}) (DataMapper, error) {
	mapper := &dataMapper{dest: dest}
	err := mapper.parseMetadataFromDest()
	return mapper, err
}

// parseMetadataFromDest parses modelType and modelDefinitionSchema from m.dest
func (m *dataMapper) parseMetadataFromDest() (err error) {
	m.modelType = nil
	destType := reflect.TypeOf(m.dest)
	switch destType.Kind() {
	case reflect.Ptr:
		if destType.Elem().Kind() != reflect.Slice || destType.Elem().Elem().Kind() != reflect.Struct {
			err = fmt.Errorf("invalid type: expecting *[]struct or chan *struct but got '%s' of kind '%s'", destType.String(), destType.Kind().String())
		} else {
			m.modelType = destType.Elem().Elem() // 1st elem gets slice from ptr, 2nd elem gets struct type from slice
			m.mode = destModeSlice
		}
	case reflect.Chan:
		if destType.Elem().Kind() != reflect.Struct {
			err = fmt.Errorf("invalid type: expecting *[]struct or chan *struct but got '%s' of kind '%s'", destType.String(), destType.Kind().String())
		} else if m.dest == nil {
			err = fmt.Errorf("invalid type: nil channel received")
		} else {
			m.modelType = destType.Elem() // elem gets struct type from chan
			m.mode = destModeChannel
		}
	default:
		err = fmt.Errorf("invalid type: expecting *[]struct or chan *struct but got '%s' of kind '%s'", destType.String(), destType.Kind().String())
	}

	if err != nil {
		return
	}

	// finally if modelType is valid, parse model definition
	m.modelDefinitionSchema, err = newModelDefinitionMap(m.modelType)
	return
}

// AppendResultSetV2 converts src from aws-sdk-go-v2/service/athena/types/ResultSet an appends into dest
// Returns conversion error if header values are passed, i.e. first row of your athena ResultSet in page 1.
// Returns error if the athena ResultSetMetadata does not match the mapper definition.
//
// Example:
// var dest []MyModel
// mapper, err := NewMapperFor(&dest)
// if page == 1 && len(queryResultOutput.ResultSet.Rows) > 0 {
// 		queryResultOutput.ResultSet.Rows = queryResultOutput.ResultSet.Rows[1:]		// skip header row
// }
// err = mapper.AppendResultSetV2(ctx, queryResultOutput.ResultSet)
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
	var tempDest reflect.Value
	if m.mode == destModeSlice {
		tempDest = reflect.ValueOf(m.dest).Elem()
	} else {
		tempDest = reflect.ValueOf(m.dest)
	}

	LogDebugf("mapping %d row(s) from src", rowCount)
	for i, row := range src.Rows {
		// construct model of type modelType and set each field value within the struct
		model := reflect.New(m.modelType)
		for athenaColName, modelDefColInfo := range m.modelDefinitionSchema {
			mappedColumnInfo := resultSetSchema[athenaColName]
			fieldName := modelDefColInfo.fieldName
			dest := model.Elem().FieldByName(fieldName)
			destKind := dest.Kind()

			if i == 0 {
				// only log 1st row for brevity
				LogDebugf("assigning model.%s; athenaColName: %s, destKind: %s, mappedColumnInfo: %+v", fieldName, athenaColName, destKind.String(), mappedColumnInfo)
			}

			castedData, err := castAthenaRowData(ctx, row.Data[mappedColumnInfo.index], mappedColumnInfo.athenaColumnType, destKind)
			if err != nil {
				return err
			}
			if castedData != nil {
				dest.Set(reflect.ValueOf(castedData))
			}
		}

		// append/push depending if dest is slice/channel
		modelToAppend := reflect.ValueOf(model.Elem().Interface())
		if m.mode == destModeSlice {
			tempDest = reflect.Append(tempDest, modelToAppend)
		} else {
			tempDest.Send(modelToAppend)
		}
		if i == 0 {
			LogDebugf("appended to result slice/chan, model: %+v", modelToAppend)
		}
	}

	// finally assign tempDest to dest if writing to slice
	if m.mode == destModeSlice {
		reflect.ValueOf(m.dest).Elem().Set(tempDest)
	}
	return nil
}

// GetModelType gets the underlying struct type based on dest passed to the mapper object.
func (m *dataMapper) GetModelType() reflect.Type {
	return m.modelType
}
