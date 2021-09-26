package athenaconv

import (
	"fmt"
	"reflect"
)

// schemaDefinition is a map of athenaColName to each field/column defined in struct tags
type modelDefinitionMap map[string]modelDefinitionColInfo

// modelDefinitionColInfo as defined in the user-defined struct field tags
type modelDefinitionColInfo struct {
	fieldName string
}

func newModelDefinitionMap(modelType reflect.Type) (modelDefinitionMap, error) {
	if modelType.NumField() <= 0 {
		err := fmt.Errorf("at least one field should be defined for struct of type: %s", modelType.String())
		return nil, err
	}

	schema := make(map[string]modelDefinitionColInfo)
	// generate schema from struct tags:
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		fieldName := field.Name
		athenaColName := field.Tag.Get("athenaconv")
		if athenaColName == "" {
			err := fmt.Errorf("missing athenaColName for fieldName: %s", fieldName)
			return nil, err
		}

		if _, ok := schema[athenaColName]; !ok {
			schema[athenaColName] = modelDefinitionColInfo{
				fieldName: fieldName,
			}
		} else {
			err := fmt.Errorf("duplicate athenaColName found: %s", athenaColName)
			return nil, err
		}
	}

	return schema, nil
}
