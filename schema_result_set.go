package athenaconv

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/kent-id/athenaconv/util"
)

// resultSetDefinitionMap is a map of athenaColName to each column returned by atena queries
type resultSetDefinitionMap map[string]resultSetColInfo

// resultSetColInfo as retrieved from the ResultSetMetadata returned by athena queries
type resultSetColInfo struct {
	index            int
	athenaColumnType string
}

// newResultSetDefinitionMap reads the schema definition from result set metadata
func newResultSetDefinitionMap(ctx context.Context, resultSetMetadataSchema *types.ResultSetMetadata) (resultSetDefinitionMap, error) {
	if len(resultSetMetadataSchema.ColumnInfo) <= 0 {
		err := fmt.Errorf("at least one column be returned by the data set")
		return nil, err
	}

	schema := make(map[string]resultSetColInfo)
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
			schema[*columnInfo.Name] = resultSetColInfo{
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

func validateResultSetSchema(ctx context.Context, resultSetSchema resultSetDefinitionMap, modelDefSchema modelDefinitionMap) error {
	modelSchemaLength := len(modelDefSchema)
	resultMetadataSchemaLength := len(resultSetSchema)
	if modelSchemaLength != resultMetadataSchemaLength {
		err := fmt.Errorf("mismatched schema definition and result set columns count, modelSchemaLength: %d, resultMetadataSchemaLength: %d", modelSchemaLength, resultMetadataSchemaLength)
		return err
	}

	for key, _ := range modelDefSchema {
		if _, ok := resultSetSchema[key]; !ok {
			err := fmt.Errorf("column '%s' is defined in model schema but not found in result set", key)
			return err
		}
	}

	return nil
}
