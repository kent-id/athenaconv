package athenaconv

import (
	"context"

	typesv2 "github.com/kent-id/athenaconv/types"
)

// ConvertResultSetV2 converts ResultSet from aws-sdk-go-v2/service/athena/types into dest.
// Useful for one-time conversion. For repeated use, consider creating DataMapper.
//
// Example:
// 	var dest []MyModel
// 	err := athenaconv.FromResultSetV2(ctx, &dest, queryResultOutput.ResultSet)
func ConvertResultSetV2(ctx context.Context, dest interface{}, src *typesv2.ResultSet) (err error) {
	mapper, err := NewMapperFor(dest)
	if err == nil {
		err = mapper.AppendResultSetV2(ctx, src)
	}
	return
}
