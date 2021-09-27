package athenaconv

import (
	"context"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/kent-id/athenaconv/util"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type validModel struct {
	ID   int    `athenaconv:"my_id_col"`
	Name string `athenaconv:"name_col"`
}

type invalidModel struct {
	ID   int `athenaconv:"my_id_col"`
	Name string
}

var _ = Describe("Mapper", func() {
	var ctx context.Context
	BeforeEach(func() {
		ctx = context.Background()
	})

	Context("NewMapperFor", func() {
		When("model type/definition is valid", func() {
			It("should not return any error", func() {
				_, err := NewMapperFor(reflect.TypeOf(validModel{}))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		When("model type/definition is not valid", func() {
			It("should return error", func() {
				_, err := NewMapperFor(reflect.TypeOf(invalidModel{}))
				Expect(err).To(HaveOccurred())
				Expect(strings.ToLower(err.Error())).To(MatchRegexp("missing .* name"))
			})
		})

		When("model type/definition is a pointer instead of struct value", func() {
			It("should return error", func() {
				_, err := NewMapperFor(reflect.TypeOf(&validModel{}))
				Expect(err).To(HaveOccurred())
				Expect(strings.ToLower(err.Error())).To(ContainSubstring("invalid modeltype"))
			})
		})

		When("model type/definition is a map instead of struct value", func() {
			It("should return error", func() {
				testMap := make(map[string]int)
				_, err := NewMapperFor(reflect.TypeOf(testMap))
				Expect(err).To(HaveOccurred())
				Expect(strings.ToLower(err.Error())).To(ContainSubstring("invalid modeltype"))
			})
		})
	})

	Context("FromAthenaResultSetV2", func() {
		var mapper DataMapper
		var err error
		var metadata types.ResultSetMetadata

		BeforeEach(func() {
			mapper, err = NewMapperFor(reflect.TypeOf(validModel{}))
			Expect(err).ToNot(HaveOccurred())
			Expect(mapper).ToNot(BeNil())

			// result set
			metadata = types.ResultSetMetadata{
				ColumnInfo: make([]types.ColumnInfo, 0),
			}
			metadata.ColumnInfo = append(metadata.ColumnInfo, types.ColumnInfo{
				Name: util.RefString("my_id_col"),
				Type: util.RefString("integer"),
			})
			metadata.ColumnInfo = append(metadata.ColumnInfo, types.ColumnInfo{
				Name: util.RefString("name_col"),
				Type: util.RefString("varchar"),
			})
		})

		When("model definition and result set matches", func() {
			It("should correctly map the values with no row data", func() {
				// arrange
				resultSet := types.ResultSet{
					ResultSetMetadata: &metadata,
					Rows:              make([]types.Row, 0),
				}

				// act
				mapped, err := mapper.FromAthenaResultSetV2(ctx, &resultSet)

				// assert
				Expect(err).ToNot(HaveOccurred())
				Expect(mapped).ToNot(BeNil())
				Expect(len(mapped)).To(Equal(0))
			})

			It("should correctly map the values with row data", func() {
				// arrange
				resultSet := types.ResultSet{
					ResultSetMetadata: &metadata,
					Rows:              make([]types.Row, 0),
				}
				for i := 0; i < 100; i++ {
					resultSet.Rows = append(resultSet.Rows, types.Row{
						Data: []types.Datum{
							{
								VarCharValue: util.RefString(strconv.Itoa(i)),
							},
							{
								VarCharValue: util.RefString("name " + strconv.Itoa(i)),
							},
						},
					})
				}

				// act
				mapped, err := mapper.FromAthenaResultSetV2(ctx, &resultSet)

				// assert
				Expect(err).ToNot(HaveOccurred())
				Expect(mapped).ToNot(BeNil())
				Expect(len(mapped)).To(Equal(100))
				for index, mappedItem := range mapped {
					Expect(mappedItem).To(BeAssignableToTypeOf(&validModel{}))
					casted := mappedItem.(*validModel)
					Expect(casted.ID).To(Equal(index))
					Expect(casted.Name).To(Equal("name " + strconv.Itoa(index)))
				}
			})
		})

		When("result set definition contains invalid metadata", func() {
			It("should return error", func() {
				// arrange
				metadata.ColumnInfo[0].Name = nil
				resultSet := types.ResultSet{
					ResultSetMetadata: &metadata,
					Rows:              make([]types.Row, 0),
				}

				// act
				_, err := mapper.FromAthenaResultSetV2(ctx, &resultSet)

				// assert
				Expect(err).To(HaveOccurred())
				Expect(strings.ToLower(err.Error())).To(ContainSubstring("column name from result set is empty"))
			})
		})

		When("result set definition contains invalid row data that cannot be casted", func() {
			It("should return error", func() {
				// arrange
				resultSet := types.ResultSet{
					ResultSetMetadata: &metadata,
					Rows:              make([]types.Row, 0),
				}

				resultSet.Rows = append(resultSet.Rows, types.Row{
					Data: []types.Datum{
						{
							VarCharValue: util.RefString("invalid_int_value"),
						},
						{
							VarCharValue: util.RefString("name_value"),
						},
					},
				})

				// act
				_, err := mapper.FromAthenaResultSetV2(ctx, &resultSet)

				// assert
				Expect(err).To(HaveOccurred())
				Expect(strings.ToLower(err.Error())).To(MatchRegexp("parsing .* invalid syntax"))
			})
		})

		When("model definition and result set does not match", func() {
			It("should return error on mismatched field count", func() {
				// arrange
				metadata.ColumnInfo = metadata.ColumnInfo[1:] // remove first metadata
				resultSet := types.ResultSet{
					ResultSetMetadata: &metadata,
					Rows:              make([]types.Row, 0),
				}

				// act
				_, err := mapper.FromAthenaResultSetV2(ctx, &resultSet)

				// assert
				Expect(err).To(HaveOccurred())
				Expect(strings.ToLower(err.Error())).To(ContainSubstring("mismatched schema"))
			})

			It("should return error on column name not found in model definition", func() {
				// arrange
				metadata.ColumnInfo[0].Name = util.RefString("something_other_than_my_id_col")
				resultSet := types.ResultSet{
					ResultSetMetadata: &metadata,
					Rows:              make([]types.Row, 0),
				}

				// act
				_, err := mapper.FromAthenaResultSetV2(ctx, &resultSet)

				// assert
				Expect(err).To(HaveOccurred())
				Expect(strings.ToLower(err.Error())).To(MatchRegexp("'my_id_col' .* not found"))
			})
		})
	})
})
