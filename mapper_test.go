package athenaconv

import (
	"context"
	"reflect"
	"strconv"
	"strings"

	"github.com/kent-id/athenaconv/types"
	"github.com/kent-id/athenaconv/util"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type validModel struct {
	ID   *int    `athenaconv:"my_id_col"`
	Name *string `athenaconv:"name_col"`
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
		When("valid type passed", func() {
			var mapper DataMapper
			var err error

			AfterEach(func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(mapper.GetModelType()).To(Equal(reflect.TypeOf(validModel{})))
			})

			It("should return the underlying struct type for *[]struct", func() {
				var dest []validModel
				mapper, err = NewMapperFor(&dest)
			})

			It("should return the underlying struct type for chan struct", func() {
				var dest chan validModel
				mapper, err = NewMapperFor(dest)
			})

			It("should return the underlying struct type for directional chan struct", func() {
				var dest chan<- validModel
				mapper, err = NewMapperFor(dest)
			})
		})

		When("invalid types are passed", func() {
			var err error

			AfterEach(func() {
				Expect(err).To(HaveOccurred())
				Expect(strings.ToLower(err.Error())).To(MatchRegexp("invalid type: .*"))
			})

			It("should return error for []*struct (not []struct)", func() {
				var dest []*validModel
				_, err = NewMapperFor(dest)
			})

			It("should return error for int", func() {
				var dest int
				_, err = NewMapperFor(dest)
			})

			It("should return error for map", func() {
				var dest map[int]string
				_, err = NewMapperFor(dest)
			})

			It("should return error for double pointer", func() {
				var dest **validModel
				_, err = NewMapperFor(dest)
			})

			It("should return error for chan *struct (not chan struct)", func() {
				var dest chan *validModel
				_, err = NewMapperFor(dest)
			})
		})
	})

	Context("ConvertResultSetV2", func() {
		var metadata types.ResultSetMetadata

		BeforeEach(func() {
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

		When("model type/definition is not valid", func() {
			It("should return error", func() {
				// arrange
				resultSet := types.ResultSet{
					ResultSetMetadata: &metadata,
					Rows:              make([]types.Row, 0),
				}

				// act
				var dest []invalidModel
				err := ConvertResultSetV2(ctx, &dest, &resultSet)

				// assert
				Expect(err).To(HaveOccurred())
				Expect(strings.ToLower(err.Error())).To(MatchRegexp("missing .* name"))
			})
		})

		When("model type/definition is a slice of int", func() {
			It("should return error", func() {
				// arrange
				resultSet := types.ResultSet{
					ResultSetMetadata: &metadata,
					Rows:              make([]types.Row, 0),
				}

				// act
				var dest []int
				err := ConvertResultSetV2(ctx, &dest, &resultSet)

				// assert
				Expect(err).To(HaveOccurred())
				Expect(strings.ToLower(err.Error())).To(MatchRegexp("invalid type: .*"))
			})
		})

		When("model definition and result set matches", func() {
			It("should correctly map the values with no row data with dest being nil", func() {
				// arrange
				resultSet := types.ResultSet{
					ResultSetMetadata: &metadata,
					Rows:              make([]types.Row, 0),
				}

				// act
				var dest []validModel = nil
				err := ConvertResultSetV2(ctx, &dest, &resultSet)

				// assert
				Expect(err).ToNot(HaveOccurred())
				Expect(dest).To(BeNil())
			})

			It("should correctly map the values with no row data with dest being an empty slice", func() {
				// arrange
				resultSet := types.ResultSet{
					ResultSetMetadata: &metadata,
					Rows:              make([]types.Row, 0),
				}

				// act
				dest := make([]validModel, 0)
				err := ConvertResultSetV2(ctx, &dest, &resultSet)

				// assert
				Expect(err).ToNot(HaveOccurred())
				Expect(dest).ToNot(BeNil())
				Expect(dest).To(HaveLen(0))
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
				var dest []validModel
				err := ConvertResultSetV2(ctx, &dest, &resultSet)

				// assert
				Expect(err).ToNot(HaveOccurred())
				Expect(dest).ToNot(BeNil())
				Expect(dest).To(HaveLen(100))
				for index, item := range dest {
					Expect(item.ID).ToNot(BeNil())
					Expect(*item.ID).To(Equal(index))
					Expect(item.Name).ToNot(BeNil())
					Expect(*item.Name).To(Equal("name " + strconv.Itoa(index)))
				}
			})
		})

		It("should correctly map the values with row data which contains nil", func() {
			// arrange
			resultSet := types.ResultSet{
				ResultSetMetadata: &metadata,
				Rows:              make([]types.Row, 0),
			}
			resultSet.Rows = append(resultSet.Rows, types.Row{
				Data: []types.Datum{
					{
						VarCharValue: util.RefString("1"),
					},
					{
						VarCharValue: util.RefString("name 1"),
					},
				},
			})
			resultSet.Rows = append(resultSet.Rows, types.Row{
				Data: []types.Datum{
					{
						VarCharValue: nil,
					},
					{
						VarCharValue: nil,
					},
				},
			})

			// act
			var dest []validModel
			err := ConvertResultSetV2(ctx, &dest, &resultSet)

			// assert
			Expect(err).ToNot(HaveOccurred())
			Expect(dest).ToNot(BeNil())
			Expect(dest).To(HaveLen(2))
			Expect(dest[0].ID).ToNot(BeNil())
			Expect(*dest[0].ID).To(Equal(1))
			Expect(dest[0].Name).ToNot(BeNil())
			Expect(*dest[0].Name).To(Equal("name 1"))
			Expect(dest[1].ID).To(BeNil())
			Expect(dest[1].Name).To(BeNil())
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
				var dest []validModel
				err := ConvertResultSetV2(ctx, &dest, &resultSet)

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
				var dest []validModel
				err := ConvertResultSetV2(ctx, &dest, &resultSet)

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
				var dest []validModel
				err := ConvertResultSetV2(ctx, &dest, &resultSet)

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
				var dest []validModel
				err := ConvertResultSetV2(ctx, &dest, &resultSet)

				// assert
				Expect(err).To(HaveOccurred())
				Expect(strings.ToLower(err.Error())).To(MatchRegexp("'my_id_col' .* not found"))
			})
		})
	})
})
