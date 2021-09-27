package athenaconv

import (
	"context"
	"reflect"
	"strconv"

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
			})
		})

		When("model type/definition is a pointer instead of struct value", func() {
			It("should return error", func() {
				_, err := NewMapperFor(reflect.TypeOf(&validModel{}))
				Expect(err).To(HaveOccurred())
			})
		})

		When("model type/definition is a map instead of struct value", func() {
			It("should return error", func() {
				testMap := make(map[string]int)
				_, err := NewMapperFor(reflect.TypeOf(testMap))
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Context("FromAthenaResultSetV2", func() {
		When("model definition and result set matches", func() {
			var mapper DataMapper
			var err error
			var metadata types.ResultSetMetadata

			BeforeEach(func() {
				mapper, err = NewMapperFor(reflect.TypeOf(validModel{}))
				Expect(err).ToNot(HaveOccurred())
				Expect(mapper).ToNot(BeNil())

				// arrange result set
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

			It("should correctly map the values with no row data", func() {
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
	})
})
