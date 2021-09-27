package athenaconv

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/kent-id/athenaconv/util"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Schema: result set", func() {
	var ctx context.Context
	BeforeEach(func() {
		ctx = context.Background()
	})

	Context("newResultSetDefinitionMap", func() {
		When("result set metadata is valid", func() {
			It("should return expected column definition", func() {
				metadata := types.ResultSetMetadata{
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
				def, err := newResultSetDefinitionMap(ctx, &metadata)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(def)).To(Equal(2))

				Expect(def["my_id_col"].index).To(Equal(0))
				Expect(def["my_id_col"].athenaColumnType).To(Equal("integer"))
				Expect(def["name_col"].index).To(Equal(1))
				Expect(def["name_col"].athenaColumnType).To(Equal("varchar"))
			})
		})

		When("result set metadata has missing column name / type", func() {
			It("should return error on missing name", func() {
				metadata := types.ResultSetMetadata{
					ColumnInfo: make([]types.ColumnInfo, 0),
				}
				metadata.ColumnInfo = append(metadata.ColumnInfo, types.ColumnInfo{
					Type: util.RefString("integer"),
				})
				_, err := newResultSetDefinitionMap(ctx, &metadata)
				Expect(err).To(HaveOccurred())
			})

			It("should return error on missing name", func() {
				metadata := types.ResultSetMetadata{
					ColumnInfo: make([]types.ColumnInfo, 0),
				}
				metadata.ColumnInfo = append(metadata.ColumnInfo, types.ColumnInfo{
					Name: util.RefString("my_id_col"),
				})
				_, err := newResultSetDefinitionMap(ctx, &metadata)
				Expect(err).To(HaveOccurred())
			})
		})

		When("result set metadata has duplicate column name", func() {
			It("should return error", func() {
				metadata := types.ResultSetMetadata{
					ColumnInfo: make([]types.ColumnInfo, 0),
				}
				metadata.ColumnInfo = append(metadata.ColumnInfo, types.ColumnInfo{
					Name: util.RefString("my_id_col"),
					Type: util.RefString("varchar"),
				})
				metadata.ColumnInfo = append(metadata.ColumnInfo, types.ColumnInfo{
					Name: util.RefString("my_id_col"),
					Type: util.RefString("varchar"),
				})
				_, err := newResultSetDefinitionMap(ctx, &metadata)
				Expect(err).To(HaveOccurred())
			})
		})

		When("result set metadata has no column info defined", func() {
			It("should return error", func() {
				metadata := types.ResultSetMetadata{
					ColumnInfo: make([]types.ColumnInfo, 0),
				}
				_, err := newResultSetDefinitionMap(ctx, &metadata)
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Context("newResultSetDefinitionMap", func() {
		When("model type/definition is valid", func() {
			It("should return expected column definition", func() {
				// model definition schema
				type test struct {
					ID   int    `athenaconv:"my_id_col"`
					Name string `athenaconv:"name_col"`
				}
				modelDefinitionSchema, err := newModelDefinitionMap(reflect.TypeOf(test{}))
				Expect(err).ToNot(HaveOccurred())
				Expect(len(modelDefinitionSchema)).To(Equal(2))

				// result set schema
				metadata := types.ResultSetMetadata{
					ColumnInfo: make([]types.ColumnInfo, 0),
				}
				metadata.ColumnInfo = append(metadata.ColumnInfo, types.ColumnInfo{
					Name: util.RefString("name_col"),
					Type: util.RefString("varchar"),
				})
				metadata.ColumnInfo = append(metadata.ColumnInfo, types.ColumnInfo{
					Name: util.RefString("my_id_col"),
					Type: util.RefString("integer"),
				})
				resultSetSchema, err := newResultSetDefinitionMap(ctx, &metadata)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(resultSetSchema)).To(Equal(2))

				err = validateResultSetSchema(ctx, resultSetSchema, modelDefinitionSchema)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		When("result set metadata column is not found in model definition", func() {
			It("should return error", func() {
				// model definition schema
				type test struct {
					ID   int    `athenaconv:"my_id_col"`
					Name string `athenaconv:"name_col"`
				}
				modelDefinitionSchema, err := newModelDefinitionMap(reflect.TypeOf(test{}))
				Expect(err).ToNot(HaveOccurred())
				Expect(len(modelDefinitionSchema)).To(Equal(2))

				// result set schema
				metadata := types.ResultSetMetadata{
					ColumnInfo: make([]types.ColumnInfo, 0),
				}
				metadata.ColumnInfo = append(metadata.ColumnInfo, types.ColumnInfo{
					Name: util.RefString("my_id_col"),
					Type: util.RefString("integer"),
				})
				metadata.ColumnInfo = append(metadata.ColumnInfo, types.ColumnInfo{
					Name: util.RefString("something_else"),
					Type: util.RefString("varchar"),
				})
				resultSetSchema, err := newResultSetDefinitionMap(ctx, &metadata)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(resultSetSchema)).To(Equal(2))

				err = validateResultSetSchema(ctx, resultSetSchema, modelDefinitionSchema)
				Expect(err).To(HaveOccurred())
			})
		})
	})
})
