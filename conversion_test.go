package athenaconv

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/kent-id/athenaconv/util"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	athenaTypeBool      = "boolean"
	athenaTypeString    = "varchar"
	athenaTypeInt       = "integer"
	athenaTypeBigInt    = "bigint"
	athenaTypeArray     = "array"
	athenaTypeTimestamp = "timestamp"
	athenaTypeDate      = "date"
)

var _ = Describe("Conversion", func() {
	var ctx context.Context
	BeforeEach(func() {
		ctx = context.Background()
	})

	Context("Boolean", func() {
		It("should return false on FALSE bool value", func() {
			rowData := types.Datum{VarCharValue: util.RefString("true")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeBool)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(BeTrue())
		})
		It("should return true on TRUE bool value", func() {
			rowData := types.Datum{VarCharValue: util.RefString("false")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeBool)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(BeFalse())
		})
		It("should default to false on invalid bool value", func() {
			rowData := types.Datum{VarCharValue: util.RefString("some-invalid-value")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeBool)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(BeFalse())
		})
	})

	Context("String", func() {
		It("should return value as is", func() {
			rowData := types.Datum{VarCharValue: util.RefString("test data")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeString)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal("test data"))
		})
	})

	Context("Integer", func() {
		It("should return value if valid", func() {
			rowData := types.Datum{VarCharValue: util.RefString("-2147483648")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeInt)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(int(-2147483648)))
		})

		It("should return error if not valid", func() {
			rowData := types.Datum{VarCharValue: util.RefString("-----2147483648")}
			_, err := castAthenaRowData(ctx, rowData, athenaTypeInt)
			Expect(err).To(HaveOccurred())
		})

		// anything above int64 range will overflow
		It("should return error if overflow", func() {
			rowData := types.Datum{VarCharValue: util.RefString("9223372036854775807123213122")}
			_, err := castAthenaRowData(ctx, rowData, athenaTypeInt)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("BigInt", func() {
		It("should return value if valid", func() {
			rowData := types.Datum{VarCharValue: util.RefString("9223372036854775807")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeBigInt)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(int64(9223372036854775807)))
		})

		It("should return error if not valid", func() {
			rowData := types.Datum{VarCharValue: util.RefString("9223372_NOT_VALID_036854775807")}
			_, err := castAthenaRowData(ctx, rowData, athenaTypeBigInt)
			Expect(err).To(HaveOccurred())
		})

		// anything above int64 range will overflow
		It("should return error if overflow", func() {
			rowData := types.Datum{VarCharValue: util.RefString("9223372036854775807123213122")}
			_, err := castAthenaRowData(ctx, rowData, athenaTypeInt)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("Array", func() {
		When("array has no items", func() {
			It("should return expected array value", func() {
				rowData := types.Datum{VarCharValue: util.RefString("[]")}
				result, err := castAthenaRowData(ctx, rowData, athenaTypeArray)
				Expect(err).ToNot(HaveOccurred())
				arr := result.([]string)
				Expect(len(arr)).To(BeZero())
			})
		})

		When("array has one item", func() {
			It("should return expected array value", func() {
				rowData := types.Datum{VarCharValue: util.RefString("[data1]")}
				result, err := castAthenaRowData(ctx, rowData, athenaTypeArray)
				Expect(err).ToNot(HaveOccurred())
				arr := result.([]string)
				Expect(len(arr)).To(Equal(1))
				Expect(arr[0]).To(Equal("data1"))
			})
		})

		When("array has two items", func() {
			It("should return expected array value", func() {
				rowData := types.Datum{VarCharValue: util.RefString("[data1, data2]")}
				result, err := castAthenaRowData(ctx, rowData, athenaTypeArray)
				Expect(err).ToNot(HaveOccurred())
				arr := result.([]string)
				Expect(len(arr)).To(Equal(2))
				Expect(arr[0]).To(Equal("data1"))
				Expect(arr[1]).To(Equal("data2"))
			})
		})
	})

	Context("Timestamp", func() {
		It("should return value if valid", func() {
			rowData := types.Datum{VarCharValue: util.RefString("2012-10-31 08:11:22.000")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeTimestamp)
			Expect(err).ToNot(HaveOccurred())

			expected := time.Date(2012, 10, 31, 8, 11, 22, 0, time.UTC)
			Expect(result).To(BeAssignableToTypeOf(expected))
			ts := result.(time.Time)
			Expect(ts).To(Equal(expected))
		})

		It("should return value if valid with milliseconds", func() {
			rowData := types.Datum{VarCharValue: util.RefString("2012-10-31 08:11:22.512")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeTimestamp)
			Expect(err).ToNot(HaveOccurred())

			expected := time.Date(2012, 10, 31, 8, 11, 22, int(time.Millisecond)*512, time.UTC)
			Expect(result).To(BeAssignableToTypeOf(expected))
			ts := result.(time.Time)
			Expect(ts).To(Equal(expected))
		})

		It("should return error if invalid", func() {
			rowData := types.Datum{VarCharValue: util.RefString("INVALID DATE 2012-10-31 08:11:22.000")}
			_, err := castAthenaRowData(ctx, rowData, athenaTypeTimestamp)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("Date", func() {
		It("should return value if valid", func() {
			rowData := types.Datum{VarCharValue: util.RefString("2016-02-29")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeDate)
			Expect(err).ToNot(HaveOccurred())

			expected := time.Date(2016, 02, 29, 0, 0, 0, 0, time.UTC)
			Expect(result).To(BeAssignableToTypeOf(expected))
			ts := result.(time.Time)
			Expect(ts).To(Equal(expected))
		})

		It("should return error if invalid", func() {
			rowData := types.Datum{VarCharValue: util.RefString("2016-02-29 08:11:22.000")} // date type contains timestamp
			_, err := castAthenaRowData(ctx, rowData, athenaTypeDate)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("Invalid type", func() {
		It("should default to string", func() {
			rowData := types.Datum{VarCharValue: util.RefString("unknown data gem")}
			result, err := castAthenaRowData(ctx, rowData, "some-invalid-athena-type")
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal("unknown data gem"))
		})
	})
})
