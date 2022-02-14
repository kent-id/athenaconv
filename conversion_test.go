package athenaconv

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/kent-id/athenaconv/types"
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
		It("should return true for 'true'", func() {
			rowData := types.Datum{VarCharValue: util.RefString("true")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeBool, reflect.Bool)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(BeTrue())
		})
		It("should return false for 'False'", func() {
			rowData := types.Datum{VarCharValue: util.RefString("False")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeBool, reflect.Bool)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(BeFalse())
		})
		It("should return error if not valid", func() {
			rowData := types.Datum{VarCharValue: util.RefString("some-invalid-value")}
			_, err := castAthenaRowData(ctx, rowData, athenaTypeBool, reflect.Bool)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(MatchRegexp("parsing .* invalid syntax"))
		})

		It("should return error on nil", func() {
			rowData := types.Datum{VarCharValue: nil}
			_, err := castAthenaRowData(ctx, rowData, athenaTypeBool, reflect.Bool)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(MatchRegexp("parsing .* invalid syntax"))
		})
	})

	Context("String", func() {
		It("should return string value as is", func() {
			rowData := types.Datum{VarCharValue: util.RefString("test data")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeString, reflect.String)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal("test data"))
		})

		It("should return value as is and we're assigning to *string (ptr kind)", func() {
			rowData := types.Datum{VarCharValue: util.RefString("test data")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeString, reflect.Ptr)
			Expect(err).ToNot(HaveOccurred())
			v := result.(*string)
			Expect(v).ToNot(BeNil())
			Expect(*v).To(Equal("test data"))
		})

		It("should return empty value on nil", func() {
			rowData := types.Datum{VarCharValue: nil}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeString, reflect.String)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(BeEmpty())
		})

		It("should return empty value on empty when we're assigning to *string (ptr kind)", func() {
			rowData := types.Datum{VarCharValue: util.RefString("")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeString, reflect.Ptr)
			Expect(err).ToNot(HaveOccurred())
			v := result.(*string)
			Expect(v).ToNot(BeNil())
			Expect(*v).To(BeEmpty())
		})

		It("should return nil value on nil when we're assigning to *string (ptr kind)", func() {
			rowData := types.Datum{VarCharValue: util.RefString("")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeString, reflect.Ptr)
			Expect(err).ToNot(HaveOccurred())
			v := result.(*string)
			Expect(v).ToNot(BeNil())
			Expect(*v).To(BeEmpty())
		})
	})

	Context("Integer", func() {
		It("should return value if valid", func() {
			rowData := types.Datum{VarCharValue: util.RefString("-2147483648")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeInt, reflect.Int)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(int(-2147483648)))
		})

		It("should return error if not valid", func() {
			rowData := types.Datum{VarCharValue: util.RefString("-----2147483648")}
			_, err := castAthenaRowData(ctx, rowData, athenaTypeInt, reflect.Int)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(MatchRegexp("parsing .* invalid syntax"))
		})

		// anything above int64 range will overflow
		It("should return error if overflow", func() {
			rowData := types.Datum{VarCharValue: util.RefString("9223372036854775807123213122")}
			_, err := castAthenaRowData(ctx, rowData, athenaTypeInt, reflect.Int)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(ContainSubstring("out of range"))
		})
	})

	Context("BigInt", func() {
		It("should return value if valid", func() {
			rowData := types.Datum{VarCharValue: util.RefString("9223372036854775807")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeBigInt, reflect.Int64)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(int64(9223372036854775807)))
		})

		It("should return error if not valid", func() {
			rowData := types.Datum{VarCharValue: util.RefString("9223372_NOT_VALID_036854775807")}
			_, err := castAthenaRowData(ctx, rowData, athenaTypeBigInt, reflect.Int64)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(MatchRegexp("parsing .* invalid syntax"))
		})

		// anything above int64 range will overflow
		It("should return error if overflow", func() {
			rowData := types.Datum{VarCharValue: util.RefString("9223372036854775807123213122")}
			_, err := castAthenaRowData(ctx, rowData, athenaTypeInt, reflect.Int)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(ContainSubstring("out of range"))
		})
	})

	Context("Array", func() {
		When("array has no items", func() {
			It("should return expected array value", func() {
				rowData := types.Datum{VarCharValue: util.RefString("[]")}
				result, err := castAthenaRowData(ctx, rowData, athenaTypeArray, reflect.Slice)
				Expect(err).ToNot(HaveOccurred())
				arr := result.([]string)
				Expect(len(arr)).To(BeZero())
			})
		})

		When("array has one item", func() {
			It("should return expected array value", func() {
				rowData := types.Datum{VarCharValue: util.RefString("[data1]")}
				result, err := castAthenaRowData(ctx, rowData, athenaTypeArray, reflect.Slice)
				Expect(err).ToNot(HaveOccurred())
				arr := result.([]string)
				Expect(len(arr)).To(Equal(1))
				Expect(arr[0]).To(Equal("data1"))
			})
		})

		When("array has two items", func() {
			It("should return expected array value", func() {
				rowData := types.Datum{VarCharValue: util.RefString("[data1, data2]")}
				result, err := castAthenaRowData(ctx, rowData, athenaTypeArray, reflect.Slice)
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
			result, err := castAthenaRowData(ctx, rowData, athenaTypeTimestamp, reflect.Struct)
			Expect(err).ToNot(HaveOccurred())

			expected := time.Date(2012, 10, 31, 8, 11, 22, 0, time.UTC)
			Expect(result).To(BeAssignableToTypeOf(expected))
			ts := result.(time.Time)
			Expect(ts).To(Equal(expected))
		})

		It("should return value if valid with milliseconds", func() {
			rowData := types.Datum{VarCharValue: util.RefString("2012-10-31 08:11:22.512")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeTimestamp, reflect.Struct)
			Expect(err).ToNot(HaveOccurred())

			expected := time.Date(2012, 10, 31, 8, 11, 22, int(time.Millisecond)*512, time.UTC)
			Expect(result).To(BeAssignableToTypeOf(expected))
			ts := result.(time.Time)
			Expect(ts).To(Equal(expected))
		})

		It("should return value if valid and we're assigning to *time.Time (ptr kind)", func() {
			rowData := types.Datum{VarCharValue: util.RefString("2021-10-31 23:59:59.999")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeTimestamp, reflect.Ptr)
			Expect(err).ToNot(HaveOccurred())

			expected := time.Date(2021, 10, 31, 23, 59, 59, int(time.Millisecond)*999, time.UTC)
			ts := result.(*time.Time)
			Expect(*ts).To(Equal(expected))
		})

		It("should return error if data is empty/nil and we're assigning to time.Time", func() {
			rowData := types.Datum{VarCharValue: util.RefString("")}
			_, err := castAthenaRowData(ctx, rowData, athenaTypeTimestamp, reflect.Struct)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(ContainSubstring("cannot parse"))

			rowData = types.Datum{VarCharValue: nil}
			_, err = castAthenaRowData(ctx, rowData, athenaTypeTimestamp, reflect.Struct)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(ContainSubstring("cannot parse"))
		})

		It("should return error if data is empty/nil and we're assigning to *time.Time (ptr kind)", func() {
			rowData := types.Datum{VarCharValue: util.RefString("")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeTimestamp, reflect.Ptr)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(BeNil())

			rowData = types.Datum{VarCharValue: nil}
			_, err = castAthenaRowData(ctx, rowData, athenaTypeTimestamp, reflect.Ptr)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(BeNil())
		})

		It("should return error if invalid format", func() {
			rowData := types.Datum{VarCharValue: util.RefString("INVALID DATE 2012-10-31 08:11:22.000")}
			_, err := castAthenaRowData(ctx, rowData, athenaTypeTimestamp, reflect.Struct)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(ContainSubstring("cannot parse"))
		})

		It("should return error if out of range", func() {
			rowData := types.Datum{VarCharValue: util.RefString("2012-10-31 08:00:61.000")} // 61 secs, out of range
			_, err := castAthenaRowData(ctx, rowData, athenaTypeTimestamp, reflect.Struct)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(ContainSubstring("out of range"))
		})
	})

	Context("Date", func() {
		It("should return value if valid", func() {
			rowData := types.Datum{VarCharValue: util.RefString("2016-02-29")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeDate, reflect.Struct)
			Expect(err).ToNot(HaveOccurred())

			expected := time.Date(2016, 02, 29, 0, 0, 0, 0, time.UTC)
			Expect(result).To(BeAssignableToTypeOf(expected))
			ts := result.(time.Time)
			Expect(ts).To(Equal(expected))
		})

		It("should return value if valid and we're assigning to *time.Time (ptr kind)", func() {
			rowData := types.Datum{VarCharValue: util.RefString("2021-12-31")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeDate, reflect.Ptr)
			Expect(err).ToNot(HaveOccurred())

			expected := time.Date(2021, 12, 31, 0, 0, 0, 0, time.UTC)
			ts := result.(*time.Time)
			Expect(*ts).To(Equal(expected))
		})

		It("should return error if data is empty/nil and we're assigning to time.Time", func() {
			rowData := types.Datum{VarCharValue: util.RefString("")}
			_, err := castAthenaRowData(ctx, rowData, athenaTypeDate, reflect.Struct)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(ContainSubstring("cannot parse"))

			rowData = types.Datum{VarCharValue: nil}
			_, err = castAthenaRowData(ctx, rowData, athenaTypeDate, reflect.Struct)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(ContainSubstring("cannot parse"))
		})

		It("should return error if data is empty/nil and we're assigning to *time.Time (ptr kind)", func() {
			rowData := types.Datum{VarCharValue: util.RefString("")}
			result, err := castAthenaRowData(ctx, rowData, athenaTypeDate, reflect.Ptr)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(BeNil())

			rowData = types.Datum{VarCharValue: nil}
			_, err = castAthenaRowData(ctx, rowData, athenaTypeDate, reflect.Ptr)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(BeNil())
		})

		It("should return error if invalid", func() {
			rowData := types.Datum{VarCharValue: util.RefString("2016-02-29 08:11:22.000")} // date type contains timestamp
			_, err := castAthenaRowData(ctx, rowData, athenaTypeDate, reflect.Struct)
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(MatchRegexp("parsing time .* extra text"))
		})
	})

	Context("Invalid type", func() {
		It("should default to string", func() {
			rowData := types.Datum{VarCharValue: util.RefString("unknown data gem")}
			result, err := castAthenaRowData(ctx, rowData, "some-invalid-athena-type", reflect.String)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal("unknown data gem"))
		})
	})
})
