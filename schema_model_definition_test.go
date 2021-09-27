package athenaconv

import (
	"reflect"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Schema: model definition", func() {
	When("model type/definition is valid", func() {
		It("should return expected column definition", func() {
			type test struct {
				ID           int    `athenaconv:"my_id_col"`
				Name         string `athenaconv:"name_col"`
				privateField string `athenaconv:"pvt_field"`
			}
			def, err := newModelDefinitionMap(reflect.TypeOf(test{}))
			Expect(err).ToNot(HaveOccurred())
			Expect(len(def)).To(Equal(3))
			Expect(def["my_id_col"].fieldName).To(Equal("ID"))
			Expect(def["name_col"].fieldName).To(Equal("Name"))
			Expect(def["pvt_field"].fieldName).To(Equal("privateField"))
		})
	})

	When("any struct field is missing athenaconv tags", func() {
		It("should return error", func() {
			type test struct {
				ID   int `athenaconv:"my_id_col"`
				Name string
			}
			_, err := newModelDefinitionMap(reflect.TypeOf(test{}))
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(MatchRegexp("missing .* name"))
		})
	})

	When("struct fields have duplicate tags", func() {
		It("should return error", func() {
			type test struct {
				ID   int    `athenaconv:"my_id_col"`
				Name string `athenaconv:"my_id_col"`
			}
			_, err := newModelDefinitionMap(reflect.TypeOf(test{}))
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(MatchRegexp("duplicate .* my_id_col"))
		})
	})

	When("struct has no fields", func() {
		It("should return expected column definition", func() {
			type test struct{}
			_, err := newModelDefinitionMap(reflect.TypeOf(test{}))
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(ContainSubstring("at least one field"))
		})
	})

	When("model type is a pointer instead of struct value", func() {
		It("should return error", func() {
			type test struct {
				ID int `athenaconv:"my_id_col"`
			}
			_, err := newModelDefinitionMap(reflect.TypeOf(&test{}))
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(ContainSubstring("invalid modeltype"))
		})
	})

	When("model type is an int instead of struct", func() {
		It("should return error", func() {
			var num int = 0
			_, err := newModelDefinitionMap(reflect.TypeOf(num))
			Expect(err).To(HaveOccurred())
			Expect(strings.ToLower(err.Error())).To(ContainSubstring("invalid modeltype"))
		})
	})
})
