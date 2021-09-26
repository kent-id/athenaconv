package athenaconv

import (
	"reflect"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Schema: model definition", func() {
	Context("Valid struct definition", func() {
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

	Context("Missing struct tags", func() {
		It("should return error", func() {
			type test struct {
				ID   int `athenaconv:"my_id_col"`
				Name string
			}
			_, err := newModelDefinitionMap(reflect.TypeOf(test{}))
			Expect(err).To(HaveOccurred())
		})
	})

	Context("Duplicate struct tags", func() {
		It("should return error", func() {
			type test struct {
				ID   int    `athenaconv:"my_id_col"`
				Name string `athenaconv:"my_id_col"`
			}
			_, err := newModelDefinitionMap(reflect.TypeOf(test{}))
			Expect(err).To(HaveOccurred())
		})
	})

	Context("Empty struct", func() {
		It("should return expected column definition", func() {
			type test struct{}
			_, err := newModelDefinitionMap(reflect.TypeOf(test{}))
			Expect(err).To(HaveOccurred())
		})
	})
})
