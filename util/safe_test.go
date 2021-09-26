package util

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Safe", func() {
	Context("when safeString is called", func() {
		It("returns empty string if nil", func() {
			out := SafeString(nil)
			Expect(out).To(Equal(""))
		})

		It("returns original string if not nil", func() {
			in := "hello"
			out := SafeString(&in)
			Expect(out).To(Equal("hello"))
		})
	})

	Context("when refString is called", func() {
		It("returns a pointer to a string", func() {
			in := "hello"
			out := RefString(in)
			Expect(*out).To(Equal("hello"))
		})
	})
})
