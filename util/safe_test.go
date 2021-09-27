package util

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Safe", func() {
	When("SafeString is called", func() {
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

	When("SafeInt is called", func() {
		It("returns zero value if nil", func() {
			out := SafeInt(nil)
			Expect(out).To(Equal(int(0)))
		})

		It("returns original value if not nil", func() {
			var in int = 10
			out := SafeInt(&in)
			Expect(out).To(Equal(int(10)))
		})
	})

	When("SafeInt32 is called", func() {
		It("returns zero value if nil", func() {
			out := SafeInt32(nil)
			Expect(out).To(Equal(int32(0)))
		})

		It("returns original value if not nil", func() {
			var in int32 = 10
			out := SafeInt32(&in)
			Expect(out).To(Equal(int32(10)))
		})
	})

	When("SafeInt64 is called", func() {
		It("returns zero value if nil", func() {
			out := SafeInt64(nil)
			Expect(out).To(Equal(int64(0)))
		})

		It("returns original value if not nil", func() {
			var in int64 = 10
			out := SafeInt64(&in)
			Expect(out).To(Equal(int64(10)))
		})
	})

	When("refString is called", func() {
		It("returns a pointer to a string", func() {
			in := "hello"
			out := RefString(in)
			Expect(*out).To(Equal("hello"))
		})
	})

	When("refInt is called", func() {
		It("returns pointer to value", func() {
			out := RefInt(5)
			Expect(*out).To(Equal(int(5)))
		})
	})

	When("refInt32 is called", func() {
		It("returns pointer to value", func() {
			out := RefInt32(5)
			Expect(*out).To(Equal(int32(5)))
		})
	})

	When("refInt64 is called", func() {
		It("returns pointer to value", func() {
			out := RefInt64(5)
			Expect(*out).To(Equal(int64(5)))
		})
	})
})
