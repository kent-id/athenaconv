package athenaconv_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAthenaconv(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Athenaconv Suite")
}
