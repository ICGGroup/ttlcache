package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestSapUpload(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SapUpload Suite")
}
