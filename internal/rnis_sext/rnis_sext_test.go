package rnis_sext

import (
	"testing"
	//"time"
	"fmt"
)

func TestRnisKeyDecode(t *testing.T) {
	var key = []byte{0x10, 0x00, 0x00, 0x00, 0x04, 0x0C, 0xB7, 0x80, 0x08, 0x0C, 0xB9, 0xDC, 0x00, 0x08, 0x0A, 0x00, 0x00, 0x21, 0x22, 0x08, 0xFF, 0xFF, 0xFF, 0xFE, 0xFF, 0xC2, 0x60, 0x1F, 0xFF, 0xFF, 0xFB, 0x23, 0xE7, 0xDA, 0x76, 0xB7, 0x20, 0x08, 0xFF, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

	k, err := RnisKeyDecode(key)
	fmt.Println(k, err)

	if 1 == 2 {
		t.Errorf("Hello()")
	}
}

//func ExampleInit() {
//	fmt.Println("tesstss")
//	// Output: 123
//}
