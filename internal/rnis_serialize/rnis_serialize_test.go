package rnis_serialize

import (
	"testing"
	//"time"
	"fmt"
)

func TestRnisKeyDecode(t *testing.T) {
	values := []byte{5, 97, 95, 98, 116, 110, 10, 0,
		7, 98, 101, 97, 114, 105, 110, 103,
		10, 119, 3, 108, 97, 116, 31, 64, 75,
		226, 191, 21, 27, 226, 191, 3, 108,
		111, 110, 31, 64, 66, 209, 220,
		10, 146, 209, 220, 5, 114, 116,
		105, 109, 101, 23, 0, 0, 1, 110,
		24, 75, 37, 73, 5, 115, 112, 101,
		101, 100, 10, 14}

	k, err := Deserialize(values)
	fmt.Println(k, err)

	if 1 == 2 {
		t.Errorf("Hello()")
	}
}

//func ExampleInit() {
//	fmt.Println("tesstss")
//	// Output: 123
//}
