package writer

import (
	"testing"
	//"time"
	"fmt"
)

func TestInitParallel(t *testing.T) {
	//fmt.Println("hi")
	rec := make(chan CsvRecord, 10)
	fin := make(chan struct{})
	path := "./"

	go Init(path, rec, fin)

	for i := 0; i < 5; i++ {
		rec <- CsvRecord{Id: 1, Line: "test1"}
		fmt.Println("sended")
	}
	close(rec)
	<-fin

	fmt.Println("end")

	if 1 == 2 {
		t.Errorf("Hello()")
	}
}

//func ExampleInit() {
//	fmt.Println("tesstss")
//	// Output: 123
//}
