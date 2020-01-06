package sst

import (
	"testing"
	//"time"
	"fmt"
	"io/ioutil"
)

func TestGetFooter(t *testing.T) {
	data, err := ioutil.ReadFile("/home/admin/redmine_3618/rnis_sst/test/test_db/sst_0/000007.sst")
	if err != nil {
		fmt.Println(err)
	}

	footer, err := GetFooter(data)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Footer", footer)
	//fmt.Println("hi")
	//rec := make(chan CsvRecord, 10)
	//fin := make(chan struct{})
	//path := "./"

	//go Init(path, rec, fin)

	//for i := 0; i < 5; i++ {
	//	rec <- CsvRecord{Id: 1, Line: "test1"}
	//	fmt.Println("sended")
	//}
	//close(rec)
	//<-fin

	//fmt.Println("end")

	//if 1 == 2 {
	//	t.Errorf("Hello()")
	//}
}

//func ExampleInit() {
//	fmt.Println("tesstss")
//	// Output: 123
//}
