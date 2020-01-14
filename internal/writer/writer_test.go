package writer

import (
	"fmt"
	"testing"
	"time"
)

func TestInitParallel(t *testing.T) {
	//fmt.Println("hi")
	rec := make(chan CsvRecord, 10)
	fin := make(chan struct{})
	path := "./"

	go InitWriter(path, rec, fin)

	for j := 0; j < 10000; j++ {
		for i := 0; i < 1000000; i++ {
			rec <- CsvRecord{Id: j * 1000, Line: "01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"}
		}
	}
	time.Sleep(10 * time.Second)
	close(rec)
	<-fin

	fmt.Println("end")
}
