package main

import (
	"flag"
	"fmt"
	"github.com/a-pashkov/rnis_sst/internal/reader"
	"github.com/a-pashkov/rnis_sst/internal/writer"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"time"
)

const (
	wrBuffer   = 100
	statBuffer = 10
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	timeStart := time.Now()
	in := flag.String("i", "./", "file or directory with .sst")
	out := flag.String("o", "./results", "directory for results")
	flag.Parse()

	//// Получить имя файла -f или список файлов из директории sst_?/*.sst -d
	files, err := getFilenames(*in)
	if err != nil {
		panic(err)
	}

	// Канал записи результатов
	res := make(chan writer.CsvRecord, wrBuffer)

	// Канал ожидания завершения записи
	wrFin := make(chan struct{})

	//// Запустить writer и передать в него директорию с результатами, канал для данных и канал флага завершения
	go writer.InitWriter(*out, res, wrFin)

	// Для каждого файла запустить считыватель и передать имя файла, канал для записи результатов и канал статистики
	fullStat := reader.ReaderStat{File: *in}
	for _, f := range files {
		// Канал статистики считывателя
		rStat := make(chan reader.ReaderStat, statBuffer)

		reader.Read(f, res, rStat)

		// Ожиданиие завершения канала статистики
		for stat := range rStat {
			fullStat.DataBlocks += stat.DataBlocks
			fullStat.UsedRecords += stat.UsedRecords
			fullStat.UnusedRecords += stat.UnusedRecords
			fmt.Println(stat.String())
		}

	}

	// Закрытие канала writer
	close(res)

	// Ожидание завершения writer
	<-wrFin

	// Удалить исходные файлы

	timeStop := time.Now()
	fullStat.Time = timeStop.Sub(timeStart)
	fmt.Println(fullStat.String())
}

func getFilenames(in string) (result []string, err error) {
	stat, err := os.Stat(in)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("%s not found", in)
	}
	mode := stat.Mode()
	if mode.IsDir() {
		result, err = getSSTFromDir(in)
	} else {
		if isSST(in) {
			result = append(result, in)
		} else {
			err = fmt.Errorf("%s has incorrect extension", in)
		}
	}
	return
}

func getSSTFromDir(in string) (result []string, err error) {
	err = filepath.Walk(in, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		m := info.Mode()
		if m.IsRegular() && isSST(path) {
			result = append(result, path)
		}
		return nil
	})
	if len(result) == 0 {
		err = fmt.Errorf("%s has no sst files", in)
	}
	return
}

func isSST(path string) bool {
	if filepath.Ext(path) == ".sst" {
		return true
	}
	return false
}