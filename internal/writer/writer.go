package writer

import (
	"fmt"
	//"time"
	"os"
	"path"
	//"io"
	"bufio"
	"compress/gzip"
)

const (
	ext          = ".csv.gz"
	maxOpenFiles = 10000
)

type CsvRecord struct {
	Id   int
	Line string
}

type wGz struct {
	f  *os.File
	gf *gzip.Writer
	fw *bufio.Writer
}

// Считывает данные для записи
// Получает path, in, finished
func InitWriter(path string, in <-chan CsvRecord, finished chan<- struct{}) {
	// map хранит пары id:fileDescroptor
	o := make(map[int]wGz)

	defer func() {
		for _, f := range o {
			err := closeGz(f)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}
		finished <- struct{}{}
	}()

	for rec := range in {
		// Получить дескриптор для записи
		f, err := getFile(o, path, rec.Id)
		if err != nil {
			panic(err)
		}
		// Запись в файл
		_, err = (f.fw).WriteString(rec.Line + "\n")
		if err != nil {
			panic(err)
		}
	}
}

// Открывает файл и создаёт путь к нему
// Возвращает файл
func getFile(o map[int]wGz, p string, id int) (f wGz, err error) {
	// Если id есть в map возвращаем его дескриптор
	if f, ok := o[id]; ok {
		return f, nil
	}

	// удаляем лишние ключи из map
	for iD, fD := range o {
		if len(o) >= maxOpenFiles {
			// fmt.Println("закрыть файл, превышен буфер", i, f)
			// закрыть файл
			err = closeGz(fD)
			if err != nil {
				return
			}
			// удалить из отображения
			delete(o, iD)
		} else {
			break
		}
	}

	idStr := fmt.Sprintf("%07d", id)
	dir := path.Join(p, idStr[0:1], idStr[1:2], idStr[2:3])
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return
		}
	}

	full := path.Join(dir, idStr+ext)

	// Открытие файла
	f.f, err = os.OpenFile(full, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	// gzip writer
	f.gf = gzip.NewWriter(f.f)
	// buffer
	f.fw = bufio.NewWriter(f.gf)

	o[id] = f
	return
}

// Сброс буфера, закрытие gzip, закрытие файла
func closeGz(f wGz) (err error) {
	name := f.f.Name()
	err = f.fw.Flush()
	if err != nil {
		return fmt.Errorf("%s\n%s", name, err.Error())
	}
	err = f.gf.Close()
	if err != nil {
		return fmt.Errorf("%s\n%s", name, err.Error())
	}
	err = f.f.Close()
	if err != nil {
		return fmt.Errorf("%s\n%s", name, err.Error())
	}
	return
}
