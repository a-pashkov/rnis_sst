package writer

import (
	"fmt"
	//"time"
	"os"
	"path"
	//"io"
)

const (
	ext          = ".csv"
	maxOpenFiles = 100
)

type CsvRecord struct {
	Id   int
	Line string
}

// Считывает данные для записи
// Получает path, in, finished
func InitWriter(path string, in <-chan CsvRecord, finished chan<- struct{}) {
	// map хранит пары id:fileDescroptor
	o := make(map[int]*os.File)

	for rec := range in {
		// Получить дескриптор для записи
		f, err := getFile(o, path, rec.Id)
		if err != nil {
			panic(err)
		}
		// Запись в файл
		_, err = f.WriteString(rec.Line + "\n")
		if err != nil {
			panic(err)
		}
		//fmt.Println(f, err, len(o))
	}
	fmt.Println("finished")

	// Закрыть файлы
	for _, f := range o {
		fileClose(f)
	}

	// Сообщение о завершении
	finished <- struct{}{}
}

// Открывает файл и создаёт путь к нему
// Возвращает файл
func getFile(o map[int]*os.File, p string, id int) (f *os.File, err error) {
	// Если id есть в map возвращаем его дескриптор
	if o[id] != nil {
		return o[id], nil
	}

	// удаляем лишние ключи из map
	for i, f := range o {
		if len(o) >= maxOpenFiles {
			// fmt.Println("закрыть файл, превышен буфер", i, f)
			// закрыть файл
			err := f.Close()
			if err != nil {
				return nil, err
			}
			// удалить из отображения
			delete(o, i)
		}
	}

	idStr := fmt.Sprintf("%07d", id)
	dir := path.Join(p, idStr[0:1], idStr[1:2], idStr[2:3])
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}
	}

	full := path.Join(dir, idStr+ext)

	f, err = os.OpenFile(full, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	o[id] = f
	return f, err
}

// Закрытие файла
func fileClose(f *os.File) {
	name := f.Name()
	err := f.Close()
	if err != nil {
		fmt.Println(name, err)
	}
}
