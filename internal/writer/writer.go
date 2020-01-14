package writer

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"path"
)

const (
	ext          = ".csv.gz"
	maxOpenFiles = 10000
	buffVol      = 200000
)

// Данные для записи
type CsvRecord struct {
	Id   int
	Line string
}

// Дескрипторы и буфер
type wGz struct {
	f  *os.File
	fb *bufio.Writer
	gf *gzip.Writer
	b  []byte
}

// Создание структуры для записи
func newWriter(s string) (w *wGz, err error) {
	w = &wGz{b: make([]byte, 0, buffVol)}
	w.f, err = os.OpenFile(s, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	w.fb = bufio.NewWriter(w.f)
	return
}

// Записывает данные из буфера и закрывает fzip&file
func (w *wGz) wClose() (err error) {
	w.gf.Reset(w.fb)
	_, err = w.gf.Write(w.b)
	if err != nil {
		return
	}

	err = w.gf.Close()
	if err != nil {
		return
	}

	err = w.fb.Flush()
	if err != nil {
		return
	}

	err = w.f.Close()
	if err != nil {
		return
	}
	return
}

// Записывает строку в буфер, при достижении буфером buffVol сжимает и записывает
func (w *wGz) write(s string) (err error) {
	rec := []byte(s + "\n")
	i := copy(w.b[len(w.b):buffVol], rec)
	w.b = w.b[:len(w.b)+i]
	tail := rec[i:]

	if len(tail) > 0 {
		w.gf.Reset(w.fb)

		_, err = w.gf.Write(w.b)
		if err != nil {
			return
		}

		_, err = w.gf.Write(tail)
		if err != nil {
			return
		}

		err = w.gf.Close()
		if err != nil {
			return
		}

		w.b = w.b[:0]
	}
	return
}

// структура с каналом id и map id-writer
type pool struct {
	c  chan int
	m  map[int]*wGz
	gf *gzip.Writer
}

func newPool() (p *pool) {
	p = new(pool)
	p.c = make(chan int, 10000)
	p.m = make(map[int]*wGz)
	return
}

// Получить writer по id
func (p *pool) get(i int) (w *wGz, found bool) {
	w, found = p.m[i]
	return
}

// Записать id и writer в конец pool в случае переполнения удаляет первый writer и возвращает его
func (p *pool) put(i int, w *wGz) (err error) {
	if _, found := p.m[i]; found {
		err = fmt.Errorf("Key %d already in pool", i)
		return
	}
	p.m[i] = w
	select {
	case p.c <- i:
		return
	default:
		err = fmt.Errorf("Buffer is overflowing. Len:%d", len(p.c))
		return
	}
}

// извлечь writer соответствующий первому id
func (p *pool) pop() (w *wGz) {
	select {
	case firstId := <-p.c:
		w, _ = p.m[firstId]
		delete(p.m, firstId)
		return
	default:
		return nil
	}
}

// Считывает данные для записи
// Получает ph, in, finished
func InitWriter(ph string, in <-chan CsvRecord, finished chan<- struct{}) {
	// map хранит пары id:fileDescroptor
	p := newPool()
	defer func() {
		for e := p.pop(); e != nil; e = p.pop() {
			err := e.wClose()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}
		finished <- struct{}{}
	}()
	// gzip Writer
	p.gf = gzip.NewWriter(nil)

	for rec := range in {
		w, err := getWriter(p, ph, rec.Id)
		if err != nil {
			panic(err)
		}

		err = w.write(rec.Line)
		if err != nil {
			panic(err)
		}
	}
}

// Открывает файл и создаёт путь к нему
// Возвращает
func getWriter(p *pool, ph string, id int) (w *wGz, err error) {
	// Если id есть в map возвращаем его дескриптор
	if w, found := p.m[id]; found {
		return w, nil
	}

	//debug.FreeOSMemory()
	// удаляем лишние ключи из map
	for len(p.m) >= maxOpenFiles {
		e := p.pop()
		err := e.wClose()
		if err != nil {
			return nil, err
		}
	}

	idStr := fmt.Sprintf("%07d", id)
	dir := path.Join(ph, idStr[0:1], idStr[1:2], idStr[2:3])
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return
		}
	}

	full := path.Join(dir, idStr+ext)

	// Получение writer
	w, err = newWriter(full)
	w.gf = p.gf
	if err != nil {
		return
	}

	p.put(id, w)
	return
}
