package sst

import (
	"bytes"
	"encoding/binary"
	"errors"
	//"fmt"
	"github.com/golang/snappy"
	"os"
)

const (
	footerSize       = 48
	blockTrailerSize = 5
)

type Footer struct {
	MetaOffset  uint64
	MetaSize    uint64
	IndexOffset uint64
	IndexSize   uint64
}

//type IndexHandler struct {
//	Offset uint64
//	Size   uint64
//}

type Record struct {
	//sharedBytes   uint64
	//unsharedBytes uint64
	//keyLength     uint64
	//valueLength   uint64
	Key   []byte
	Value []byte
}

//func GetIndexHandles(f *os.File) (handlers *[]IndexHandler, err error) {
//	footer, err := getFooter(f)
//	if err != nil {
//		return nil, err
//	}
//
//	// прочитать блок из файла и распаковать
//	block, err := GetBlock(f, footer.indexOffset, footer.indexSize)
//	if err != nil {
//		return nil, err
//	}
//
//	// заполнить срез адресов из блока
//	for len(block) > 0 {
//		record, block := getBlockRecord(block, nil)
//		h: = new(IndexHandler)
//		h.Offset, tail := getUvarint(record.Value)
//		h.Size, _ = getUvarint(record.Value)
//		handlers = append(handlers, h)
//	}
//
//		fmt.Println("index", index.Key, index.Value)
//	return nil, nil
//}

func GetFooter(f *os.File) (footer Footer, err error) {
	// сдвигаем до footer
	f.Seek(-footerSize, 2)

	// Читаем footer
	buffer := make([]byte, footerSize)
	n, err := f.Read(buffer)
	if err != nil {
		return footer, err
	}

	// Проверить размер footer
	if n != footerSize {
		return footer, errors.New("Error! reading file")
	}

	// проверить magic word
	if !bytes.Equal(buffer[40:], []byte{0x57, 0xFB, 0x80, 0x8B, 0x24, 0x75, 0x47, 0xDB}) {
		return footer, errors.New("Error! bad footer")
	}

	footer = parseFooter(buffer)

	return footer, err

}

func parseFooter(b []byte) (footer Footer) {
	var tail []byte
	footer.MetaOffset, tail = GetUvarint(b)
	footer.MetaSize, tail = GetUvarint(tail)
	footer.IndexOffset, tail = GetUvarint(tail)
	footer.IndexSize, tail = GetUvarint(tail)
	return footer
}

func GetUvarint(b []byte) (uvarint uint64, tail []byte) {
	var uvarintLen int
	uvarint, uvarintLen = binary.Uvarint(b)
	tail = b[uvarintLen:]
	return uvarint, tail
}

func GetBlock(f *os.File, offset uint64, size uint64) (block []byte, err error) {
	buffer := make([]byte, int64(size)+blockTrailerSize)
	n, err := f.ReadAt(buffer, int64(offset))

	if err != nil {
		return block, err
	}

	if n != int(size+blockTrailerSize) {
		return block, errors.New("Error! reading block data")
	}

	block, err = blockUnpack(buffer)
	return block, err
}

// Распаковать если пожат snappy
func blockUnpack(buffer []byte) (block []byte, err error) {
	blockType := buffer[len(buffer)-blockTrailerSize]
	//crc := binary.LittleEndian.Uint32(block[len(block)-blockTrailerSize+1:])

	if blockType == 1 {
		ublock, err := snappy.Decode(nil, buffer[:len(buffer)-blockTrailerSize])
		if err != nil {
			return block, err
		}
		block = ublock
	} else {
		block = buffer[:len(buffer)-blockTrailerSize]
	}
	return block, err
}

// Возвращает первую запись k,v из блока и остаток байт
func GetBlockRecord(b []byte, lastKey []byte) (record *Record, tail []byte) {
	var sharedBytes, unsharedBytes, keyLength, valueLength uint64
	sharedBytes, tail = GetUvarint(b)
	unsharedBytes, tail = GetUvarint(tail)
	keyLength = sharedBytes + unsharedBytes
	valueLength, tail = GetUvarint(tail)
	//fmt.Println("sharedBytes:", sharedBytes, "unsharedBytes", unsharedBytes, "tail_len:", len(tail))
	if keyLength == 0 && valueLength == 0 {
		return nil, nil
	} else {
		//block.key = append(lastKey[:block.sharedBytes], b[:block.unsharedBytes]...)
		record = new(Record)
		record.Key = make([]byte, keyLength)
		copy(record.Key, lastKey[:sharedBytes])
		copy(record.Key[sharedBytes:], tail[:unsharedBytes])
		tail = tail[unsharedBytes:]
		record.Value = tail[:valueLength]
		tail = tail[valueLength:]
		return record, tail
	}
}
