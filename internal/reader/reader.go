package reader

import (
	"fmt"
	"github.com/a-pashkov/rnis_sst/internal/rnis_serialize"
	"github.com/a-pashkov/rnis_sst/internal/rnis_sext"
	"github.com/a-pashkov/rnis_sst/internal/sst"
	"github.com/a-pashkov/rnis_sst/internal/writer"
	"os"
	"strconv"
)

// Статистика чтения из .sst файла
type ReaderStat struct {
	File          string
	DataBlocks    uint64
	UsedRecords   uint64
	UnusedRecords uint64
}

func (readerStat *ReaderStat) String() string {
	return fmt.Sprintf("File:%s DataBlocks:%d UsedRecords:%d UnusedRecords:%d", readerStat.File, readerStat.DataBlocks, readerStat.UsedRecords, readerStat.UnusedRecords)
}

func Read(r string, res chan<- writer.CsvRecord, stat chan<- ReaderStat) {
	// Открыть файл
	f, err := os.Open(r)
	defer f.Close()
	if err != nil {
		panic(err)
	}

	// Получить footer
	footer, err := sst.GetFooter(f)
	if err != nil {
		panic(err)
	}

	// Получить индексный блок из файла и распаковать его
	indexBlock, err := sst.GetBlock(f, footer.IndexOffset, footer.IndexSize)
	if err != nil {
		panic(err)
	}

	// Перебор индексов
	var s ReaderStat
	s.File = r
	for len(indexBlock) > 0 {
		var indexRecord *sst.Record
		indexRecord, indexBlock = sst.GetBlockRecord(indexBlock, nil)
		//fmt.Println("indexRecord:", indexRecord.Key, indexRecord.Value)

		if indexRecord != nil {
			//fmt.Println("indexRecord:", indexRecord.Key, indexRecord.Value /*, "tail:", indexBlock*/)
			dataOffset, tail := sst.GetUvarint(indexRecord.Value)
			dataSize, _ := sst.GetUvarint(tail)
			//fmt.Println("dataOffset", dataOffset, "dataSize", dataSize)

			// Получаем dataBlock
			dataBlock, err := sst.GetBlock(f, dataOffset, dataSize)
			if err != nil {
				panic(err)
			}
			s.DataBlocks++

			// Перебор записей в dataBlock
			var lastKey []byte
			for len(dataBlock) > 0 {
				var dataRecord *sst.Record
				dataRecord, dataBlock = sst.GetBlockRecord(dataBlock, lastKey)
				if dataRecord != nil {
					lastKey = dataRecord.Key
					key, err := rnis_sext.RnisKeyDecode(dataRecord.Key)
					if err != nil {
						panic(err)
					}
					if key != nil {
						val, err := rnis_serialize.Deserialize(dataRecord.Value)
						if err != nil {
							panic(err)
						}
						valStr := rnis_serialize.JoinRnisRecs(val)
						s.UsedRecords++
						res <- writer.CsvRecord{Id: key.Id, Line: strconv.Itoa(int(key.Time)) + valStr}
					} else {
						s.UnusedRecords++
					}
				}
			}
		}
	}
	stat <- s
	close(stat)
}
