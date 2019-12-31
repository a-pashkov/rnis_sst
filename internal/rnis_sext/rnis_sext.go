package rnis_sext

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	enegbig = 8
	epos4   = 10
	etuple  = 16
	eatom   = 12

/*
-define(rev_sext , 4).
%%
-define(negbig   , 8).
-define(neg4     , 9).
-define(pos4     , 10).
-define(posbig   , 11).
-define(atom     , 12).
-define(reference, 13).
-define(port     , 14).
-define(pId      , 15).
-define(tuple    , 16).
-define(list     , 17).
-define(binary   , 18).
-define(bin_tail , 19).*/
)

const maxUint = ^uint64(0)

type RnisObjKey struct {
	//KeyType string // atom 'o'
	//Prefix  string // atom 'sp'|'tmp'
	Id   int
	Time int64
}

func RnisKeyDecode(b []byte) (*RnisObjKey, error) {
	return rnisTupleDecode(b)

}

func rnisTupleDecode(b []byte) (rnisObjKey *RnisObjKey, err error) {
	rnisObjKey = new(RnisObjKey)

	//// Проверка, что tuple
	//if b[0] != etuple {
	//	return nil, fmt.Errorf("%#X not a tuple\n", b)
	//}

	//// Проверка размера tuple
	//size := binary.BigEndian.Uint32(b[1:5])
	//b = b[5:]
	//if size != 4 {
	//	return nil, fmt.Errorf("tuple size %d != 4\n", size)
	//}

	//// Элемент 1 atom
	//if b[0] != eatom {
	//	return nil, fmt.Errorf("%#X not a atom\n", b)
	//}
	//rnisObjKey.KeyType, b = sextDecodeAtom(b)
	//if rnisObjKey.KeyType != "o" {
	//	return nil, nil
	//}

	////Элемент 2 atom
	//if b[0] != eatom {
	//	return nil, fmt.Errorf("%#X not a atom\n", b)
	//}
	//rnisObjKey.Prefix, b = sextDecodeAtom(b)
	//if rnisObjKey.Prefix != "sp" {
	//	return nil, nil
	//}

	// Проверка, что ключ {'o', 'sp', _, _}
	if !bytes.Equal(b[:14], []byte{16, 0, 0, 0, 4, 12, 183, 128, 8, 12, 185, 220, 0, 8}) {
		return nil, nil
	}
	b = b[14:]

	// декодировать Id
	if b[0] != epos4 {
		return nil, fmt.Errorf("%#X not a pos4\n", b)
	}
	rnisObjKey.Id, b, err = sextDecodePos(b)
	if err != nil {
		return nil, err
	}

	// декодировать Time
	if b[0] != enegbig {
		return nil, fmt.Errorf("%#X not a negbig\n", b)
	}
	rnisObjKey.Time, b, err = sextDecodeNegbig(b)
	if err != nil {
		return nil, err
	}

	return rnisObjKey, nil
}

func sextDecodeAtom(b []byte) (string, []byte) {
	var a []byte
	a, b = sextDecodeBinary(b[1:])
	return string(a), b
}

func sextDecodeBinary(b []byte) ([]byte, []byte) {
	if b[0] == 8 {
		return b[1:], []byte{}
	}
	var acc []byte
	for i := uint8(0); b[0]&(1<<(7-i)) != 0; i = (i + 1) & 0x07 {
		acc = append(acc, (b[0]<<(i+1))|(b[1]>>(7-i)))
		if i == 7 {
			b = b[2:]
		} else {
			b = b[1:]
		}
	}
	return acc, b[2:]
}

// Не реализованы числа с плавающей запятой
func sextDecodePos(b []byte) (int, []byte, error) {
	var d uint32
	header := b[1:5]
	if header[3]&0x01 == 0 {
		b = b[5:]
		d = binary.BigEndian.Uint32(header) >> 1
	} else {
		return 0, nil, fmt.Errorf("Error decode pos4: %d\n", b)
	}
	return int(d), b, nil
}

func sextDecodeNegbig(b []byte) (int64, []byte, error) {
	// Как много 64битных слов необходимо для представления положительного числа?
	words := 0xffffFFFF - binary.BigEndian.Uint32(b[1:5])

	// Максимальное число помещающееся в words 64битных слов
	max, err := imax(words)
	if err != nil {
		return 0, nil, err
	}
	b = b[5:]

	// Получаем
	ib0, b := sextDecodeBinary(b)

	ib, err := removeSizeBits(ib0)
	if err != nil {
		return 0, nil, err
	}

	var i0 uint64
	for _, b1 := range ib {
		i0 = i0 << 8
		i0 |= uint64(b1)
	}

	i := max - i0

	f := b[0]

	if f != 0xFF {
		return 0, nil, fmt.Errorf("%#X it's a float!\n", b)
	}
	b = b[1:]

	return -int64(i), b, nil
}

func removeSizeBits(b []byte) ([]byte, error) {
	if b[0] != 0xFF {
		return nil, fmt.Errorf("%#X legacy bignum\n", b)
	}
	b = b[1:]
	_, uvarintLen := binary.Uvarint(b)
	return b[uvarintLen:], nil
}

// Возвращает максимально возможное число для size*64битных слов
func imax(size uint32) (uint64, error) {
	switch size {
	case 1:
		return maxUint, nil
	}
	return 0, fmt.Errorf("Numbers greater then uint64 are not supported~n")
}
