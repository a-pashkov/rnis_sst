package rnis_serialize

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
)

const (
	tNull        = 0
	tBinary255   = 1
	tBinary65535 = 2
	tUint1       = 10
	tSint1       = 20
	tSint4       = 22
	tSint8       = 23
	tFloat8      = 31
	delimiter    = ";"
)

/*
-define(VALUE_TYPE_NULL, 0).
-define(VALUE_TYPE_BINARY_255, 1).
-define(VALUE_TYPE_BINARY_65535, 2).
-define(VALUE_TYPE_UINT_1B, 10).
-define(VALUE_TYPE_SINT_1B, 20).
-define(VALUE_TYPE_SINT_4B, 22).
-define(VALUE_TYPE_SINT_8B, 23).
-define(VALUE_TYPE_FLOAT_8B, 31).
*/

type RnisRecord interface {
	String() string
}

// tNull
type sRnisNull struct {
	name []byte
}

func (rnisNull *sRnisNull) String() string {
	return string(rnisNull.name) + delimiter
}

// tBinary255
type sRnisBinary255 struct {
	name  []byte
	value []byte
}

func (rnisBinary255 *sRnisBinary255) String() string {
	return string(rnisBinary255.name) + delimiter + "x" + hex.EncodeToString(rnisBinary255.value)
}

// tBinary65535
type sRnisBinary65535 struct {
	name  []byte
	value []byte
}

func (rnisBinary65535 *sRnisBinary65535) String() string {
	return string(rnisBinary65535.name) + delimiter + "x" + hex.EncodeToString(rnisBinary65535.value)
}

// tUint1
type sRnisUint struct {
	name  []byte
	value uint8
}

func (rnisUint *sRnisUint) String() string {
	return string(rnisUint.name) + delimiter + strconv.Itoa(int(rnisUint.value))
}

// tSint1
type sRnisSint1 struct {
	name  []byte
	value int8
}

func (rnisSint1 *sRnisSint1) String() string {
	return string(rnisSint1.name) + delimiter + strconv.Itoa(int(rnisSint1.value))
}

// tSint4
type sRnisSint4 struct {
	name  []byte
	value int32
}

func (rnisSint4 *sRnisSint4) String() string {
	return string(rnisSint4.name) + delimiter + strconv.Itoa(int(rnisSint4.value))
}

// tSint8
type sRnisSint8 struct {
	name  []byte
	value int64
}

func (rnisSint8 *sRnisSint8) String() string {
	return string(rnisSint8.name) + delimiter + strconv.FormatInt(rnisSint8.value, 10)
}

// tFloat8
type sRnisFloat8 struct {
	name  []byte
	value float64
}

func (rnisFloat8 *sRnisFloat8) String() string {
	return string(rnisFloat8.name) + delimiter + strconv.FormatFloat(rnisFloat8.value, 'f', -1, 64)
}

func Deserialize(b []byte) (records []RnisRecord, err error) {

	for len(b) > 0 {
		// Длина имени
		size := uint(b[0])
		b = b[1:]

		// Имя
		name := b[:size]
		b = b[size:]

		// Тип значения
		vType := b[0]
		b = b[1:]

		switch vType {
		case tNull:
			records = append(records, &sRnisNull{name: name})
		case tBinary255:
			size := uint8(b[0])
			b = b[1:]
			v := b[:size]
			b = b[size:]
			records = append(records, &sRnisBinary255{name: name, value: v})
		case tBinary65535:
			size := binary.BigEndian.Uint16(b[:2])
			b = b[2:]
			v := b[:size]
			b = b[size:]
			records = append(records, &sRnisBinary65535{name: name, value: v})

		case tUint1:
			v := uint8(b[0])
			b = b[1:]
			records = append(records, &sRnisUint{name: name, value: v})
		case tSint1:
			v := int8(b[0])
			b = b[1:]
			records = append(records, &sRnisSint1{name: name, value: v})
		case tSint4:
			bits := binary.BigEndian.Uint32(b[:4])
			b = b[4:]
			v := int32(bits)
			records = append(records, &sRnisSint4{name: name, value: v})
		case tSint8:
			bits := binary.BigEndian.Uint64(b[:8])
			b = b[8:]
			v := int64(bits)
			records = append(records, &sRnisSint8{name: name, value: v})

		case tFloat8:
			bits := binary.BigEndian.Uint64(b[:8])
			b = b[8:]
			v := math.Float64frombits(bits)
			records = append(records, &sRnisFloat8{name: name, value: v})
		default:
			return nil, fmt.Errorf("'%s' has unknown type %s\n", string(name), strconv.Itoa(int(vType)))
		}
	}
	return records, nil
}

func JoinRnisRecs(records []RnisRecord) (res string) {
	for _, r := range records {
		res += delimiter + r.String()
	}
	return res
}
