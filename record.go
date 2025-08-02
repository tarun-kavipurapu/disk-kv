package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

/*

append only storage
FORMAT
-------|-----------|
HEADER |  KEY_VALUE|
-------|-----------|
*/

// the structure that we store in the disk

type Record struct {
	Header *Header
	Key    string
	value  []byte
}

// Lets use this afterwards
// var bufPool = sync.Pool{
// 	New: func() interface{} {
// 		return new(bytes.Buffer)
// 	},
// }

func NewRecord(FileID int, key string, val []byte) ([]byte, *Record) {
	header := &Header{
		Timestamp: uint32(time.Now().Unix()),
		Keysize:   uint32(len([]byte(key))),
		Valsize:   uint32(len(val)),
	}

	buffer := new(bytes.Buffer)
	defer buffer.Reset()
	err := header.encode(buffer)
	if err != nil {
		fmt.Printf("error encoding the header %v", err)
	}
	record := &Record{
		Header: header,
		Key:    key,
		value:  val,
	}
	buffer.WriteString(key)
	buffer.Write(val)

	return buffer.Bytes(), record
}

func DecodeRecord(buffer []byte) (*Record, error) {
	if len(buffer) < 12 { // Header is 12 bytes (3 * uint32)
		return nil, fmt.Errorf("buffer too small for header")
	}
	header := &Header{}
	buff := bytes.NewBuffer(buffer[:12])
	err := binary.Read(buff, binary.LittleEndian, header)
	if err != nil {
		return nil, fmt.Errorf("error decoding header : %v", err)
	}
	keysize := header.Keysize
	keystart := 12
	keyend := keystart + int(keysize)
	key := string(buffer[keystart:keyend])

	valueStart := keyend
	valueEnd := valueStart + int(header.Valsize)
	value := buffer[valueStart:valueEnd]

	record := &Record{
		Header: header,
		Key:    key,
		value:  value,
	}

	return record, nil

}
