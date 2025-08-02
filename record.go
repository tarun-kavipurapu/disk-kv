package main

import (
	"bytes"
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
	Header Header
	Key    string
	value  []byte
}

// Lets use this afterwards
// var bufPool = sync.Pool{
// 	New: func() interface{} {
// 		return new(bytes.Buffer)
// 	},
// }

func NewRecord(FileID int, key string, val []byte) []byte {
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
	buffer.WriteString(key)
	buffer.Write(val)

	return buffer.Bytes()
}
